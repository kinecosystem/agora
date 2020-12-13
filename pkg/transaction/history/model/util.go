package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"math"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/go/strkey"
)

type SortableEntries []*Entry

// Len is the number of elements in the collection.
func (s SortableEntries) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (s SortableEntries) Less(i int, j int) bool {
	iKey, err := s[i].GetOrderingKey()
	if err != nil {
		iKey = []byte{}
	}
	jKey, err := s[j].GetOrderingKey()
	if err != nil {
		jKey = []byte{}
	}

	return bytes.Compare(iKey, jKey) < 0
}

// Swap swaps the elements with indexes i and j.
func (s SortableEntries) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func (m *Entry) GetTxID() ([]byte, error) {
	switch v := m.Kind.(type) {
	case *Entry_Stellar:
		var env xdr.TransactionEnvelope
		if _, err := xdr.Unmarshal(bytes.NewReader(v.Stellar.EnvelopeXdr), &env); err != nil {
			return nil, errors.Wrap(err, "failed to parse envelope xdr")
		}

		hash, err := network.HashTransaction(&env.Tx, v.Stellar.NetworkPassphrase)
		return hash[:], err
	case *Entry_Solana:
		var txn solana.Transaction
		if err := txn.Unmarshal(v.Solana.Transaction); err != nil {
			return nil, errors.Wrap(err, "failed to parse solana transaction")
		}
		return txn.Signature(), nil
	default:
		return nil, errors.Errorf("unsupported entry version: %d", m.Version)
	}
}

func (m *Entry) GetAccounts() ([]string, error) {
	switch v := m.Kind.(type) {
	case *Entry_Stellar:
		var env xdr.TransactionEnvelope
		if _, err := xdr.Unmarshal(bytes.NewReader(v.Stellar.EnvelopeXdr), &env); err != nil {
			return nil, errors.Wrap(err, "failed to parse envelope xdr")
		}

		accounts, err := GetAccountsFromEnvelope(env)
		if err != nil {
			return nil, err
		}

		accountIDs := make([]string, 0, len(accounts))
		for k := range accounts {
			accountIDs = append(accountIDs, k)
		}
		return accountIDs, nil
	case *Entry_Solana:
		var txn solana.Transaction
		if err := txn.Unmarshal(v.Solana.Transaction); err != nil {
			return nil, errors.Wrap(err, "failed to parse solana transaction")
		}

		accounts, err := GetAccountsFromTransaction(txn)
		if err != nil {
			return nil, err
		}
		accountIDs := make([]string, 0, len(accounts))
		for k := range accounts {
			accountIDs = append(accountIDs, k)
		}
		return accountIDs, nil
	default:
		return nil, errors.Errorf("unsupported entry version: %d", m.Version)
	}
}

func (m *Entry) GetOrderingKey() ([]byte, error) {
	switch v := m.Kind.(type) {
	case *Entry_Stellar:
		var b [9]byte
		b[0] = byte(m.Version)
		binary.BigEndian.PutUint64(b[1:], v.Stellar.PagingToken)
		return b[:], nil
	case *Entry_Solana:
		h, err := m.GetTxID()
		if err != nil {
			return nil, err
		}

		var b [1 + 8 + 8]byte
		b[0] = byte(m.Version)
		binary.BigEndian.PutUint64(b[1:], v.Solana.Slot)
		copy(b[9:], h[:8])
		return b[:], nil
	default:
		return nil, errors.Errorf("unsupported entry version: %d", m.Version)
	}
}

func OrderingKeyFromCursor(v KinVersion, cursor string) ([]byte, error) {
	pt, err := strconv.ParseUint(cursor, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "cursor is not in toid format")
	}

	var b [9]byte
	b[0] = byte(v)
	binary.BigEndian.PutUint64(b[1:], pt)
	return b[:], nil
}

func OrderingKeyFromBlock(block uint64, max bool) []byte {
	var b [1 + 8 + 8]byte
	b[0] = byte(KinVersion_KIN4)
	binary.BigEndian.PutUint64(b[1:], block)

	if max {
		binary.BigEndian.PutUint64(b[9:], math.MaxUint64)
	}

	return b[:]
}

// GetAccountsFromEnvelope returns the set of accounts involved in a transaction
// contained within a transaction envelope.
func GetAccountsFromEnvelope(env xdr.TransactionEnvelope) (map[string]struct{}, error) {
	// Gather the list of all 'associated' accounts in the transaction.
	idSet := make(map[string]struct{})
	addr, err := env.Tx.SourceAccount.GetAddress()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get source addr")
	}
	idSet[addr] = struct{}{}

	for _, op := range env.Tx.Operations {
		if op.SourceAccount != nil {
			addr, err := op.SourceAccount.GetAddress()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get op source addr")
			}
			idSet[addr] = struct{}{}
		}

		switch op.Body.Type {
		case xdr.OperationTypePayment:
			if p, ok := op.Body.GetPaymentOp(); ok {
				addr, err := p.Destination.GetAddress()
				if err != nil {
					return nil, errors.Wrap(err, "failed to get source addr")
				}

				idSet[addr] = struct{}{}
			}
		case xdr.OperationTypeCreateAccount:
			if c, ok := op.Body.GetCreateAccountOp(); ok {
				addr, err := c.Destination.GetAddress()
				if err != nil {
					return nil, errors.Wrap(err, "failed to get source addr")
				}

				idSet[addr] = struct{}{}
			}
		case xdr.OperationTypeAccountMerge:
			if d, ok := op.Body.GetDestination(); ok {
				addr, err := d.GetAddress()
				if err != nil {
					return nil, errors.Wrap(err, "failed to get source addr")
				}

				idSet[addr] = struct{}{}
			}
		case xdr.OperationTypeChangeTrust:
			// No action; op has no account
		default:
			logrus.StandardLogger().WithFields(logrus.Fields{
				"type":   "transaction/history/ingestion/stellar",
				"method": "GetAccountsFromEnvelope",
			}).Warn("Unsupported transaction type, unable to get relevant accounts")
		}
	}

	return idSet, nil
}

func GetAccountsFromTransaction(txn solana.Transaction) (map[string]struct{}, error) {
	idSet := make(map[string]struct{})

	for i := range txn.Message.Instructions {
		transfer, err := token.DecompileTransferAccount(txn.Message, i)
		if err == solana.ErrIncorrectProgram || err == solana.ErrIncorrectInstruction {
			continue
		} else if err != nil {
			return nil, errors.Wrap(err, "failed to decompile transfer instruction")
		}

		idSet[accountFromRaw(transfer.Source)] = struct{}{}
		idSet[accountFromRaw(transfer.Destination)] = struct{}{}
	}

	return idSet, nil
}

func accountFromRaw(raw ed25519.PublicKey) string {
	return strkey.MustEncode(strkey.VersionByteAccountID, raw)
}
