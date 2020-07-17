package model

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
)

func (m *Entry) GetTxHash() ([]byte, error) {
	switch v := m.Kind.(type) {
	case *Entry_Stellar:
		var env xdr.TransactionEnvelope
		if _, err := xdr.Unmarshal(bytes.NewReader(v.Stellar.EnvelopeXdr), &env); err != nil {
			return nil, errors.Wrap(err, "failed to parse envelope xdr")
		}

		hash, err := network.HashTransaction(&env.Tx, v.Stellar.NetworkPassphrase)
		return hash[:], err
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
		default:
			logrus.StandardLogger().WithFields(logrus.Fields{
				"type":   "transaction/history/ingestion/stellar",
				"method": "GetAccountsFromEnvelope",
			}).Warn("Unsupported transaction type, unable to get relevant accounts")
		}
	}

	return idSet, nil
}
