package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/xdr"
)

func (m *Entry) GetTxHash() ([]byte, error) {
	switch v := m.Kind.(type) {
	case *Entry_Stellar:
		var env xdr.TransactionEnvelope
		if _, err := xdr.Unmarshal(bytes.NewReader(v.Stellar.EnvelopeXdr), &env); err != nil {
			return nil, errors.Wrap(err, "failed to parse envelope xdr")
		}

		txBytes, err := env.Tx.MarshalBinary()
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal tx")
		}

		hash := sha256.Sum256(txBytes)
		return hash[:], nil
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

		// Gather the list of all 'associated' accounts in the transaction.
		idSet := make(map[string]struct{})
		addr, err := env.Tx.SourceAccount.GetAddress()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get source addr")
		}
		idSet[addr] = struct{}{}

		for _, op := range env.Tx.Operations {
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
					"type":   "transaction/history/model",
					"method": "GetAccounts",
				}).Warn("Unsupported transaction type, unable to get relevant accounts")
			}
		}

		// Convert back into a slice and return
		accountIDs := make([]string, 0, len(idSet))
		for k := range idSet {
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
		return OrderKeyFromSequence(m.Version, uint32(v.Stellar.Ledger)), nil
	default:
		return nil, errors.Errorf("unsupported entry version: %d", m.Version)
	}
}

func OrderKeyFromSequence(v KinVersion, seq uint32) []byte {
	var b [5]byte
	b[0] = byte(v)
	binary.BigEndian.PutUint32(b[1:], uint32(seq))
	return b[:]
}
