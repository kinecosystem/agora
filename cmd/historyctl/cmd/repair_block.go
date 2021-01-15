package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

// blockRepairer repairs missing block in the tx-by-X tables.
type blockRepairer struct {
	log *logrus.Entry
	sc  solana.Client
	rw  history.ReaderWriter
}

func newBlockRepairer(
	sc solana.Client,
	rw history.ReaderWriter,
) *blockRepairer {
	return &blockRepairer{
		log: logrus.StandardLogger().WithField("type", "repairer"),
		sc:  sc,
		rw:  rw,
	}
}

func (r *blockRepairer) Repair(start, end uint64) error {
	log := r.log.WithFields(logrus.Fields{
		"method": "Repair",
		"start":  start,
		"end":    end,
	})

	repairFileName := fmt.Sprintf("repair-%d-%d.json", start, end)

	var repairFile *os.File
	_, err := os.Stat(repairFileName)
	if os.IsNotExist(err) {
		repairFile, err = os.Create(fmt.Sprintf("%s.tmp", repairFileName))
		if err != nil {
			return errors.Wrap(err, "failed to open repair file")
		}
	} else {
		return nil
	}
	defer func() {
		if repairFile != nil {
			repairFile.Close()
		}
	}()

	const pageSize = 1000
	startKey := model.OrderingKeyFromBlock(start)
	endKey := model.OrderingKeyFromBlock(end)
	for i := 0; ; i++ {
		entries, err := r.rw.GetTransactions(context.Background(), startKey, endKey, pageSize)
		if err != nil {
			return errors.Wrapf(err, "failed to get transactions over [%d, %d)", model.MustBlockFromOrderingKey(startKey), model.MustBlockFromOrderingKey(endKey))
		}

		for _, e := range entries {
			se := e.GetSolana()
			if se == nil {
				log.Warn("non-solana entry detected in history")
				continue
			}

			txID, err := e.GetTxID()
			if err != nil {
				return errors.Wrap(err, "failed to get transaction id")
			}

			entry, err := r.rw.GetTransaction(context.Background(), txID)
			if err != nil {
				return errors.Wrap(err, "failed to get entry direct")
			}

			if entry.GetSolana().GetSlot() != 0 {
				continue
			}

			if !dryRun {
				if err := r.rw.Write(context.Background(), e); err != nil {
					return errors.Wrap(err, "failed to write")
				}
			}

			repair := &struct {
				TxID string `json:"tx_id"`
				Slot uint64 `json:"slot"`
			}{
				Slot: se.Slot,
				TxID: base58.Encode(txID),
			}
			out, err := json.Marshal(repair)
			if err != nil {
				return errors.Wrap(err, "failed to marshal repair report")
			}
			if _, err := repairFile.Write(append(out, '\n')); err != nil {
				return errors.Wrap(err, "failed to write to repair file")
			}
		}

		log.WithFields(logrus.Fields{
			"sk": model.MustBlockFromOrderingKey(startKey),
			"ek": model.MustBlockFromOrderingKey(endKey),
			"ec": len(entries),
		}).Debug("Processed range")

		if len(entries) < pageSize {
			break
		}

		lastProcessed, err := entries[len(entries)-1].GetOrderingKey()
		if err != nil {
			return errors.Wrapf(err, "failed to get ordering key")
		}
		startKey = append(lastProcessed, 0)
	}

	if repairFile != nil {
		repairFile.Close()
		repairFile = nil
		if err := os.Rename(fmt.Sprintf("%s.tmp", repairFileName), repairFileName); err != nil {
			log.WithError(err).Warn("failed to rename file")
		}
	}

	return nil
}
