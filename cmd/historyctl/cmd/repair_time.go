package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

// timeRepairer repairs missing timestamps.
type timeRepairer struct {
	log *logrus.Entry
	sc  solana.Client
	rw  history.ReaderWriter
}

func newTimeRepairer(sc solana.Client, rw history.ReaderWriter) *timeRepairer {
	return &timeRepairer{
		log: logrus.StandardLogger().WithField("type", "repairer"),
		sc:  sc,
		rw:  rw,
	}
}

func (r *timeRepairer) Repair(start, end uint64) error {
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

			if !se.BlockTime.AsTime().IsZero() && se.BlockTime.AsTime().Unix() != 0 {
				continue
			}

			blockTime, err := r.sc.GetBlockTime(se.Slot)
			if err == solana.ErrBlockNotAvailable {
				log.WithField("slot", se.Slot).Warn("block not available for repair")
			} else if err != nil {
				return errors.Wrap(err, "failed to get block time")
			}

			newTs, err := ptypes.TimestampProto(blockTime)
			if err != nil {
				return errors.Wrap(err, "failed to marshal block time")
			}

			txID, _ := e.GetTxID()
			se.BlockTime = newTs
			if !dryRun {
				log.WithFields(logrus.Fields{
					"tx":   base58.Encode(txID),
					"slot": se.Slot,
					"time": se.BlockTime.AsTime(),
				}).Debug("Repairing")
				if err := r.rw.Write(context.Background(), e); err != nil {
					return errors.Wrap(err, "failed to update entry")
				}
			}

			repair := &struct {
				Slot           uint64    `json:"slot"`
				TxID           string    `json:"tx_id"`
				LowerBound     uint64    `json:"lower_bound"`
				LowerBoundTime time.Time `json:"lower_bound_time"`
				UpperBound     uint64    `json:"upper_bound"`
				UpperBoundTime time.Time `json:"upper_bound_time"`
				BlockTime      time.Time `json:"block_time"`
			}{
				Slot:      se.Slot,
				TxID:      base58.Encode(txID),
				BlockTime: blockTime,
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
