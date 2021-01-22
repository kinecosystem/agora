package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

const (
	driftStartBlock = 59053920
	driftEndBlock   = 60912004
)

var (
	// driftStartTime is the date corresponding to driftStartBlock
	driftStartTime = time.Unix(1609753027, 0)
	// driftEndTime is the date corresponding to driftEndBlock
	driftEndTime = time.Unix(1610804314, 0)
)

// driftRepairer repairs the timestamp drift that occurred
// over [59053920, 60912004]
type driftRepairer struct {
	log *logrus.Entry
	rw  history.ReaderWriter
}

func newDriftRepairer(rw history.ReaderWriter) *driftRepairer {
	return &driftRepairer{
		log: logrus.StandardLogger().WithField("type", "repairer"),
		rw:  rw,
	}
}

func (r *driftRepairer) Repair(start, end uint64) error {
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

			if se.Slot < driftStartBlock {
				continue
			}
			if se.Slot >= driftEndBlock {
				return nil
			}

			blockTime := getApproximateBlockTime(driftStartBlock, driftEndBlock, se.Slot, driftStartTime, driftEndTime)
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
					"time": asTime(se.BlockTime),
				}).Debug("Repairing")
				if err := r.rw.Write(context.Background(), e); err != nil {
					return errors.Wrap(err, "failed to update entry")
				}
			}

			repair := &struct {
				Slot      uint64    `json:"slot"`
				TxID      string    `json:"tx_id"`
				BlockTime time.Time `json:"block_time"`
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

func getApproximateBlockTime(lower, upper, slot uint64, lowerTime, upperTime time.Time) time.Time {
	approxRate := float64(upperTime.Sub(lowerTime)) / float64(upper-lower)
	return lowerTime.Add(time.Duration(approxRate * float64(slot-lower)))
}
