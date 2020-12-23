package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// refDate is a few days before the migration date.
// all history entries should be after this point in time.
var refDate = time.Date(2020, 12, 14, 0, 0, 0, 0, time.Now().UTC().Location())

type repairer struct {
	log *logrus.Entry
	sc  solana.Client
	rw  history.ReaderWriter
}

func newRepairer(sc solana.Client, rw history.ReaderWriter) *repairer {
	return &repairer{
		log: logrus.StandardLogger().WithField("type", "repairer"),
		sc:  sc,
		rw:  rw,
	}
}

func (r *repairer) Repair(start, end uint64) error {
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

	// If the blocktime isn't set, we try to interpolate the value by scanning
	// for the nearest time in either direction, and guessing where we would be
	var lowerBound, upperBound uint64
	var lowerBoundTime, upperBoundTime time.Time

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

			if !se.BlockTime.AsTime().Before(refDate) {
				lowerBound = se.Slot
				lowerBoundTime = se.BlockTime.AsTime()
				continue
			}

			blockTime, err := r.sc.GetBlockTime(se.Slot)
			if err == solana.ErrBlockNotAvailable {
				log.WithField("slot", se.Slot).Warn("block not available for repair")
			} else if err != nil {
				return errors.Wrap(err, "failed to get block time")
			}

			// If there is no blockchain data, attempt to interpolate.
			if blockTime.Before(refDate) {
				if lowerBound == 0 {
					// this rarely occurs, and the logic gets a bit more hairy to reverse sort.Search.
					return errors.New("no lower bound encountered, please expand range")
				}

				if upperBound < se.Slot {
					upperBound, upperBoundTime, err = r.findNearestUpperBound(se.Slot, end+pageSize)
					if err != nil {
						return errors.Wrap(err, "failed to find nearest upper bound")
					}
				}

				blockTime = getApproximateBlockTime(lowerBound, upperBound, se.Slot, lowerBoundTime, upperBoundTime)
			} else {
				lowerBound = se.Slot
				lowerBoundTime = se.BlockTime.AsTime()
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
				}).Info("Repairing")
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
				Slot:           se.Slot,
				TxID:           base58.Encode(txID),
				LowerBound:     lowerBound,
				LowerBoundTime: lowerBoundTime,
				UpperBound:     upperBound,
				UpperBoundTime: upperBoundTime,
				BlockTime:      blockTime,
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

func (r *repairer) findNearestUpperBound(slot, maxSearchBlock uint64) (upper uint64, t time.Time, err error) {
	const searchWindow = 1000

	offset := sort.Search(int(maxSearchBlock-slot), func(n int) bool {
		s := uint64(n) + slot
		log := r.log.WithFields(logrus.Fields{
			"search_slot":     s,
			"target_slot":     slot,
			"max_search_slot": maxSearchBlock,
		})
		log.Debug("Searching for upper bound")

		sk := model.OrderingKeyFromBlock(s)
		ek := model.OrderingKeyFromBlock(s + searchWindow)
		var entries []*model.Entry
		entries, err = r.rw.GetTransactions(context.Background(), sk, ek, 1)
		if err != nil {
			log.Warn("failed to lookup entries")
			return false
		}
		if len(entries) == 0 {
			log.Warn("no entries in block")
		}

		for _, e := range entries {
			if refDate.Before(e.GetSolana().BlockTime.AsTime()) {
				log.Info("Found repair time")
				return true
			}
		}

		return false
	})
	upper = uint64(offset) + slot
	if upper >= maxSearchBlock {
		return 0, time.Time{}, errors.New("failed to find an upper bound")
	}

	sk := model.OrderingKeyFromBlock(upper)
	ek := model.OrderingKeyFromBlock(upper + searchWindow)
	entries, err := r.rw.GetTransactions(context.Background(), sk, ek, 1)
	if err != nil {
		return 0, time.Time{}, errors.New("failed to find retrieve entries from upper bound")
	}
	if len(entries) == 0 {
		return 0, time.Time{}, errors.Errorf("found slot [%d,%d) does not contain entries", upper, upper+searchWindow)
	}

	return upper, entries[0].GetSolana().BlockTime.AsTime(), nil
}

func getApproximateBlockTime(lower, upper, slot uint64, lowerTime, upperTime time.Time) time.Time {
	approxRate := float64(upperTime.Sub(lowerTime)) / float64(upper-lower)
	return lowerTime.Add(time.Duration(approxRate * float64(slot-lower)))
}
