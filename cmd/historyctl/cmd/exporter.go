package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/kinecosystem/agora/pkg/transaction/history/processor"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type exporter struct {
	log    *logrus.Entry
	p      *processor.Processor
	mapper app.Mapper
}

func newExporter(p *processor.Processor, mapper app.Mapper) *exporter {
	return &exporter{
		log:    logrus.StandardLogger().WithField("type", "exporter"),
		p:      p,
		mapper: mapper,
	}
}

func (e *exporter) Export(start, end uint64) error {
	log := e.log.WithFields(logrus.Fields{
		"start":  start,
		"end":    end,
		"method": "Process",
	})
	ctx := context.Background()

	creationsFileName := fmt.Sprintf("creations-%d-%d.json", start, end)
	paymentsFileName := fmt.Sprintf("payments-%d-%d.json", start, end)
	var creationsFile, paymentsFile *os.File

	_, err := os.Stat(creationsFileName)
	if os.IsNotExist(err) {
		creationsFile, err = os.Create(fmt.Sprintf("%s.tmp", creationsFileName))
		if err != nil {
			return errors.Wrap(err, "failed to open creations file")
		}
	} else if err != nil {
		return errors.Wrapf(err, "failed to stat %s", creationsFileName)
	}
	defer func() {
		if creationsFile != nil {
			creationsFile.Close()
		}
	}()

	_, err = os.Stat(paymentsFileName)
	if os.IsNotExist(err) {
		paymentsFile, err = os.Create(fmt.Sprintf("%s.tmp", paymentsFileName))
		if err != nil {
			return errors.Wrap(err, "failed to open payments file")
		}
	} else if err != nil {
		return errors.Wrapf(err, "failed to stat %s", paymentsFileName)
	}
	defer func() {
		if paymentsFile != nil {
			paymentsFile.Close()
		}
	}()

	// If both files are complete, nothing to do.
	if creationsFile == nil && paymentsFile == nil {
		return nil
	}

	startKey := model.OrderingKeyFromBlock(start)
	endKey := model.OrderingKeyFromBlock(end)

	for i := 0; ; i++ {
		sc, err := e.p.ProcessRange(ctx, startKey, endKey, 1024)
		if err != nil {
			return errors.Wrapf(err, "failed to get transactions over [%d, %d)", model.MustBlockFromOrderingKey(startKey), model.MustBlockFromOrderingKey(endKey))
		}

		for _, c := range sc.Creations {
			if c.AppIndex == 0 && c.MemoText != nil {
				if appID, ok := transaction.AppIDFromTextMemo(*c.MemoText); ok {
					appIndex, err := e.mapper.GetAppIndex(ctx, appID)
					if err != nil && err != app.ErrMappingNotFound {
						return errors.Wrap(err, "failed to map app index")
					}
					c.AppIndex = int(appIndex)
				}
			}

			row, _, err := c.Save()
			if err != nil {
				return errors.Wrap(err, "failed to 'save' row")
			}
			rowBytes, err := json.Marshal(row)
			if err != nil {
				return errors.Wrap(err, "failed to marshal row")
			}
			if _, err := creationsFile.Write(rowBytes); err != nil {
				return errors.Wrap(err, "failed to write creations")
			}
			if _, err := creationsFile.Write([]byte("\n")); err != nil {
				return errors.Wrap(err, "failed to write creations")
			}
		}
		for _, p := range sc.Payments {
			if p.AppIndex == 0 && p.MemoText != nil {
				if appID, ok := transaction.AppIDFromTextMemo(*p.MemoText); ok {
					appIndex, err := e.mapper.GetAppIndex(ctx, appID)
					if err != nil && err != app.ErrMappingNotFound {
						return errors.Wrap(err, "failed to map app index")
					}
					p.AppIndex = int(appIndex)
				}
			}

			row, _, err := p.Save()
			if err != nil {
				return errors.Wrap(err, "failed to 'save' row")
			}
			rowBytes, err := json.Marshal(row)
			if err != nil {
				return errors.Wrap(err, "failed to marshal row")
			}
			if _, err := paymentsFile.Write(rowBytes); err != nil {
				return errors.Wrap(err, "failed to write payments")
			}
			if _, err := paymentsFile.Write([]byte("\n")); err != nil {
				return errors.Wrap(err, "failed to write payments")
			}
		}

		if sc.LastKey == nil {
			log.WithField("iterations", i).Debug("Finished processing range")
			break
		}

		startKey = append(sc.LastKey, 0)
	}

	if creationsFile != nil {
		creationsFile.Close()
		creationsFile = nil
		if err := os.Rename(fmt.Sprintf("%s.tmp", creationsFileName), creationsFileName); err != nil {
			log.WithError(err).Warn("failed to rename file")
		}
	}
	if paymentsFile != nil {
		paymentsFile.Close()
		paymentsFile = nil
		if err := os.Rename(fmt.Sprintf("%s.tmp", paymentsFileName), paymentsFileName); err != nil {
			log.WithError(err).Warn("failed to rename file")
		}
	}

	return nil
}
