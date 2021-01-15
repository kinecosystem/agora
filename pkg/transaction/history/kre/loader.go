package kre

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/transaction/history"
)

type Submitter interface {
	Submit(ctx context.Context, src interface{}) (err error)
}

type Loader struct {
	log *logrus.Entry

	creationsSubmitter Submitter
	paymentsSubmitter  Submitter
	mapper             app.Mapper
}

const KREIngestorName = "kre_ingestor"

func NewLoader(
	creationsSubmitter Submitter,
	paymentsSubmitter Submitter,
	mapper app.Mapper,
) *Loader {
	return &Loader{
		log:                logrus.StandardLogger().WithField("type", "transaction/history/kre"),
		creationsSubmitter: creationsSubmitter,
		paymentsSubmitter:  paymentsSubmitter,
		mapper:             mapper,
	}
}

func (l *Loader) LoadData(sc history.StateChange) error {
	creations := make([]*history.Creation, len(sc.Creations))
	copy(creations, sc.Creations)
	payments := make([]*history.Payment, len(sc.Payments))
	copy(payments, sc.Payments)

	for i, c := range creations {
		if c.AppIndex != 0 || c.MemoText == nil {
			continue
		}

		if c.BlockTime.IsZero() || c.BlockTime.Unix() == 0 {
			return errors.Errorf("invalid date for %d", c.Block)
		}

		if appID, ok := transaction.AppIDFromTextMemo(*c.MemoText); ok {
			appIndex, err := l.mapper.GetAppIndex(context.Background(), appID)
			if err != nil && err != app.ErrMappingNotFound {
				return errors.Wrap(err, "failed to map app index")
			}
			cloned := *c
			cloned.AppIndex = int(appIndex)
			creations[i] = &cloned
		}
	}
	for i, p := range payments {
		if p.AppIndex != 0 || p.MemoText == nil {
			continue
		}

		if p.BlockTime.IsZero() || p.BlockTime.Unix() == 0 {
			return errors.Errorf("invalid date for %d", p.Block)
		}

		if appID, ok := transaction.AppIDFromTextMemo(*p.MemoText); ok {
			appIndex, err := l.mapper.GetAppIndex(context.Background(), appID)
			if err != nil && err != app.ErrMappingNotFound {
				return errors.Wrap(err, "failed to map app index")
			}
			cloned := *p
			cloned.AppIndex = int(appIndex)
			payments[i] = &cloned
		}
	}

	if len(sc.Creations) > 0 {
		if err := l.creationsSubmitter.Submit(context.Background(), creations); err != nil {
			return errors.Wrap(err, "failed to insert creations")
		}
	}
	if len(sc.Payments) > 0 {
		if err := l.paymentsSubmitter.Submit(context.Background(), payments); err != nil {
			return errors.Wrap(err, "failed to insert payments")
		}
	}

	return nil
}
