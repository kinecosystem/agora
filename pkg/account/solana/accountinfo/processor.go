package accountinfo

import (
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/metrics"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/pkg/errors"
)

const IngestorName = "accountinfo_ingestor"

var (
	stateBalanceNegative = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "processor_state_balance_negative",
		Help:      "Number of times a state has a negative balance",
	})
)

func init() {
	stateBalanceNegative = metrics.Register(stateBalanceNegative).(prometheus.Counter)
}

type StateProcessor struct {
	log   *logrus.Entry
	store StateStore
	sc    solana.Client
	tc    *token.Client
}

func NewStateProcessor(
	stateStore StateStore,
	sc solana.Client,
	tc *token.Client,
) *StateProcessor {
	return &StateProcessor{
		log:   logrus.StandardLogger().WithField("type", "accountinfo/StateProcessor"),
		store: stateStore,
		sc:    sc,
		tc:    tc,
	}
}

func (p *StateProcessor) UpdateState(ctx context.Context, sc history.StateChange) (err error) {
	log := p.log.WithField("method", "updateAccounts")
	slot, err := model.BlockFromOrderingKey(sc.LastKey)
	if err != nil {
		return errors.Wrap(err, "failed to get block from last key")
	}

	newAccounts := getNewAccounts(sc.Creations)
	balanceDiffs := getBalanceDiffs(sc.Payments)

	updateMap := make(map[string]*State)
	for account, owner := range newAccounts {
		updateMap[account] = &State{
			Account: []byte(account),
			Owner:   owner,
			Balance: 0,
			Slot:    slot,
		}
	}

	fetchRequired := make(map[string]struct{})
	for account, diff := range balanceDiffs {
		if state, ok := updateMap[account]; ok {
			state.Balance = state.Balance + diff
			continue
		}

		storedState, err := p.store.Get(ctx, []byte(account))
		if err == ErrNotFound {
			fetchRequired[account] = struct{}{}
		} else if err != nil {
			log.WithError(err).Warn("failed to get account state from account state store")
			return errors.Wrap(err, "failed to get account state from account state store")
		} else {
			// Don't overwrite if our slot is behind the stored slot
			if storedState.Slot >= slot {
				continue
			}

			storedState.Balance = storedState.Balance + diff
			storedState.Slot = slot
			updateMap[account] = storedState
			continue
		}

	}

	for _, oc := range sc.OwnershipChanges {
		if state, ok := updateMap[string(oc.Address)]; ok {
			state.Owner = oc.Owner
		}

		storedState, err := p.store.Get(ctx, oc.Address)
		if err == ErrNotFound {
			fetchRequired[string(oc.Address)] = struct{}{}
		} else if err != nil {
			log.WithError(err).Warn("failed to get account state from account state store")
			return errors.Wrap(err, "failed to get account state from account state store")
		} else {
			// Don't overwrite if our slot is behind the stored slot
			if storedState.Slot >= slot {
				continue
			}

			storedState.Owner = oc.Owner
			storedState.Slot = slot
			updateMap[string(oc.Address)] = storedState
			continue
		}
	}

	for _, state := range updateMap {
		if state.Balance < 0 {
			// An incorrect assumption was made above; meter + mark the account for a fetch
			stateBalanceNegative.Inc()
			fetchRequired[string(state.Account)] = struct{}{}
			continue
		}

		err := p.store.Put(ctx, state)
		if err != nil {
			log.WithError(err).Warn("failed to put account state")

			// Ensure entry is deleted since it will be invalid
			err = p.store.Delete(ctx, state.Account)
			if err != nil {
				log.WithError(err).Warn("failed to delete outdated account state")
			}
			return errors.Wrap(err, "failed to update account state")
		}
	}

	// We get slot prior to getting account to make sure we don't miss an update.
	// If we were to switch the order, there is a risk of missing updates since the
	// fetched slot is more likely to be later than the slot at which the acocunt was fetched.
	var committedSlot uint64
	if len(fetchRequired) > 0 {
		committedSlot, err = p.sc.GetSlot(solana.CommitmentMax)
		if err != nil {
			log.WithError(err).Warn("failed to get slot")
			return errors.Wrap(err, "failed to get slot")
		}
	}
	for account := range fetchRequired {
		a, err := p.tc.GetAccount(ed25519.PublicKey(account), solana.CommitmentMax)
		if err != nil {
			log.WithError(err).Warn("failed to get account from Solana")
			return errors.Wrap(err, "failed to get account from Solana")
		}

		if err := p.store.Put(ctx, &State{
			Account: ed25519.PublicKey(account),
			Owner:   a.Owner,
			Balance: int64(a.Amount),
			Slot:    committedSlot,
		}); err != nil {
			log.WithError(err).Warn("failed to put account state")
			return errors.Wrap(err, "failed to update account state")
		}
	}

	return nil
}

func getBalanceDiffs(payments []*history.Payment) map[string]int64 {
	diffMap := make(map[string]int64)

	// update map with any payments
	for _, p := range payments {
		diffMap[string(p.Source)] -= int64(p.Quarks)
		diffMap[string(p.Dest)] += int64(p.Quarks)
	}

	return diffMap
}

func getNewAccounts(creations []*history.Creation) map[string]ed25519.PublicKey {
	accounts := make(map[string]ed25519.PublicKey)
	for _, c := range creations {
		accounts[string(c.Account)] = c.AccountOwner
	}

	return accounts
}
