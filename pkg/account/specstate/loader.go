package specstate

import (
	"context"
	"crypto/ed25519"
	"sync"

	"github.com/kinecosystem/agora-common/metrics"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account/info"
)

type Loader struct {
	log       *logrus.Entry
	tc        *token.Client
	infoCache info.Cache
}

func NewSpeculativeLoader(
	tc *token.Client,
	cache info.Cache,
) *Loader {
	return &Loader{
		log:       logrus.StandardLogger().WithField("type", "account/info/speculative_loader"),
		tc:        tc,
		infoCache: cache,
	}
}

// Load loads existing account states, and merges it with the provided balance updates.
//
// Currently, Load uses a chained strategy, whereby we re-use the speculations
// from previous executions if they exist before using on chain information. This approach
// has some drawbacks, but it is sufficient for what we want in the current 'state of kin'.
//
// Benefits: This approach is fairly simple, and solves the problem of doing "rapid" transfers
// in succession. Currently, the time from Submit() to the effect being visible on chain tends
// to be from 5 - 30 seconds. If we were to submit multiple transfers for the same account using
// the block chain information (inside this window), the speculations would all be the same,
// rather than being additive. By using the cached values first, we get the additive property.
//
// Cons: If the transaction is rolled back, or external (to agora) transactions are submitted,
// then our speculations will be wrong. Worse, if we continually speculate within the infoCache window,
// we will never have a chance to resync with/rebase off of the block chain state. Since everyone
// who uses agora's GetAccountBalance() also uses agoras Submit() (currently), we don't care as
// much about the latter case. In the former case, we rely (maybe incorrectly) on:
//
//  1. The simulation phase of the transaction will yield any early failures, reducing the chance
//     of rollback at a later point in time.
//  2. Regular observers of accounts don't have extended transfer 'sessions'. For example, a user may
//     receive and earn, open the app, and send it back. Or they may just send kin to a few places. This
//     activity is likely to occur within a small time window, with a decent (infoCache expiry+) gap between
//     the next window, giving us a chance to rsync.
//
// (2)'s caveat is developer wallets (wallet that have a sustained transfers) are at risk of divergence
// from our API. So far they mostly been using explorer, but they also are more tolerant.
//
// Finally, transactions never use our data, so a user still cannot send kin they do not have.
//
// Future work would involve recording speculative predictions as diffs, and periodically re-syncing
// with the underlying blockchain, and discarding all speculations before the sync (based on slot.)
func (s *Loader) Load(ctx context.Context, balanceUpdates map[string]int64, commitment solana.Commitment) (newState map[string]int64) {
	log := s.log.WithField("method", "Load")

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(balanceUpdates))

	newState = make(map[string]int64)

	for accountKey, diff := range balanceUpdates {
		go func(a string, b int64) {
			defer wg.Done()

			ai, err := s.infoCache.Get(ctx, ed25519.PublicKey(a))
			if err != nil && err != info.ErrAccountInfoNotFound {
				speculativeUpdateFailures.WithLabelValues("info_cache_load").Inc()
				infoCacheFailures.WithLabelValues("speculative_load").Inc()
				log.WithError(err).Warn("failed to load infoCache info for speculative execution")
			}

			if ai != nil {
				mu.Lock()
				newState[a] += ai.Balance + b
				mu.Unlock()
				return
			}

			account, err := s.tc.GetAccount(ed25519.PublicKey(a), commitment)
			if err == token.ErrAccountNotFound || err == token.ErrInvalidTokenAccount {
				return
			} else if err != nil {
				speculativeUpdateFailures.WithLabelValues("solana_load").Inc()
				log.WithError(err).Warn("failed to load tokenAccount info for speculative execution")
				return
			} else if account == nil {
				speculativeUpdateFailures.WithLabelValues("solana_load_empty").Inc()
				return
			}

			mu.Lock()
			newState[a] += int64(account.Amount) + b
			mu.Unlock()
		}(accountKey, diff)
	}

	wg.Wait()
	return newState
}

func (s *Loader) Write(ctx context.Context, speculativeStates map[string]int64) {
	log := s.log.WithField("method", "Write")

	var wg sync.WaitGroup
	wg.Add(len(speculativeStates))

	for accountKey, balance := range speculativeStates {
		go func(a string, b int64) {
			defer wg.Done()

			err := s.infoCache.Put(ctx, &accountpb.AccountInfo{
				AccountId: &commonpb.SolanaAccountId{
					Value: []byte(a),
				},
				Balance: b,
			})
			if err != nil {
				speculativeUpdateFailures.WithLabelValues("speculative_write").Inc()
				log.WithError(err).Warn("failed to update info infoCache")
			} else {
				speculativeUpdates.Inc()
			}
		}(accountKey, balance)
	}

	wg.Wait()
}

var (
	speculativeUpdates = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "speculative_updates",
	})
	speculativeUpdateFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "speculative_update_failures",
	}, []string{"type"})
	infoCacheFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "info_cache_failures",
	}, []string{"type"})
)

func init() {
	speculativeUpdates = metrics.Register(speculativeUpdates).(prometheus.Counter)
	speculativeUpdateFailures = metrics.Register(speculativeUpdateFailures).(*prometheus.CounterVec)
	infoCacheFailures = metrics.Register(infoCacheFailures).(*prometheus.CounterVec)
}
