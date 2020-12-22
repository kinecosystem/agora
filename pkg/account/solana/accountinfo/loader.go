package accountinfo

import (
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/metrics"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/prometheus/client_golang/prometheus"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount"
)

var (
	infoLoaderHits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "info_loader_hits",
	}, []string{"negative_hit"})
	infoLoaderMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "info_loader_misses",
	})
	infoLoaderLoadFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "info_loader_load_failures",
	}, []string{"type"})
	infoLoaderWritebackFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "info_loader_writeback_failures",
	}, []string{"cache"})
)

func init() {
	infoLoaderHits = metrics.Register(infoLoaderHits).(*prometheus.CounterVec)
	infoLoaderMisses = metrics.Register(infoLoaderMisses).(prometheus.Counter)
	infoLoaderLoadFailures = metrics.Register(infoLoaderLoadFailures).(*prometheus.CounterVec)
	infoLoaderWritebackFailures = metrics.Register(infoLoaderWritebackFailures).(*prometheus.CounterVec)
}

type Loader struct {
	tc         *token.Client
	cache      Cache
	tokenCache tokenaccount.Cache
}

func NewLoader(
	tc *token.Client,
	cache Cache,
	tokenCache tokenaccount.Cache,
) *Loader {
	return &Loader{
		tc:         tc,
		cache:      cache,
		tokenCache: tokenCache,
	}
}

func (l *Loader) Load(ctx context.Context, account ed25519.PublicKey, commitment solana.Commitment) (*accountpb.AccountInfo, error) {
	info, err := l.cache.Get(ctx, account)
	if err != nil && err != ErrAccountInfoNotFound {
		infoLoaderLoadFailures.WithLabelValues("cache").Inc()
	}

	if info != nil {
		if info.Balance < 0 {
			infoLoaderHits.WithLabelValues("true").Inc()
			return nil, ErrAccountInfoNotFound
		} else {
			infoLoaderHits.WithLabelValues("false").Inc()
		}

		return info, nil
	}

	infoLoaderMisses.Inc()

	acc, err := l.tc.GetAccount(account, commitment)
	if err != nil {
		switch err {
		case token.ErrAccountNotFound:
			infoLoaderLoadFailures.WithLabelValues("not_found").Inc()
		case token.ErrInvalidTokenAccount:
			infoLoaderLoadFailures.WithLabelValues("invalid").Inc()
		default:
			infoLoaderLoadFailures.WithLabelValues("unknown").Inc()
		}

		// If it's an unexpected error, just propagate it.
		//
		// Otherwise, perform a negative writeback
		if err != token.ErrAccountNotFound && err != token.ErrInvalidTokenAccount {
			return nil, err
		}

		negativeInfo := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: account,
			},
			Balance: -1,
		}
		if err := l.cache.Put(ctx, negativeInfo); err != nil {
			infoLoaderWritebackFailures.WithLabelValues("accountinfo").Inc()
		}

		return nil, ErrAccountInfoNotFound
	}

	info = &accountpb.AccountInfo{
		AccountId: &commonpb.SolanaAccountId{
			Value: account,
		},
		Balance: int64(acc.Amount),
	}

	if err := l.cache.Put(ctx, info); err != nil {
		infoLoaderWritebackFailures.WithLabelValues("accountinfo").Inc()
	}
	if err := l.tokenCache.Put(ctx, acc.Owner, []ed25519.PublicKey{account}); err != nil {
		infoLoaderWritebackFailures.WithLabelValues("tokenaccount").Inc()
	}

	return info, nil
}
