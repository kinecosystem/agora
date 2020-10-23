package rate

import (
	"sync"

	"github.com/go-redis/redis_rate/v8"
	"golang.org/x/time/rate"
)

// Limiter limits operations based on a provided key.
type Limiter interface {
	Allow(key string) (bool, error)
}

// LimiterCtor allows the creation of a Limiter using a provided rate.
type LimiterCtor func(rate int) Limiter

type redisRateLimiter struct {
	l     *redis_rate.Limiter
	limit *redis_rate.Limit
}

// NewRedisRateLimiter returns a redis backed limiter.
func NewRedisRateLimiter(limiter *redis_rate.Limiter, limit *redis_rate.Limit) Limiter {
	return &redisRateLimiter{
		l:     limiter,
		limit: limit,
	}
}

// Allow implements limiter.Allow.
func (r *redisRateLimiter) Allow(key string) (bool, error) {
	result, err := r.l.Allow(key, r.limit)
	if err != nil {
		return false, err
	}

	return result.Allowed, nil
}

type localRateLimiter struct {
	limit rate.Limit

	sync.Mutex
	limiters map[string]*rate.Limiter
}

// NewRedisRateLimiter returns an in memory limiter.
func NewLocalRateLimiter(limit rate.Limit) Limiter {
	return &localRateLimiter{
		limit:    limit,
		limiters: make(map[string]*rate.Limiter),
	}
}

// Allow implements limiter.Allow.
func (l *localRateLimiter) Allow(key string) (bool, error) {
	l.Lock()
	limiter, ok := l.limiters[key]
	if !ok {
		limiter = rate.NewLimiter(l.limit, int(l.limit))
		l.limiters[key] = limiter
	}
	l.Unlock()

	return limiter.Allow(), nil
}

// NoLimiter never limits operations
type NoLimiter struct {
}

// Allow implements limiter.Allow.
func (n *NoLimiter) Allow(key string) (bool, error) {
	return true, nil
}
