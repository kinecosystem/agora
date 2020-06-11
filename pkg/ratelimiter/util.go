package ratelimiter

import (
	"github.com/go-redis/redis_rate/v8"
	"github.com/pkg/errors"
)

// CanProceed indicates whether the action corresponding to the provided rate limit key can proceed based on the
// provided limit.
func CanProceed(rl *redis_rate.Limiter, rateLimitKey string, limitPerSec int) (bool, error) {
	result, err := rl.Allow(rateLimitKey, redis_rate.PerSecond(limitPerSec))
	if err != nil {
		return false, errors.Wrap(err, "failed to check rate limit")
	}
	return result.Allowed, nil
}
