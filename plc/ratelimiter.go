package plc

import (
	"context"
	"time"
)

// RateLimiter implements a token bucket rate limiter
type RateLimiter struct {
	tokens     chan struct{}
	refillRate time.Duration
	maxTokens  int
	stopRefill chan struct{}
}

// NewRateLimiter creates a new rate limiter
// Example: NewRateLimiter(90, time.Minute) = 90 requests per minute
func NewRateLimiter(requestsPerPeriod int, period time.Duration) *RateLimiter {
	rl := &RateLimiter{
		tokens:     make(chan struct{}, requestsPerPeriod),
		refillRate: period / time.Duration(requestsPerPeriod),
		maxTokens:  requestsPerPeriod,
		stopRefill: make(chan struct{}),
	}

	// Fill initially
	for i := 0; i < requestsPerPeriod; i++ {
		rl.tokens <- struct{}{}
	}

	// Start refill goroutine
	go rl.refill()

	return rl
}

// refill adds tokens at the specified rate
func (rl *RateLimiter) refill() {
	ticker := time.NewTicker(rl.refillRate)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case rl.tokens <- struct{}{}:
				// Token added
			default:
				// Buffer full, skip
			}
		case <-rl.stopRefill:
			return
		}
	}
}

// Wait blocks until a token is available or context is cancelled
func (rl *RateLimiter) Wait(ctx context.Context) error {
	select {
	case <-rl.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop stops the rate limiter and cleans up resources
func (rl *RateLimiter) Stop() {
	close(rl.stopRefill)
}
