package backoff

import (
	"context"
	"math/rand"
	"time"
)

func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(d)))
}

func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func Next(current, max time.Duration) time.Duration {
	if current >= max {
		return max
	}

	next := current * 2
	if next > max {
		return max
	}

	return next
}
