package backoff

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestJitter(t *testing.T) {
	t.Run("non-positive durations return zero", func(t *testing.T) {
		for _, d := range []time.Duration{0, -time.Nanosecond, -time.Second} {
			if got := Jitter(d); got != 0 {
				t.Errorf("Jitter(%v) = %v, want 0", d, got)
			}
		}
	})

	t.Run("stays within [0, d)", func(t *testing.T) {
		const d = 100 * time.Millisecond
		for range 1000 {
			if got := Jitter(d); got < 0 || got >= d {
				t.Fatalf("Jitter(%v) = %v, want a value in [0, %v)", d, got, d)
			}
		}
	})

	// Jitter exists to stop every venue's poller retrying in lockstep after
	// a shared outage, so a constant return would defeat its only purpose.
	t.Run("varies across calls", func(t *testing.T) {
		const d = time.Hour
		first := Jitter(d)
		for range 100 {
			if Jitter(d) != first {
				return
			}
		}
		t.Errorf("Jitter(%v) returned %v on all 100 calls, want variation", d, first)
	})
}

func TestNext(t *testing.T) {
	cases := []struct {
		name         string
		current, max time.Duration
		want         time.Duration
	}{
		{"doubles below the ceiling", 10 * time.Millisecond, time.Second, 20 * time.Millisecond},
		{"caps when doubling would exceed the ceiling", 600 * time.Millisecond, time.Second, time.Second},
		{"holds at the ceiling", time.Second, time.Second, time.Second},
		{"clamps back down from above the ceiling", 2 * time.Second, time.Second, time.Second},
		// Doubling zero stays zero forever, so a caller that starts from an
		// unset backoff would retry in a hot loop. poller.New defends against
		// that by applying Tuning defaults; this pins the arithmetic that
		// makes the defence necessary.
		{"zero stays zero", 0, time.Second, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Next(c.current, c.max); got != c.want {
				t.Errorf("Next(%v, %v) = %v, want %v", c.current, c.max, got, c.want)
			}
		})
	}
}

// Next must never hand back more than max, whatever it is given.
func TestNextNeverExceedsMax(t *testing.T) {
	const max = 60 * time.Second

	current := time.Millisecond
	for range 100 {
		current = Next(current, max)
		if current > max {
			t.Fatalf("Next escalated to %v, above the %v ceiling", current, max)
		}
	}
	if current != max {
		t.Errorf("after 100 doublings Next settled at %v, want the %v ceiling", current, max)
	}
}

func TestSleep(t *testing.T) {
	t.Run("returns nil once the duration elapses", func(t *testing.T) {
		if err := Sleep(context.Background(), time.Millisecond); err != nil {
			t.Errorf("Sleep() = %v, want nil", err)
		}
	})

	// A poller parked in a long backoff must abandon it the moment the
	// process is shutting down, rather than holding shutdown open.
	cases := []struct {
		name string
		ctx  func(t *testing.T) context.Context
		want error
	}{
		{
			name: "cancelled context",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			want: context.Canceled,
		},
		{
			name: "expired deadline",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
			want: context.DeadlineExceeded,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			start := time.Now()
			err := Sleep(c.ctx(t), time.Hour)

			if !errors.Is(err, c.want) {
				t.Errorf("Sleep() = %v, want %v", err, c.want)
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Errorf("Sleep took %v to abandon a 1h wait, want it to return promptly", elapsed)
			}
		})
	}
}
