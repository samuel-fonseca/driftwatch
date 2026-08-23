package backoff

import (
	"context"
	"testing"
	"time"
)

func TestJitterNonPositiveReturnsZero(t *testing.T) {
	if got := Jitter(0); got != 0 {
		t.Errorf("Jitter(0) = %v, want 0", got)
	}
	if got := Jitter(-time.Second); got != 0 {
		t.Errorf("Jitter(-1s) = %v, want 0", got)
	}
}

func TestJitterIsWithinBounds(t *testing.T) {
	max := 100 * time.Millisecond
	for range 1000 {
		got := Jitter(max)
		if got < 0 || got >= max {
			t.Fatalf("Jitter(%v) = %v, want value in [0, %v)", max, got, max)
		}
	}
}

func TestJitterVaries(t *testing.T) {
	max := time.Hour
	first := Jitter(max)
	for range 100 {
		if Jitter(max) != first {
			return
		}
	}
	t.Errorf("Jitter(%v) returned %v every time across 100 calls, want variation", max, first)
}

func TestSleepReturnsNilAfterDuration(t *testing.T) {
	err := Sleep(context.Background(), time.Millisecond)
	if err != nil {
		t.Errorf("Sleep() = %v, want nil", err)
	}
}

func TestSleepReturnsContextErrorWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Sleep(ctx, time.Hour)
	if err != context.Canceled {
		t.Errorf("Sleep() = %v, want %v", err, context.Canceled)
	}
}

func TestSleepReturnsDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := Sleep(ctx, time.Hour)
	if err != context.DeadlineExceeded {
		t.Errorf("Sleep() = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestNextDoublesCurrent(t *testing.T) {
	got := Next(10*time.Millisecond, time.Second)
	want := 20 * time.Millisecond
	if got != want {
		t.Errorf("Next(10ms, 1s) = %v, want %v", got, want)
	}
}

func TestNextCapsAtMaxWhenDoublingExceedsIt(t *testing.T) {
	got := Next(600*time.Millisecond, time.Second)
	want := time.Second
	if got != want {
		t.Errorf("Next(600ms, 1s) = %v, want %v", got, want)
	}
}

func TestNextReturnsMaxWhenCurrentAtMax(t *testing.T) {
	got := Next(time.Second, time.Second)
	if got != time.Second {
		t.Errorf("Next(1s, 1s) = %v, want 1s", got)
	}
}

func TestNextReturnsMaxWhenCurrentExceedsMax(t *testing.T) {
	got := Next(2*time.Second, time.Second)
	if got != time.Second {
		t.Errorf("Next(2s, 1s) = %v, want 1s", got)
	}
}
