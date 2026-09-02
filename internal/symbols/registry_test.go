package symbols

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- test helpers ---

// fakeLoader is the one loader stand-in these tests need: it counts calls,
// reports each one on a channel so Run tests can wait on real progress
// instead of sleeping, and serves a scripted sequence of errors. The last
// error in the sequence repeats once the rest are exhausted.
type fakeLoader struct {
	table Table
	errs  []error

	calls atomic.Int32
	ch    chan int
}

// newLoader serves a table containing symbol. With no errs it always
// succeeds; otherwise errs[i] is returned on call i+1.
func newLoader(symbol string, errs ...error) *fakeLoader {
	return &fakeLoader{
		table: Table{symbol: {Symbol: symbol, Base: "BTC", Quote: "USDT", Market: "BTC-USDT"}},
		errs:  errs,
		ch:    make(chan int, 64),
	}
}

// Load satisfies Loader.
func (f *fakeLoader) Load(context.Context) (Table, error) {
	call := int(f.calls.Add(1))

	select {
	case f.ch <- call:
	default:
	}

	if len(f.errs) > 0 {
		if err := f.errs[min(call-1, len(f.errs)-1)]; err != nil {
			return nil, err
		}
	}
	return f.table, nil
}

// waitForCall blocks until the loader reports call number n, or fails the test.
func (f *fakeLoader) waitForCall(t *testing.T, n int) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case got := <-f.ch:
			if got >= n {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for loader call %d (saw %d)", n, f.calls.Load())
		}
	}
}

// start runs r.Run in the background and stops it with the test.
func start(t *testing.T, r *Registry) <-chan error {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})
	return done
}

// --- Lookup tests ---

func TestLookupBeforeLoadReturnsNotFound(t *testing.T) {
	r := NewRegistry(newLoader("BTCUSDT").Load, time.Hour)

	inst, ok := r.Lookup("BTCUSDT")
	if ok {
		t.Errorf("Lookup on an unloaded registry = %+v, true; want zero value, false", inst)
	}
	if inst != (Instrument{}) {
		t.Errorf("Lookup instrument = %+v, want zero value", inst)
	}
}

func TestLookupUnknownSymbolReturnsNotFound(t *testing.T) {
	r := NewRegistry(newLoader("BTCUSDT").Load, time.Hour)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}

	if _, ok := r.Lookup("ETHUSDT"); ok {
		t.Error("Lookup(\"ETHUSDT\") = true, want false -- symbol was never loaded")
	}
}

// --- Load tests ---

func TestLoadPopulatesTable(t *testing.T) {
	loader := newLoader("BTCUSDT")
	r := NewRegistry(loader.Load, time.Hour)

	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if got := loader.calls.Load(); got != 1 {
		t.Errorf("loader called %d times, want 1", got)
	}

	inst, ok := r.Lookup("BTCUSDT")
	if !ok {
		t.Fatal("Lookup(\"BTCUSDT\") = false, want true after Load")
	}
	want := Instrument{Symbol: "BTCUSDT", Base: "BTC", Quote: "USDT", Market: "BTC-USDT"}
	if inst != want {
		t.Errorf("Lookup = %+v, want %+v", inst, want)
	}
}

func TestLoadPropagatesLoaderError(t *testing.T) {
	sentinel := errors.New("venue unreachable")
	r := NewRegistry(func(context.Context) (Table, error) { return nil, sentinel }, time.Hour)

	err := r.Load(context.Background())
	if !errors.Is(err, sentinel) {
		t.Errorf("Load() = %v, want %v", err, sentinel)
	}
}

func TestLoadFailureKeepsPreviousTable(t *testing.T) {
	fail := false
	r := NewRegistry(func(context.Context) (Table, error) {
		if fail {
			return nil, errors.New("boom")
		}
		return Table{"BTCUSDT": {Symbol: "BTCUSDT"}}, nil
	}, time.Hour)

	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("first Load() = %v, want nil", err)
	}

	fail = true
	if err := r.Load(context.Background()); err == nil {
		t.Fatal("second Load() = nil, want an error")
	}

	// A failed refresh must leave the registry serving stale data rather than
	// wiping it -- lookups keep working through a venue outage.
	if _, ok := r.Lookup("BTCUSDT"); !ok {
		t.Error("Lookup(\"BTCUSDT\") = false after a failed reload, want true (stale table retained)")
	}
}

func TestLoadReplacesTableWholesale(t *testing.T) {
	second := false
	r := NewRegistry(func(context.Context) (Table, error) {
		if second {
			return Table{"ETHUSDT": {Symbol: "ETHUSDT"}}, nil
		}
		return Table{"BTCUSDT": {Symbol: "BTCUSDT"}}, nil
	}, time.Hour)

	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("first Load() = %v, want nil", err)
	}
	second = true
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("second Load() = %v, want nil", err)
	}

	// A symbol delisted by the venue must disappear, not linger from the old table.
	if _, ok := r.Lookup("BTCUSDT"); ok {
		t.Error("Lookup(\"BTCUSDT\") = true after a reload that dropped it, want false")
	}
	if _, ok := r.Lookup("ETHUSDT"); !ok {
		t.Error("Lookup(\"ETHUSDT\") = false, want true")
	}
}

func TestLoadPassesContextToLoader(t *testing.T) {
	type ctxKey struct{}
	var got context.Context
	r := NewRegistry(func(ctx context.Context) (Table, error) {
		got = ctx
		return Table{}, nil
	}, time.Hour)

	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
	if err := r.Load(ctx); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if got.Value(ctxKey{}) != "marker" {
		t.Error("loader did not receive the context passed to Load")
	}
}

// --- Run tests ---

// Run refreshes; it does not prime. Callers Load once before starting it --
// binance.Adapter.Run does exactly that, and relies on a failed first Load
// aborting startup rather than being swallowed by a background goroutine.
func TestRunDoesNotLoadBeforeFirstTick(t *testing.T) {
	loader := newLoader("BTCUSDT")
	// A refresh interval far longer than the test: any load that happens
	// could only be a priming load.
	r := NewRegistry(loader.Load, time.Hour)
	start(t, r)

	time.Sleep(100 * time.Millisecond)

	if got := loader.calls.Load(); got != 0 {
		t.Errorf("loader ran %d times before the first tick, want 0", got)
	}
	if _, ok := r.Lookup("BTCUSDT"); ok {
		t.Error("Lookup(\"BTCUSDT\") = true before any tick, want false")
	}
}

func TestRunRefreshesOnInterval(t *testing.T) {
	loader := newLoader("BTCUSDT")
	r := NewRegistry(loader.Load, 5*time.Millisecond)
	start(t, r)

	// Run does not prime, so every one of these came from a tick.
	loader.waitForCall(t, 3)

	if _, ok := r.Lookup("BTCUSDT"); !ok {
		t.Error("Lookup(\"BTCUSDT\") = false after a refresh, want true")
	}
}

func TestRunSurvivesRefreshFailure(t *testing.T) {
	// Succeed, then fail, then succeed: a mid-stream refresh error must be
	// logged and swallowed, not returned.
	loader := newLoader("BTCUSDT", nil, errors.New("transient"), nil)
	r := NewRegistry(loader.Load, 5*time.Millisecond)
	done := start(t, r)

	loader.waitForCall(t, 3)
	select {
	case err := <-done:
		t.Fatalf("Run() returned %v after a failed refresh, want it to keep running", err)
	default:
	}

	// The table stayed usable across the failed tick.
	if _, ok := r.Lookup("BTCUSDT"); !ok {
		t.Error("Lookup(\"BTCUSDT\") = false, want true")
	}
}

func TestRunReturnsWhenContextAlreadyCancelled(t *testing.T) {
	r := NewRegistry(newLoader("BTCUSDT").Load, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run() = %v, want %v", err, context.Canceled)
	}
}

// TestLookupDuringRefreshIsRaceFree is meaningful under `go test -race`: it
// hammers Lookup while Run swaps the table underneath it.
func TestLookupDuringRefreshIsRaceFree(t *testing.T) {
	loader := newLoader("BTCUSDT")
	r := NewRegistry(loader.Load, time.Millisecond)
	start(t, r)
	loader.waitForCall(t, 1)

	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 500 {
				r.Lookup("BTCUSDT")
			}
		})
	}
	readers.Wait()
}
