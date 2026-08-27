package symbols

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- test helpers ---

// staticLoader returns a Loader serving a table with a single instrument
// keyed by symbol, plus a counter of how many times it was invoked.
func staticLoader(symbol string) (Loader, *int) {
	calls := 0
	return func(context.Context) (Table, error) {
		calls++
		return Table{symbol: {Symbol: symbol, Base: "BTC", Quote: "USDT", Market: "BTC-USDT"}}, nil
	}, &calls
}

// signalLoader returns a Loader that reports each invocation on the returned
// channel, letting Run tests wait on real progress instead of sleeping. The
// results slice is consumed one entry per call; the last entry repeats once
// exhausted.
func signalLoader(results []error) (Loader, <-chan int) {
	ch := make(chan int, 64)
	call := 0
	return func(context.Context) (Table, error) {
		call++
		err := results[min(call-1, len(results)-1)]
		select {
		case ch <- call:
		default:
		}
		if err != nil {
			return nil, err
		}
		return Table{"BTCUSDT": {Symbol: "BTCUSDT"}}, nil
	}, ch
}

// waitForCall blocks until the loader reports call number n, or fails the test.
func waitForCall(t *testing.T, ch <-chan int, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case got := <-ch:
			if got >= n {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for loader call %d", n)
		}
	}
}

// --- Lookup tests ---

func TestLookupBeforeLoadReturnsNotFound(t *testing.T) {
	loader, _ := staticLoader("BTCUSDT")
	r := NewRegistry(loader, time.Hour)

	inst, ok := r.Lookup("BTCUSDT")
	if ok {
		t.Errorf("Lookup on an unloaded registry = %+v, true; want zero value, false", inst)
	}
	if inst != (Instrument{}) {
		t.Errorf("Lookup instrument = %+v, want zero value", inst)
	}
}

func TestLookupUnknownSymbolReturnsNotFound(t *testing.T) {
	loader, _ := staticLoader("BTCUSDT")
	r := NewRegistry(loader, time.Hour)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}

	if _, ok := r.Lookup("ETHUSDT"); ok {
		t.Error("Lookup(\"ETHUSDT\") = true, want false -- symbol was never loaded")
	}
}

// --- Load tests ---

func TestLoadPopulatesTable(t *testing.T) {
	loader, calls := staticLoader("BTCUSDT")
	r := NewRegistry(loader, time.Hour)

	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if *calls != 1 {
		t.Errorf("loader called %d times, want 1", *calls)
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
	loader, ch := signalLoader([]error{nil})
	// A refresh interval far longer than the test: any load that happens
	// could only be a priming load.
	r := NewRegistry(loader, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case n := <-ch:
		t.Fatalf("loader ran (call %d) before the first tick, want no load until then", n)
	case <-time.After(100 * time.Millisecond):
	}

	if _, ok := r.Lookup("BTCUSDT"); ok {
		t.Error("Lookup(\"BTCUSDT\") = true before any tick, want false")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run() = %v, want %v", err, context.Canceled)
	}
}

func TestRunRefreshesOnInterval(t *testing.T) {
	loader, ch := signalLoader([]error{nil})
	r := NewRegistry(loader, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Call 1 is the priming load; 2 and 3 can only come from ticks.
	waitForCall(t, ch, 3)

	cancel()
	<-done
}

func TestRunSurvivesRefreshFailure(t *testing.T) {
	// Succeed, then fail, then succeed: a mid-stream refresh error must be
	// logged and swallowed, not returned.
	loader, ch := signalLoader([]error{nil, errors.New("transient"), nil})
	r := NewRegistry(loader, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitForCall(t, ch, 3)
	select {
	case err := <-done:
		t.Fatalf("Run() returned %v after a failed refresh, want it to keep running", err)
	default:
	}

	// The table stayed usable across the failed tick.
	if _, ok := r.Lookup("BTCUSDT"); !ok {
		t.Error("Lookup(\"BTCUSDT\") = false, want true")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run() = %v, want %v", err, context.Canceled)
	}
}

func TestRunReturnsWhenContextAlreadyCancelled(t *testing.T) {
	loader, _ := staticLoader("BTCUSDT")
	r := NewRegistry(loader, time.Hour)

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
	loader, ch := signalLoader([]error{nil})
	r := NewRegistry(loader, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitForCall(t, ch, 1)

	readers := make(chan struct{}, 4)
	for range 4 {
		go func() {
			for range 500 {
				r.Lookup("BTCUSDT")
			}
			readers <- struct{}{}
		}()
	}
	for range 4 {
		<-readers
	}

	cancel()
	<-done
}
