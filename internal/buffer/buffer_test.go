package buffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/quotetest"
)

// --- test helpers ---

// takeSelections drains up to max quotes and returns their selections, which
// the tests below use as short stand-ins for whole keys. Draining is how the
// queue order is meant to be observed: TakeBatch hands back oldest-first, so
// asserting through it keeps these tests off the internal list.
func takeSelections(t *testing.T, b *Buffer, max int) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	batch, err := b.TakeBatch(ctx, max)
	if err != nil {
		t.Fatalf("TakeBatch(%d) = %v, want nil", max, err)
	}

	got := make([]string, len(batch))
	for i, q := range batch {
		got[i] = q.Selection
	}
	return got
}

// push adds one quote per selection, all on the same venue and market, so the
// selection alone distinguishes the keys.
func push(b *Buffer, selections ...string) {
	for _, sel := range selections {
		b.Push(quotetest.Sel("binance", "BTC-USD", sel, 100))
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- Push ---

func TestPushNewKey(t *testing.T) {
	b := New(10)
	b.Push(quotetest.Bid("binance", "BTC-USD", 100))

	got := b.Stats()
	if got.Pushed != 1 {
		t.Errorf("Pushed = %d, want 1", got.Pushed)
	}
	if got.Depth != 1 {
		t.Errorf("Depth = %d, want 1", got.Depth)
	}
	if got.Coalesced != 0 {
		t.Errorf("Coalesced = %d, want 0 on a first push", got.Coalesced)
	}
}

// Coalescing is the buffer's whole reason to exist: a venue that reprices
// faster than the workers drain must not grow the queue without bound.
func TestPushCoalescesSameKey(t *testing.T) {
	b := New(10)

	for i := 1; i <= 50; i++ {
		b.Push(quotetest.Bid("binance", "BTC-USD", float64(i)))
	}

	got := b.Stats()
	if got.Depth != 1 {
		t.Errorf("Depth = %d, want 1 -- one key must occupy one slot", got.Depth)
	}
	if got.Pushed != 50 {
		t.Errorf("Pushed = %d, want 50", got.Pushed)
	}
	if got.Coalesced != 49 {
		t.Errorf("Coalesced = %d, want 49", got.Coalesced)
	}
}

// Coalescing must overwrite, not discard: the consumer wants the newest
// price on that book, not the one that happened to arrive first.
func TestPushCoalesceKeepsNewestValue(t *testing.T) {
	b := New(10)

	b.Push(quotetest.Bid("binance", "BTC-USD", 100, quotetest.Size(1)))
	b.Push(quotetest.Bid("binance", "BTC-USD", 250, quotetest.Size(7)))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	batch, err := b.TakeBatch(ctx, 10)
	if err != nil {
		t.Fatalf("TakeBatch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("len(batch) = %d, want 1", len(batch))
	}
	if batch[0].Price != 250 || batch[0].Size != 7 {
		t.Errorf("coalesced quote = %v @ %v, want the newest 250 @ 7", batch[0].Price, batch[0].Size)
	}
}

// A hot key repricing constantly must not be able to jump the queue and
// starve the books behind it, so a coalesce keeps its original position.
func TestPushCoalesceRetainsQueuePosition(t *testing.T) {
	b := New(3)
	push(b, "A", "B", "C")

	b.Push(quotetest.Sel("binance", "BTC-USD", "A", 2000))

	got := takeSelections(t, b, 3)
	if want := []string{"A", "B", "C"}; !equalSlices(got, want) {
		t.Errorf("drain order = %v, want %v", got, want)
	}
}

func TestPushEvictsOldestAtCapacity(t *testing.T) {
	b := New(3)
	push(b, "A", "B", "C", "D")

	got := takeSelections(t, b, 4)
	if want := []string{"B", "C", "D"}; !equalSlices(got, want) {
		t.Errorf("drain order = %v, want %v -- the oldest key should have gone", got, want)
	}
}

func TestPushEvictionAccounting(t *testing.T) {
	b := New(3)
	push(b, "A", "B", "C", "D")

	got := b.Stats()
	cases := []struct {
		name      string
		got, want int
	}{
		{"Pushed", got.Pushed, 4},
		{"Depth", got.Depth, 3},
		{"Evicted", got.Evicted, 1},
		{"Coalesced", got.Coalesced, 0},
		{"Taken", got.Taken, 0},
		{"MaxDepth", got.MaxDepth, 3},
		{"Capacity", got.Capacity, 3},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

// A repriced key already in a full buffer is an overwrite, not an arrival, so
// nothing needs to be dropped to make room for it.
func TestPushCoalesceAtCapacityEvictsNothing(t *testing.T) {
	b := New(3)
	push(b, "A", "B", "C")

	b.Push(quotetest.Sel("binance", "BTC-USD", "B", 999))

	got := b.Stats()
	if got.Evicted != 0 {
		t.Errorf("Evicted = %d, want 0 -- coalescing frees no space and needs none", got.Evicted)
	}
	if got.Depth != 3 {
		t.Errorf("Depth = %d, want 3", got.Depth)
	}
}

// MaxDepth is a watermark for sizing the buffer, so it has to survive the
// drain that brought the depth back down -- otherwise it only ever reports
// whatever the queue happens to hold at scrape time.
func TestMaxDepthHoldsThePeakAfterDraining(t *testing.T) {
	b := New(10)
	push(b, "A", "B", "C")

	if got := b.Stats(); got.MaxDepth != 3 || got.Depth != 3 {
		t.Fatalf("before draining: MaxDepth = %d, Depth = %d, want both 3", got.MaxDepth, got.Depth)
	}

	takeSelections(t, b, 10)

	got := b.Stats()
	if got.Depth != 0 {
		t.Errorf("Depth = %d, want 0 after draining everything", got.Depth)
	}
	if got.MaxDepth != 3 {
		t.Errorf("MaxDepth = %d, want it held at the earlier peak of 3", got.MaxDepth)
	}
}

// --- TakeBatch ---

func TestTakeBatchReturnsImmediatelyWhenDataPresent(t *testing.T) {
	b := New(10)
	b.Push(quotetest.Bid("binance", "BTC-USD", 1))
	b.Push(quotetest.Ask("binance", "BTC-USD", 2))

	if got := takeSelections(t, b, 10); len(got) != 2 {
		t.Fatalf("drained %v, want 2 quotes", got)
	}

	stats := b.Stats()
	if stats.Depth != 0 {
		t.Errorf("Depth = %d, want 0", stats.Depth)
	}
	if stats.Taken != 2 {
		t.Errorf("Taken = %d, want 2", stats.Taken)
	}
}

func TestTakeBatchRespectsMaxAndTakesOldestFirst(t *testing.T) {
	b := New(10)
	push(b, "A", "B", "C")

	if got := takeSelections(t, b, 2); !equalSlices(got, []string{"A", "B"}) {
		t.Errorf("first batch = %v, want [A B]", got)
	}
	if got := b.Stats().Depth; got != 1 {
		t.Errorf("Depth = %d, want 1 remaining", got)
	}
	if got := takeSelections(t, b, 2); !equalSlices(got, []string{"C"}) {
		t.Errorf("second batch = %v, want [C]", got)
	}
}

func TestTakeBatchBlocksThenWakesOnPush(t *testing.T) {
	b := New(10)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		batch []quote.Quote
		err   error
	}
	done := make(chan result, 1)
	go func() {
		batch, err := b.TakeBatch(ctx, 10)
		done <- result{batch, err}
	}()

	select {
	case got := <-done:
		t.Fatalf("TakeBatch returned %+v (err=%v) before anything was pushed", got.batch, got.err)
	case <-time.After(50 * time.Millisecond):
		// expected: still parked
	}

	b.Push(quotetest.Bid("binance", "BTC-USD", 42))

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("TakeBatch = %v, want nil after a push", got.err)
		}
		if len(got.batch) != 1 || got.batch[0].Price != 42 {
			t.Errorf("got %+v, want one quote at price 42", got.batch)
		}
	case <-time.After(time.Second):
		t.Error("TakeBatch never woke after Push")
	}
}

// A parked worker must abandon its wait at shutdown rather than holding the
// pipeline open.
func TestTakeBatchRespectsContextCancellation(t *testing.T) {
	b := New(10)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := b.TakeBatch(ctx, 10)

	if err == nil {
		t.Fatal("TakeBatch = nil error on a cancelled context, want one")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("TakeBatch took %v to notice cancellation", elapsed)
	}
}

// --- concurrency ---

// Every quote that goes in must be accounted for exactly once: coalesced onto
// an existing key, evicted, taken by a worker, or still resident. A lost
// increment or a double-count shows up here as a broken sum. Meaningful
// under -race.
func TestConcurrentAccounting(t *testing.T) {
	b := New(64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		producers         = 8
		pushesPerProducer = 500
	)

	var producerWg sync.WaitGroup
	for p := range producers {
		producerWg.Go(func() {
			for i := range pushesPerProducer {
				b.Push(quotetest.Sel(
					"binance", "BTC-USD",
					string(rune('A'+(i+p)%20)),
					float64(i),
				))
			}
		})
	}

	var consumerWg sync.WaitGroup
	for range 4 {
		consumerWg.Go(func() {
			for {
				if _, err := b.TakeBatch(ctx, 8); err != nil {
					return
				}
			}
		})
	}

	producerWg.Wait()
	cancel()
	consumerWg.Wait()

	got := b.Stats()
	if want := producers * pushesPerProducer; got.Pushed != want {
		t.Fatalf("Pushed = %d, want %d", got.Pushed, want)
	}
	if sum := got.Coalesced + got.Evicted + got.Taken + got.Depth; sum != got.Pushed {
		t.Errorf("Coalesced(%d) + Evicted(%d) + Taken(%d) + Depth(%d) = %d, want %d (== Pushed)",
			got.Coalesced, got.Evicted, got.Taken, got.Depth, sum, got.Pushed)
	}
	if got.Depth > got.Capacity {
		t.Errorf("Depth = %d, above Capacity %d", got.Depth, got.Capacity)
	}
}
