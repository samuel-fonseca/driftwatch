package buffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

func TestPushNewKey(t *testing.T) {
	b := New(10)
	b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100})

	stats := b.Stats()
	if stats.Pushed != 1 {
		t.Errorf("Pushed = %d, want 1", stats.Pushed)
	}

	if stats.Depth != 1 {
		t.Errorf("Depth = %d, want 1", stats.Depth)
	}

	if stats.Coalesced != 0 {
		t.Errorf("Coalesced = %d, want 0 for a first push", stats.Coalesced)
	}
}

func TestPushCoalescesSameKey(t *testing.T) {
	b := New(10)
	q := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid"}

	for i := 1; i <= 50; i++ {
		q.Price = float64(i)
		b.Push(q)
	}

	stats := b.Stats()
	if stats.Depth != 1 {
		t.Errorf("Depth = %d, want 1 (same key coalesces, doesn't grow)", stats.Depth)
	}

	if stats.Coalesced != 49 {
		t.Errorf("Coalesced = %d, want 49 for 50 pushes of the same key", stats.Coalesced)
	}

	if stats.Pushed != 50 {
		t.Errorf("Pushed = %d, want 50 for 50 pushes of the same key", stats.Pushed)
	}
}

func TestPushRetainsPositionOnCoalesce(t *testing.T) {
	b := New(3)
	quoteSelections := []string{"A", "B", "C"}
	for _, k := range quoteSelections {
		q := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: k, Price: 100}
		b.Push(q)
	}

	b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "A", Price: 2000})

	// ensure order of elements
	keys := b.keysInOrder()
	if len(keys) != 3 {
		t.Errorf("keysInOrder = %v, want 3 elements", keys)
	}
	if keys[0] != "binance|BTC-USD|A" || keys[1] != "binance|BTC-USD|B" || keys[2] != "binance|BTC-USD|C" {
		t.Errorf("keysInOrder = %v, want [binance|BTC-USD|A binance|BTC-USD|B binance|BTC-USD|C]", keys)
	}
}

func TestPushEvictsOldestAtCapacity(t *testing.T) {
	b := New(3)
	q := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "A", Price: 100}
	b.Push(q)
	q.Selection = "B"
	b.Push(q)
	q.Selection = "C"
	b.Push(q)
	q.Selection = "D"
	b.Push(q)

	stats := b.Stats()
	if stats.Depth != 3 {
		t.Errorf("Depth = %d, want 3 after pushing 4 elements at capacity", stats.Depth)
	}

	keys := b.keysInOrder()
	if len(keys) != 3 {
		t.Errorf("keysInOrder = %v, want 3 elements", keys)
	}
	if keys[0] != "binance|BTC-USD|B" || keys[1] != "binance|BTC-USD|C" || keys[2] != "binance|BTC-USD|D" {
		t.Errorf("keysInOrder = %v, want [binance|BTC-USD|B binance|BTC-USD|C binance|BTC-USD|D]", keys)
	}

	stats = b.Stats()
	if stats.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1 after pushing 4 elements at capacity", stats.Evicted)
	}
	if stats.Pushed != 4 {
		t.Errorf("Pushed = %d, want 4 after pushing 4 elements at capacity", stats.Pushed)
	}
	if stats.Coalesced != 0 {
		t.Errorf("Coalesced = %d, want 0 after pushing 4 elements at capacity", stats.Coalesced)
	}
	if stats.Taken != 0 {
		t.Errorf("Taken = %d, want 0 after pushing 4 elements at capacity", stats.Taken)
	}
	if stats.Depth != 3 {
		t.Errorf("Depth = %d, want 3 after pushing 4 elements at capacity", stats.Depth)
	}
	if stats.MaxDepth != 3 {
		t.Errorf("MaxDepth = %d, want 3 after pushing 4 elements at capacity", stats.MaxDepth)
	}
}

func TestMaxDepthTracksPeak(t *testing.T) {
	b := New(10)
	b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "A"})
	b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "B"})
	b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "C"})

	stats := b.Stats()
	if stats.MaxDepth != 3 {
		t.Errorf("MaxDepth = %d, want 3", stats.MaxDepth)
	}
	if stats.MaxDepth != stats.Depth {
		t.Errorf("with no removals yet, MaxDepth (%d) should equal Depth (%d)",
			stats.MaxDepth, stats.Depth)
	}

	// TODO: once TakeBatch exists, add a test that Takes some items
	// (dropping Depth) and confirms MaxDepth stays at the earlier peak
	// instead of following Depth back down.
}

func TestTakeBatchReturnsImmediatelyWhenDataPresent(t *testing.T) {
	b := New(10)
	b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 1})
	b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "ask", Price: 2})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	batch, err := b.TakeBatch(ctx, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("len(batch) = %d, want 2", len(batch))
	}

	stats := b.Stats()
	if stats.Depth != 0 {
		t.Errorf("Depth = %d, want 0 after taking everything", stats.Depth)
	}
	if stats.Taken != 2 {
		t.Errorf("Taken = %d, want 2", stats.Taken)
	}
}

func TestTakeBatchRespectsMax(t *testing.T) {
	b := New(10)
	for _, sel := range []string{"A", "B", "C"} {
		b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: sel})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	batch, err := b.TakeBatch(ctx, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("len(batch) = %d, want 2", len(batch))
	}
	if batch[0].Selection != "A" || batch[1].Selection != "B" {
		t.Errorf("expected oldest-first [A B], got [%s %s]", batch[0].Selection, batch[1].Selection)
	}

	if depth := b.Stats().Depth; depth != 1 {
		t.Errorf("Depth = %d, want 1 remaining (C)", depth)
	}
}

func TestTakeBatchBlocksThenWakesOnPush(t *testing.T) {
	b := New(10)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result := make(chan []quote.Quote, 1)
	errCh := make(chan error, 1)
	go func() {
		batch, err := b.TakeBatch(ctx, 10)
		if err != nil {
			errCh <- err
			return
		}
		result <- batch
	}()

	select {
	case <-result:
		t.Error("TakeBatch returned before anything was pushed")
	case err := <-errCh:
		t.Errorf("TakeBatch errored before anything was pushed: %v", err)
	case <-time.After(50 * time.Millisecond):
		// expected: still waiting
	}

	b.Push(quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 42})

	select {
	case batch := <-result:
		if len(batch) != 1 || batch[0].Price != 42 {
			t.Errorf("got %+v, want one quote with Price 42", batch)
		}
	case err := <-errCh:
		t.Errorf("TakeBatch errored after Push: %v", err)
	case <-time.After(time.Second):
		t.Error("TakeBatch never woke up after Push")
	}
}

func TestTakeBatchRespectsContextCancellation(t *testing.T) {
	b := New(10)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := b.TakeBatch(ctx, 10)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from a cancelled context, got nil")
	}
	if elapsed > time.Second {
		t.Fatalf("TakeBatch took %v to respect cancellation -- way too slow", elapsed)
	}
}

func TestConcurrentAccounting(t *testing.T) {
	b := New(64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const producers = 8
	const pushesPerProducer = 500

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range pushesPerProducer {
				b.Push(quote.Quote{
					Venue:     "binance",
					Market:    "BTC-USD",
					Selection: string(rune('A' + (i+id)%20)),
					Price:     float64(i),
				})
			}
		}(p)
	}

	var takenTotal int
	var takenMu sync.Mutex
	var consumerWg sync.WaitGroup
	for range 4 {
		consumerWg.Go(func() {
			for {
				batch, err := b.TakeBatch(ctx, 8)
				if err != nil {
					return
				}
				takenMu.Lock()
				takenTotal += len(batch)
				takenMu.Unlock()
			}
		})
	}

	wg.Wait()
	cancel()
	consumerWg.Wait()

	stats := b.Stats()
	wantPushed := producers * pushesPerProducer
	if stats.Pushed != wantPushed {
		t.Fatalf("Pushed = %d, want %d", stats.Pushed, wantPushed)
	}

	sum := stats.Coalesced + stats.Evicted + stats.Taken + stats.Depth
	if sum != stats.Pushed {
		t.Errorf("Coalesced(%d) + Evicted(%d) + Taken(%d) + Depth(%d) = %d, want %d (== Pushed)",
			stats.Coalesced, stats.Evicted, stats.Taken, stats.Depth, sum, stats.Pushed)
	}
}

func (b *Buffer) keysInOrder() []string {
	keys := make([]string, 0, b.order.Len())
	for e := b.order.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(*entry).key)
	}
	return keys
}
