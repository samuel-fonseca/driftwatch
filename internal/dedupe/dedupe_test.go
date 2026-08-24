package dedupe

import (
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

func TestFirstSightingIsAlwaysChanged(t *testing.T) {
	d := New(10)
	q := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1}

	if !d.Changed(q) {
		t.Error("first sighting of a key should always report changed = true")
	}
}

func TestIdenticalPriceAndSizeIsNotChanged(t *testing.T) {
	d := New(10)
	q := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1}

	d.Changed(q)

	q.ObservedAt = q.ObservedAt.Add(time.Second)
	if d.Changed(q) {
		t.Error("identical price/size should report changed = false on second sighting")
	}
}

func TestPriceChangeIsChanged(t *testing.T) {
	d := New(10)
	q := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1}
	d.Changed(q)

	q.Price = 101
	if !d.Changed(q) {
		t.Error("a real price change should report changed = true")
	}
}

func TestSizeOnlyChangeIsChanged(t *testing.T) {
	d := New(10)
	q := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1}
	d.Changed(q)

	q.Size = 2 // price identical, size differs
	if !d.Changed(q) {
		t.Error("a size-only change should still report changed = true")
	}
}

func TestFilterChangedDropsUnchangedFromBatch(t *testing.T) {
	d := New(10)
	bid := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1}
	ask := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "ask", Price: 101, Size: 1}

	d.Changed(bid) // establish baseline for bid only

	batch := []quote.Quote{bid, ask} // bid unchanged (repeat), ask is new
	out := d.FilterChanged(batch)

	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1 (only ask should survive)", len(out))
	}
	if out[0].Selection != "ask" {
		t.Errorf("survivor = %q, want %q", out[0].Selection, "ask")
	}
}

func TestLRUEvictsLeastRecentlySeen(t *testing.T) {
	d := New(2)
	a := quote.Quote{Venue: "bitfinex", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1}
	b := quote.Quote{Venue: "bitfinex", Market: "ETH-USD", Selection: "bid", Price: 100, Size: 1}

	d.Changed(a)
	d.Changed(b)

	// the current order should be: b, a

	if d.order.Front().Value.(*seenEntry).key != "bitfinex|ETH-USD|bid" {
		t.Errorf("expected ETH-USD to be the front, got %s", d.order.Front().Value.(*seenEntry).key)
	}

	a.Price = 101
	d.Changed(a) // new order: a, b

	c := quote.Quote{Venue: "bitfinex", Market: "DOGE-USD", Selection: "bid", Price: 100, Size: 1}
	d.Changed(c) // new order: c, a - b should have been dropped

	if !d.Changed(b) {
		t.Errorf("expected b to have been evicted and re-reported as changed, but it was still tracked")
	}
}

func TestSeenGrowsSeparateFromChanged(t *testing.T) {
	d := New(2)
	q := quote.Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1}
	d.Changed(q)
	d.Changed(q)
	q.Price = 101
	d.Changed(q)

	if d.seen != 3 {
		t.Errorf("seen = %d, want 3", d.seen)
	}
	// Both the first sighting and the price change return true and get
	// written, so both count as changed. Only the identical repeat doesn't.
	if d.changed != 2 {
		t.Errorf("changed = %d, want 2", d.changed)
	}
}
