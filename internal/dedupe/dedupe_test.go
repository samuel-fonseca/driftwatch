package dedupe

import (
	"sync"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/quotetest"
)

// Changed answers "is this worth writing?". It keys on venue+market+selection
// and compares a fingerprint of price+size, so the timestamps a venue sends on
// every poll must not, on their own, make a quote look new.
func TestChanged(t *testing.T) {
	baseline := quotetest.Bid("binance", "BTC-USD", 100, quotetest.Size(1))

	cases := []struct {
		name   string
		next   quote.Quote
		want   bool
		reason string
	}{
		{
			name: "identical price and size", next: baseline, want: false,
			reason: "a venue re-reporting an unchanged book must not be written again",
		},
		{
			name: "price moved", next: quotetest.Bid("binance", "BTC-USD", 101, quotetest.Size(1)), want: true,
			reason: "a real price change is the thing we are here to record",
		},
		{
			name: "size moved", next: quotetest.Bid("binance", "BTC-USD", 100, quotetest.Size(2)), want: true,
			reason: "depth at the same price is a genuine change in the book",
		},
		{
			name: "different selection", next: quotetest.Ask("binance", "BTC-USD", 100, quotetest.Size(1)), want: true,
			reason: "the other side of the book is a different key entirely",
		},
		{
			name: "different venue", next: quotetest.Bid("kraken", "BTC-USD", 100, quotetest.Size(1)), want: true,
			reason: "each venue's book is tracked separately",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := New(10)
			if !d.Changed(baseline) {
				t.Fatal("the first sighting of a key must always report changed")
			}

			if got := d.Changed(c.next); got != c.want {
				t.Errorf("Changed() = %v, want %v -- %s", got, c.want, c.reason)
			}
		})
	}
}

// Only the observation time moves. Dedupe exists precisely so this does not
// reach the store: at a two-second poll interval a quiet book would otherwise
// write 43,200 identical rows a day.
func TestChangedIgnoresTimestampOnlyUpdates(t *testing.T) {
	d := New(10)
	first := quotetest.Bid("binance", "BTC-USD", 100)

	if !d.Changed(first) {
		t.Fatal("the first sighting of a key must always report changed")
	}

	later := first
	later.ObservedAt = first.ObservedAt.Add(time.Minute)
	later.ReceivedAt = first.ReceivedAt.Add(time.Minute)

	if d.Changed(later) {
		t.Error("a repeat that differs only in its timestamps reported changed, want it suppressed")
	}
}

func TestFilterChangedKeepsSurvivorsInOrder(t *testing.T) {
	d := New(10)
	bid := quotetest.Bid("binance", "BTC-USD", 100)
	ask := quotetest.Ask("binance", "BTC-USD", 101)
	ethBid := quotetest.Bid("binance", "ETH-USD", 50)

	d.Changed(bid) // establish a baseline for the bid only

	got := d.FilterChanged([]quote.Quote{bid, ask, ethBid})

	if len(got) != 2 {
		t.Fatalf("len(survivors) = %d, want 2 (the repeated bid dropped): %+v", len(got), got)
	}
	if got[0].Key() != ask.Key() || got[1].Key() != ethBid.Key() {
		t.Errorf("survivors = [%s %s], want [%s %s] in batch order",
			got[0].Key(), got[1].Key(), ask.Key(), ethBid.Key())
	}
}

// A batch of pure repeats must yield an empty slice rather than nil, so the
// pipeline worker's len() check is the only thing deciding whether to write.
func TestFilterChangedAllSuppressedIsEmptyNotNil(t *testing.T) {
	d := New(10)
	q := quotetest.Bid("binance", "BTC-USD", 100)
	d.Changed(q)

	got := d.FilterChanged([]quote.Quote{q, q})
	if got == nil {
		t.Fatal("FilterChanged returned nil, want an empty slice")
	}
	if len(got) != 0 {
		t.Errorf("len(survivors) = %d, want 0: %+v", len(got), got)
	}
}

// Eviction is observable through Changed alone: an evicted key has no
// baseline left, so its next sighting reports changed alongside a bumped
// Evicted counter. Asserting it this way rather than by walking the list
// keeps the test pointed at behaviour instead of at container/list.
func TestEvictsLeastRecentlyUsedAtCapacity(t *testing.T) {
	d := New(2)
	btc := quotetest.Bid("bitfinex", "BTC-USD", 100)
	eth := quotetest.Bid("bitfinex", "ETH-USD", 100)
	doge := quotetest.Bid("bitfinex", "DOGE-USD", 100)

	d.Changed(btc)
	d.Changed(eth)

	// Touching btc again makes eth the least recently used.
	moved := btc
	moved.Price = 101
	d.Changed(moved)

	d.Changed(doge) // at capacity: eth is evicted

	if got := d.Stats().Evicted; got != 1 {
		t.Errorf("Evicted = %d, want 1", got)
	}
	// Check the survivor first: re-admitting the evicted key would itself
	// evict something, and btc is next in line.
	if d.Changed(moved) {
		t.Error("btc reported changed, want it still tracked -- recent use must protect a key")
	}
	if !d.Changed(eth) {
		t.Error("eth reported unchanged, want it evicted and treated as new")
	}
}

// Without the MoveToFront on a hit, a key seen on every poll would still age
// out purely by insertion order, and the busiest books would be the ones
// whose repeats leak through to the store.
func TestRecentUseProtectsAKeyFromEviction(t *testing.T) {
	const capacity = 4
	d := New(capacity)

	hot := quotetest.Bid("binance", "BTC-USD", 100)
	d.Changed(hot)

	// Push far more distinct keys than the cache holds, re-touching hot
	// between each one.
	for i := range 50 {
		d.Changed(quotetest.Sel("binance", "FILLER-USD", string(rune('a'+i%26)), float64(i)))
		if d.Changed(hot) {
			t.Fatalf("hot key was evicted after %d filler insertions", i+1)
		}
	}
}

func TestStatsCountsSeenSeparatelyFromChanged(t *testing.T) {
	d := New(2)
	q := quotetest.Bid("binance", "BTC-USD", 100)

	d.Changed(q) // first sighting: changed
	d.Changed(q) // identical repeat: not changed
	moved := q
	moved.Price = 101
	d.Changed(moved) // real move: changed

	got := d.Stats()
	if got.Seen != 3 {
		t.Errorf("Seen = %d, want 3 -- every call counts", got.Seen)
	}
	if got.Changed != 2 {
		t.Errorf("Changed = %d, want 2 -- the identical repeat must not count", got.Changed)
	}
	if got.Size != 1 {
		t.Errorf("Size = %d, want 1 -- all three sightings share one key", got.Size)
	}
	if got.Capacity != 2 {
		t.Errorf("Capacity = %d, want 2", got.Capacity)
	}
	if got.Evicted != 0 {
		t.Errorf("Evicted = %d, want 0 -- one key cannot overflow a capacity of 2", got.Evicted)
	}
}

func TestStatsSizeNeverExceedsCapacity(t *testing.T) {
	const capacity = 8
	d := New(capacity)

	for i := range 100 {
		d.Changed(quotetest.Bid("binance", "M"+string(rune('a'+i%40))+"-USD", float64(i)))
	}

	got := d.Stats()
	if got.Size > got.Capacity {
		t.Errorf("Size = %d, above Capacity %d", got.Size, got.Capacity)
	}
	if got.Seen != 100 {
		t.Errorf("Seen = %d, want 100", got.Seen)
	}
}

// The pipeline runs several workers against one Detector, so every counter
// and the list itself are under a shared mutex. Meaningful under -race.
func TestConcurrentChangedIsSafe(t *testing.T) {
	d := New(64)

	const (
		workers   = 8
		perWorker = 250
	)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				d.Changed(quotetest.Sel(
					"binance", "BTC-USD",
					string(rune('a'+(i+w)%20)),
					float64(i),
				))
			}
		})
	}
	wg.Wait()

	got := d.Stats()
	if want := int64(workers * perWorker); got.Seen != want {
		t.Errorf("Seen = %d, want %d", got.Seen, want)
	}
	if got.Changed > got.Seen {
		t.Errorf("Changed (%d) exceeds Seen (%d)", got.Changed, got.Seen)
	}
	if got.Size > got.Capacity {
		t.Errorf("Size = %d, above Capacity %d", got.Size, got.Capacity)
	}
}
