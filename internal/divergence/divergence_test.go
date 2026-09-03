package divergence

import (
	"fmt"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/quotetest"
)

// --- test helpers ---

const (
	edgeBps        = 5.0
	staleThreshold = 2 * time.Second
	collisionRatio = 4.0
)

func newDetector() *Detector { return New(edgeBps, staleThreshold, collisionRatio) }

// observe mirrors the pipeline worker: every arriving quote marks its venue
// alive from the raw pre-dedupe batch before the surviving quote reaches the
// detector. Observe on its own never marks a venue live.
func observe(d *Detector, q quote.Quote) *Signal {
	d.MarkAlive([]quote.Quote{q})
	return d.Observe(q)
}

// A suppression names one reason Observe can decline to emit, paired with the
// counter that records it.
type suppression struct {
	name string
	get  func(Stats) int64
}

// allSuppressions is every reason the detector tracks. Tests assert against
// the whole set so a quote suppressed for the *wrong* reason fails: checking
// only that the signal was nil would pass either way, which is how a
// misclassified suppression stays invisible in the metrics.
var allSuppressions = []suppression{
	{"invalid selection", func(s Stats) int64 { return s.SuppressedInvalidSelection }},
	{"incomplete book", func(s Stats) int64 { return s.SuppressedIncompleteBook }},
	{"not crossed", func(s Stats) int64 { return s.SuppressedNotCrossed }},
	{"same venue", func(s Stats) int64 { return s.SuppressedSameVenue }},
	{"below threshold", func(s Stats) int64 { return s.SuppressedBelowThreshold }},
	{"stale", func(s Stats) int64 { return s.SuppressedStale }},
	{"stale arrival", func(s Stats) int64 { return s.SuppressedStaleArrival }},
	{"collision", func(s Stats) int64 { return s.SuppressedCollision }},
}

// assertSuppressedBy checks that the transition from before to after bumped
// exactly the named counter and no other.
func assertSuppressedBy(t *testing.T, before, after Stats, want string) {
	t.Helper()

	for _, s := range allSuppressions {
		delta := s.get(after) - s.get(before)
		switch {
		case s.name == want && delta != 1:
			t.Errorf("%q counter moved by %d, want 1", s.name, delta)
		case s.name != want && delta != 0:
			t.Errorf("%q counter moved by %d, want 0 -- suppressed for the wrong reason", s.name, delta)
		}
	}
}

// --- emitting ---

func TestSignalFiresAcrossThreeVenues(t *testing.T) {
	d := newDetector()

	observe(d, quotetest.Bid("binance", "BTC-USD", 100.00))
	observe(d, quotetest.Bid("bitfinex", "BTC-USD", 100.50))

	sig := observe(d, quotetest.Ask("kraken", "BTC-USD", 100.00))
	if sig == nil {
		t.Fatalf("expected a signal, got nil (stats: %+v)", d.Stats())
	}

	if sig.BidVenue != "bitfinex" || sig.AskVenue != "kraken" {
		t.Errorf("legs = bid:%s ask:%s, want bid:bitfinex (the best bid) ask:kraken",
			sig.BidVenue, sig.AskVenue)
	}
	if sig.BidPrice != 100.50 || sig.AskPrice != 100.00 {
		t.Errorf("prices = bid:%v ask:%v, want 100.50 / 100.00", sig.BidPrice, sig.AskPrice)
	}
	if sig.Market != "BTC-USD" {
		t.Errorf("Market = %q, want %q", sig.Market, "BTC-USD")
	}
	if sig.DetectedAt.IsZero() {
		t.Error("DetectedAt is zero, want the detection time")
	}
	if got := d.Stats().Emitted; got != 1 {
		t.Errorf("Emitted = %d, want 1", got)
	}
}

// The mirror of the test above, and the one that catches a comparator
// pointing the wrong way on the ask side: a taker lifting an offer wants the
// LOWEST ask, just as it wants the highest bid. With only one ask venue the
// choice never arises, so this needs two.
func TestSignalPicksTheLowestAskAcrossVenues(t *testing.T) {
	d := newDetector()

	observe(d, quotetest.Bid("binance", "BTC-USD", 100.50))
	observe(d, quotetest.Ask("kraken", "BTC-USD", 100.20))

	sig := observe(d, quotetest.Ask("bitfinex", "BTC-USD", 100.00))
	if sig == nil {
		t.Fatalf("expected a signal, got nil (stats: %+v)", d.Stats())
	}
	if sig.AskVenue != "bitfinex" || sig.AskPrice != 100.00 {
		t.Errorf("ask leg = %s @ %v, want bitfinex @ 100.00 (the cheapest offer)",
			sig.AskVenue, sig.AskPrice)
	}
	if sig.BidVenue != "binance" {
		t.Errorf("bid leg = %s, want binance", sig.BidVenue)
	}
}

// The collision check runs over both sides. Two venues offering the same
// ticker thousands of times apart is the same "different assets, one symbol"
// problem, whichever side of the book it shows up on.
func TestCollisionIsDetectedOnTheAskSide(t *testing.T) {
	d := newDetector()

	observe(d, quotetest.Bid("binance", "U-USD", 0.9993))
	observe(d, quotetest.Ask("binance", "U-USD", 0.9990))

	before := d.Stats()
	sig := observe(d, quotetest.Ask("kraken", "U-USD", 0.000305))
	after := d.Stats()

	if sig != nil {
		t.Fatalf("got signal %+v, want nil for an ask-side ticker collision", sig)
	}
	assertSuppressedBy(t, before, after, "collision")
}

// The edge is the crossed spread in basis points of the mid, and it is what
// decides whether a signal clears the threshold at all.
func TestSignalReportsEdgeInBps(t *testing.T) {
	d := newDetector()

	observe(d, quotetest.Bid("binance", "BTC-USD", 100.10))
	sig := observe(d, quotetest.Ask("kraken", "BTC-USD", 100.00))
	if sig == nil {
		t.Fatalf("expected a signal, got nil (stats: %+v)", d.Stats())
	}

	// (100.10 - 100.00) / 100.05 * 10_000 ~= 9.995 bps
	if sig.EdgeBps < 9.9 || sig.EdgeBps > 10.1 {
		t.Errorf("EdgeBps = %v, want about 10", sig.EdgeBps)
	}
}

// --- suppression ---

func TestObserveSuppression(t *testing.T) {
	cases := []struct {
		name   string
		setup  []quote.Quote
		final  quote.Quote
		reason string
		why    string
	}{
		{
			name:   "selection is neither bid nor ask",
			final:  quotetest.Sel("binance", "BTC-USD", "mid", 100.00),
			reason: "invalid selection",
			why:    "the detector only understands a two-sided book",
		},
		{
			name:   "only one side quoted",
			final:  quotetest.Bid("binance", "BTC-USD", 100.00),
			reason: "incomplete book",
			why:    "a bid with no ask anywhere cannot cross",
		},
		{
			name:   "book is not crossed",
			setup:  []quote.Quote{quotetest.Bid("binance", "BTC-USD", 100.00)},
			final:  quotetest.Ask("bitfinex", "BTC-USD", 101.00),
			reason: "not crossed",
			why:    "the best bid sits below the best ask, which is a normal market",
		},
		{
			name:   "both sides from one venue",
			setup:  []quote.Quote{quotetest.Bid("binance", "BTC-USD", 101.00)},
			final:  quotetest.Ask("binance", "BTC-USD", 100.00),
			reason: "same venue",
			why:    "there is no trade to do against a single venue's own book",
		},
		{
			name:   "edge below the threshold",
			setup:  []quote.Quote{quotetest.Bid("binance", "BTC-USD", 100.00)},
			final:  quotetest.Ask("bitfinex", "BTC-USD", 99.999),
			reason: "below threshold",
			why:    "a sub-threshold edge is noise, not an opportunity",
		},
		{
			name:   "the only crossing leg is stale",
			setup:  []quote.Quote{quotetest.Bid("binance", "BTC-USD", 100.50, quotetest.Ago(5*time.Second))},
			final:  quotetest.Ask("bitfinex", "BTC-USD", 100.00),
			reason: "stale",
			why:    "a venue nothing has been heard from cannot be traded against",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := newDetector()
			for _, q := range c.setup {
				observe(d, q)
			}

			before := d.Stats()
			sig := observe(d, c.final)
			after := d.Stats()

			if sig != nil {
				t.Fatalf("got signal %+v, want nil -- %s", sig, c.why)
			}
			assertSuppressedBy(t, before, after, c.reason)
		})
	}
}

// Quotes for one venue can arrive out of order across a reconnect. Letting an
// older one overwrite the newer would rewind that venue's book and could
// manufacture a crossed market out of two different instants.
func TestObserveRejectsOutOfOrderArrival(t *testing.T) {
	d := newDetector()

	current := quotetest.Bid("binance", "BTC-USD", 100.00)
	observe(d, current)

	before := d.Stats()
	stale := quotetest.Bid("binance", "BTC-USD", 500.00, quotetest.Ago(time.Second))
	sig := observe(d, stale)
	after := d.Stats()

	if sig != nil {
		t.Fatalf("got signal %+v, want nil for a late-arriving older quote", sig)
	}
	assertSuppressedBy(t, before, after, "stale arrival")

	// The rewind must not have landed: the newer price still stands, so an
	// ask at 100.50 does not cross it.
	if sig := observe(d, quotetest.Ask("kraken", "BTC-USD", 100.50)); sig != nil {
		t.Errorf("got signal %+v -- the older quote overwrote the newer one", sig)
	}
}

// A venue holding a steady price is still a live venue. Dedupe drops those
// repeats before they reach Observe, so the stored quote's ObservedAt freezes
// while the venue keeps reporting -- only MarkAlive sees it. Judging staleness
// from the stored quote would suppress a perfectly good signal here.
func TestStableLegStillSignalsWhileVenueIsAlive(t *testing.T) {
	d := newDetector()

	// binance's bid last reached the detector 10s ago and has not moved
	// since, so dedupe has swallowed every repeat.
	d.Observe(quotetest.Bid("binance", "BTC-USD", 100.50, quotetest.Ago(10*time.Second)))

	// ...but binance is very much alive: the raw stream still carries that
	// unchanged price on every poll.
	d.MarkAlive([]quote.Quote{quotetest.Bid("binance", "BTC-USD", 100.50)})

	sig := observe(d, quotetest.Ask("kraken", "BTC-USD", 100.00))
	if sig == nil {
		t.Fatalf("expected a signal from a live venue holding a steady price (stats: %+v)", d.Stats())
	}
	if sig.BidVenue != "binance" || sig.AskVenue != "kraken" {
		t.Errorf("legs = bid:%s ask:%s, want bid:binance ask:kraken", sig.BidVenue, sig.AskVenue)
	}
}

func TestDeadVenueIsExcludedFromSelection(t *testing.T) {
	d := newDetector()

	// binance is live, with a bid that does not cross kraken's ask.
	observe(d, quotetest.Bid("binance", "BTC-USD", 100.00))

	// bitfinex holds a much better bid that would cross, but nothing has
	// been heard from it for far longer than the stale threshold.
	observe(d, quotetest.Bid("bitfinex", "BTC-USD", 100.50, quotetest.Ago(10*time.Second)))

	if sig := observe(d, quotetest.Ask("kraken", "BTC-USD", 100.20)); sig != nil {
		t.Fatalf("got signal %+v -- the only crossing bid belongs to a dead venue", sig)
	}
}

// --- collisions ---

// Two venues can list entirely different assets under the same ticker. The
// prices give it away: nothing that is one asset trades 3000x apart on two
// venues at the same moment.
func TestCollidedTickerIsSuppressed(t *testing.T) {
	d := newDetector()

	observe(d, quotetest.Bid("binance", "U-USD", 0.9993))
	observe(d, quotetest.Bid("kraken", "U-USD", 0.000305))

	before := d.Stats()
	sig := observe(d, quotetest.Ask("kraken", "U-USD", 0.000305))
	after := d.Stats()

	if sig != nil {
		t.Fatalf("got signal %+v, want nil for a ticker collision", sig)
	}
	assertSuppressedBy(t, before, after, "collision")

	if got := d.Collided(); len(got) != 1 || got[0] != "U-USD" {
		t.Errorf("Collided() = %v, want [U-USD]", got)
	}
	if got := after.MarketsCollided; got != 1 {
		t.Errorf("MarketsCollided = %d, want 1", got)
	}
}

// Prices on one side span a ratio of at least 1 by definition, so a ratio of
// 1 or below would flag every book and silence the detector entirely. Treat
// those as disabled instead -- failing open beats suppressing everything.
func TestCollisionCheckDisabledByUnusableRatio(t *testing.T) {
	for _, ratio := range []float64{0, 0.1, 1} {
		t.Run(fmtRatio(ratio), func(t *testing.T) {
			d := New(edgeBps, staleThreshold, ratio)

			observe(d, quotetest.Bid("binance", "U-USD", 0.9993))
			observe(d, quotetest.Bid("kraken", "U-USD", 0.000305))

			if sig := observe(d, quotetest.Ask("kraken", "U-USD", 0.000305)); sig == nil {
				t.Errorf("got no signal, want the check disabled (stats: %+v)", d.Stats())
			}
		})
	}
}

// Collided is read by the /stats endpoint, so it must be a stable, sorted set
// rather than whatever order the map iterates in.
func TestCollidedIsSortedAndDeduplicated(t *testing.T) {
	d := newDetector()

	for _, market := range []string{"Z-USD", "A-USD", "M-USD"} {
		observe(d, quotetest.Bid("binance", market, 0.9993))
		observe(d, quotetest.Bid("kraken", market, 0.000305))
		// Trip the same market twice; it must still be listed once.
		observe(d, quotetest.Ask("kraken", market, 0.000305))
		observe(d, quotetest.Ask("kraken", market, 0.000305))
	}

	got := d.Collided()
	want := []string{"A-USD", "M-USD", "Z-USD"}
	if len(got) != len(want) {
		t.Fatalf("Collided() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Collided() = %v, want %v (sorted, deduplicated)", got, want)
		}
	}
}

// --- market accounting ---

// MarketsCrossable exists to separate "quiet but capable" from "structurally
// incapable of ever signalling", which the suppression counters alone cannot
// distinguish.
func TestMarketsCrossable(t *testing.T) {
	cases := []struct {
		name string
		book []quote.Quote
		want int64
		why  string
	}{
		{
			name: "live bid and ask on different venues",
			book: []quote.Quote{
				quotetest.Bid("binance", "BTC-USD", 100.00),
				quotetest.Ask("kraken", "BTC-USD", 100.50),
			},
			want: 1,
			why:  "two venues on opposite sides can cross",
		},
		{
			name: "one venue quoting both sides",
			book: []quote.Quote{
				quotetest.Bid("binance", "BTC-USD", 100.00),
				quotetest.Ask("binance", "BTC-USD", 100.50),
			},
			want: 0,
			why:  "a lone venue cannot cross against itself",
		},
		{
			name: "bid side only",
			book: []quote.Quote{
				quotetest.Bid("binance", "BTC-USD", 100.00),
				quotetest.Bid("kraken", "BTC-USD", 100.10),
			},
			want: 0,
			why:  "nobody is quoting an ask",
		},
		{
			name: "the only ask venue is dead",
			book: []quote.Quote{
				quotetest.Bid("binance", "BTC-USD", 100.00),
				quotetest.Ask("kraken", "BTC-USD", 100.50, quotetest.Ago(10*time.Second)),
			},
			want: 0,
			why:  "a dead venue's quote is not a live ask",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := newDetector()
			for _, q := range c.book {
				observe(d, q)
			}

			if got := d.Stats().MarketsCrossable; got != c.want {
				t.Errorf("MarketsCrossable = %d, want %d -- %s", got, c.want, c.why)
			}
		})
	}
}

func TestMarketsTrackedCountsDistinctMarkets(t *testing.T) {
	d := newDetector()

	observe(d, quotetest.Bid("binance", "BTC-USD", 100.00))
	observe(d, quotetest.Ask("kraken", "BTC-USD", 100.50)) // same market
	observe(d, quotetest.Bid("binance", "ETH-USD", 50.00))

	if got := d.Stats().MarketsTracked; got != 2 {
		t.Errorf("MarketsTracked = %d, want 2 (BTC-USD and ETH-USD)", got)
	}
}

func TestObservedCountsEveryQuote(t *testing.T) {
	d := newDetector()

	observe(d, quotetest.Bid("binance", "BTC-USD", 100.00))
	observe(d, quotetest.Sel("binance", "BTC-USD", "mid", 100.00)) // rejected, still observed
	observe(d, quotetest.Ask("kraken", "BTC-USD", 100.50))

	if got := d.Stats().Observed; got != 3 {
		t.Errorf("Observed = %d, want 3 -- including the one it declined to use", got)
	}
}

func fmtRatio(r float64) string {
	switch r {
	case 0:
		return "ratio=0"
	case 1:
		return "ratio=1"
	default:
		return "ratio=fractional"
	}
}

// BenchmarkObserve covers the hot path: every quote that survives dedupe
// reaches Observe, which rescans both sides of that market's book under the
// detector's lock. The venue counts bracket production -- three venues today,
// eight if the venue list grows -- and the book is deliberately uncrossed,
// which is the outcome for ~94% of live traffic.
func BenchmarkObserve(b *testing.B) {
	for _, venues := range []int{2, 3, 8} {
		b.Run(fmt.Sprintf("venues=%d", venues), func(b *testing.B) {
			// A stale threshold far beyond the benchmark's runtime keeps
			// every venue live, so the scan never short-circuits.
			d := New(edgeBps, time.Hour, collisionRatio)

			quotes := make([]quote.Quote, 0, venues*2)
			for i := range venues {
				venue := fmt.Sprintf("venue%d", i)
				quotes = append(quotes,
					quotetest.Bid(venue, "BTC-USD", 100+float64(i)*0.01),
					quotetest.Ask(venue, "BTC-USD", 101+float64(i)*0.01),
				)
			}

			d.MarkAlive(quotes)
			for _, q := range quotes {
				d.Observe(q)
			}

			b.ReportAllocs()
			b.ResetTimer()

			i := 0
			for b.Loop() {
				d.Observe(quotes[i%len(quotes)])
				i++
			}
		})
	}
}

// bestSide walks a map, and Go randomises map iteration order, so which of
// its two update branches runs depends on the order the runtime happens to
// pick. Repeating the scan makes both of them certain, which keeps this
// covered deterministically instead of leaving it to chance on any given run.
func TestBestSideFindsBothEndsWhateverTheIterationOrder(t *testing.T) {
	const market = "BTC-USD"

	for range 50 {
		d := newDetector()

		// Three venues, so the first one the map yields is the middle
		// price a third of the time and an extreme the rest.
		prices := []float64{100.00, 101.00, 102.00}
		for i, price := range prices {
			observe(d, quotetest.Bid(fmt.Sprintf("venue%d", i), market, price))
		}

		now := time.Now()
		d.mu.Lock()
		bestBid, lowBid, ok, _ := d.bestSide(d.latest[market]["bid"], now, highestPrice)
		bestAsk, highAsk, _, _ := d.bestSide(d.latest[market]["bid"], now, lowestPrice)
		d.mu.Unlock()

		if !ok {
			t.Fatal("bestSide reported no live venues, want three")
		}
		if bestBid.Price != 102.00 {
			t.Errorf("highest = %v, want 102.00", bestBid.Price)
		}
		if lowBid != 100.00 {
			t.Errorf("far end of the bid side = %v, want 100.00", lowBid)
		}
		// The same book read as an ask side yields the opposite ends.
		if bestAsk.Price != 100.00 {
			t.Errorf("lowest = %v, want 100.00", bestAsk.Price)
		}
		if highAsk != 102.00 {
			t.Errorf("far end of the ask side = %v, want 102.00", highAsk)
		}
	}
}
