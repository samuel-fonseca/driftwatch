package divergence

import (
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

func TestSignalFiresAcrossThreeVenues(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	observe(d, q("binance", "BTC-USD", "bid", 100.00, 0))
	observe(d, q("bitfinex", "BTC-USD", "bid", 100.50, 0))

	sig := observe(d, q("kraken", "BTC-USD", "ask", 100.00, 0))
	if sig == nil {
		t.Fatal("expected a signal, got nil")
	}
	if sig.BidVenue != "bitfinex" || sig.AskVenue != "kraken" {
		t.Errorf("expected best bid from bitfinex and best ask from kraken, got bid=%s ask=%s", sig.BidVenue, sig.AskVenue)
	}
	if sig.BidPrice != 100.50 || sig.AskPrice != 100.00 {
		t.Errorf("unexpected prices: bid=%v ask=%v", sig.BidPrice, sig.AskPrice)
	}
}

func TestObserveIgnoresInvalidSelection(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	if sig := observe(d, q("binance", "BTC-USD", "mid", 100.00, 0)); sig != nil {
		t.Fatalf("expected nil for non bid/ask selection, got %+v", sig)
	}
}

func TestNoSignalWithOnlyOneSide(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	if sig := observe(d, q("binance", "BTC-USD", "bid", 100.00, 0)); sig != nil {
		t.Fatalf("expected nil with only a bid quote, got %+v", sig)
	}
}

func TestNoSignalWhenBookCrossedTheWrongWay(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	observe(d, q("binance", "BTC-USD", "bid", 100.00, 0))
	if sig := observe(d, q("bitfinex", "BTC-USD", "ask", 101.00, 0)); sig != nil {
		t.Fatalf("expected nil when best bid is below best ask, got %+v", sig)
	}
}

func TestNoSignalWhenSameVenueOnBothSides(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	observe(d, q("binance", "BTC-USD", "bid", 101.00, 0))
	if sig := observe(d, q("binance", "BTC-USD", "ask", 100.00, 0)); sig != nil {
		t.Fatalf("expected nil when bid and ask are the same venue, got %+v", sig)
	}
}

func TestNoSignalWhenEdgeBelowThreshold(t *testing.T) {
	d := New(50, 2*time.Second, 4)

	observe(d, q("binance", "BTC-USD", "bid", 100.00, 0))
	if sig := observe(d, q("bitfinex", "BTC-USD", "ask", 99.99, 0)); sig != nil {
		t.Fatalf("expected nil when edge is below threshold, got %+v", sig)
	}
}

func TestNoSignalWhenLegIsStale(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	observe(d, q("binance", "BTC-USD", "bid", 100.50, 5*time.Second))
	if sig := observe(d, q("bitfinex", "BTC-USD", "ask", 100.00, 0)); sig != nil {
		t.Fatalf("expected nil when a leg is stale, got %+v", sig)
	}
}

// A venue holding a steady price is still a live venue. Dedupe drops those
// repeats before they reach Observe, so the stored quote's ObservedAt freezes
// while the venue keeps reporting - only MarkAlive sees it. Judging staleness
// from the stored quote would suppress a perfectly good signal here.
func TestStableLegStillSignalsWhileVenueIsAlive(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	// binance's bid last reached the detector 10s ago and has not changed
	// price since, so dedupe has swallowed every repeat.
	d.Observe(q("binance", "BTC-USD", "bid", 100.50, 10*time.Second))

	// ...but binance is very much alive: the raw stream still carries that
	// unchanged price on every poll.
	d.MarkAlive([]quote.Quote{q("binance", "BTC-USD", "bid", 100.50, 0)})

	sig := observe(d, q("kraken", "BTC-USD", "ask", 100.00, 0))
	if sig == nil {
		t.Fatalf("expected a signal from a live venue holding a steady price, got nil (stats: %+v)", d.Stats())
	}
	if sig.BidVenue != "binance" || sig.AskVenue != "kraken" {
		t.Errorf("got bid=%s ask=%s, want bid=binance ask=kraken", sig.BidVenue, sig.AskVenue)
	}
}

func TestDeadVenueIsExcludedFromSelection(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	// binance is live, with a bid that does not cross kraken's ask.
	observe(d, q("binance", "BTC-USD", "bid", 100.00, 0))

	// bitfinex holds a much better bid that would cross, but nothing has been
	// heard from it for far longer than the stale threshold.
	bitfinexBid := q("bitfinex", "BTC-USD", "bid", 100.50, 10*time.Second)
	d.MarkAlive([]quote.Quote{bitfinexBid})
	d.Observe(bitfinexBid)

	if sig := observe(d, q("kraken", "BTC-USD", "ask", 100.20, 0)); sig != nil {
		t.Fatalf("expected nil - the only crossing bid belongs to a dead venue - got %+v", sig)
	}
}

// Two venues can list entirely different assets under the same ticker. The
// prices give it away: nothing that is one asset trades 3000x apart on two
// venues at the same moment.
func TestCollidedTickerIsSuppressed(t *testing.T) {
	d := New(5, 2*time.Second, 4)

	observe(d, q("binance", "U-USD", "bid", 0.9993, 0))
	observe(d, q("kraken", "U-USD", "bid", 0.000305, 0))

	if sig := observe(d, q("kraken", "U-USD", "ask", 0.000305, 0)); sig != nil {
		t.Fatalf("expected nil for a ticker collision, got %+v", sig)
	}
	if got := d.Stats().SuppressedCollision; got == 0 {
		t.Error("SuppressedCollision = 0, want it to have tripped")
	}
	if got := d.Collided(); len(got) != 1 || got[0] != "U-USD" {
		t.Errorf("Collided() = %v, want [U-USD]", got)
	}
}

// Prices on one side span a ratio of at least 1 by definition, so a ratio of 1
// or below would flag every book and silence the detector entirely. Treat those
// as disabled instead -- failing open beats suppressing everything.
func TestCollisionCheckDisabledByUnusableRatio(t *testing.T) {
	for _, ratio := range []float64{0, 0.1, 1} {
		d := New(5, 2*time.Second, ratio)

		observe(d, q("binance", "U-USD", "bid", 0.9993, 0))
		observe(d, q("kraken", "U-USD", "bid", 0.000305, 0))

		if sig := observe(d, q("kraken", "U-USD", "ask", 0.000305, 0)); sig == nil {
			t.Errorf("ratio %v: expected the check to be disabled, got no signal (stats: %+v)", ratio, d.Stats())
		}
	}
}

// helpers

// observe mirrors the pipeline worker: every arriving quote marks its venue
// alive from the raw pre-dedupe batch before the surviving quote reaches the
// detector. Observe on its own never marks a venue live.
func observe(d *Detector, qt quote.Quote) *Signal {
	d.MarkAlive([]quote.Quote{qt})
	return d.Observe(qt)
}

func q(venue, market, selection string, price float64, age time.Duration) quote.Quote {
	return quote.Quote{
		Venue:      venue,
		Market:     market,
		Selection:  selection,
		Price:      price,
		ObservedAt: time.Now().Add(-age),
	}
}
