package divergence

import (
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

func TestSignalFiresAcrossThreeVenues(t *testing.T) {
	d := New(5, 2*time.Second)

	d.Observe(q("binance", "BTC-USD", "bid", 100.00, 0))
	d.Observe(q("bitfinex", "BTC-USD", "bid", 100.50, 0))

	sig := d.Observe(q("kraken", "BTC-USD", "ask", 100.00, 0))
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
	d := New(5, 2*time.Second)

	if sig := d.Observe(q("binance", "BTC-USD", "mid", 100.00, 0)); sig != nil {
		t.Fatalf("expected nil for non bid/ask selection, got %+v", sig)
	}
}

func TestNoSignalWithOnlyOneSide(t *testing.T) {
	d := New(5, 2*time.Second)

	if sig := d.Observe(q("binance", "BTC-USD", "bid", 100.00, 0)); sig != nil {
		t.Fatalf("expected nil with only a bid quote, got %+v", sig)
	}
}

func TestNoSignalWhenBookCrossedTheWrongWay(t *testing.T) {
	d := New(5, 2*time.Second)

	d.Observe(q("binance", "BTC-USD", "bid", 100.00, 0))
	if sig := d.Observe(q("bitfinex", "BTC-USD", "ask", 101.00, 0)); sig != nil {
		t.Fatalf("expected nil when best bid is below best ask, got %+v", sig)
	}
}

func TestNoSignalWhenSameVenueOnBothSides(t *testing.T) {
	d := New(5, 2*time.Second)

	d.Observe(q("binance", "BTC-USD", "bid", 101.00, 0))
	if sig := d.Observe(q("binance", "BTC-USD", "ask", 100.00, 0)); sig != nil {
		t.Fatalf("expected nil when bid and ask are the same venue, got %+v", sig)
	}
}

func TestNoSignalWhenEdgeBelowThreshold(t *testing.T) {
	d := New(50, 2*time.Second)

	d.Observe(q("binance", "BTC-USD", "bid", 100.00, 0))
	if sig := d.Observe(q("bitfinex", "BTC-USD", "ask", 99.99, 0)); sig != nil {
		t.Fatalf("expected nil when edge is below threshold, got %+v", sig)
	}
}

func TestNoSignalWhenLegIsStale(t *testing.T) {
	d := New(5, 2*time.Second)

	d.Observe(q("binance", "BTC-USD", "bid", 100.50, 5*time.Second))
	if sig := d.Observe(q("bitfinex", "BTC-USD", "ask", 100.00, 0)); sig != nil {
		t.Fatalf("expected nil when a leg is stale, got %+v", sig)
	}
}

// helpers
func q(venue, market, selection string, price float64, age time.Duration) quote.Quote {
	return quote.Quote{
		Venue:      venue,
		Market:     market,
		Selection:  selection,
		Price:      price,
		ObservedAt: time.Now().Add(-age),
	}
}
