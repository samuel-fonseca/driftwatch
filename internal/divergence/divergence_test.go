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
