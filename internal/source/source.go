package source

import (
	"context"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

type Source interface {
	// Name identifies this source, e.g. "binance", "bitfinex". Used as
	// Quote.Venue.
	Name() string

	// Run Polls or streams from the venue, sending Quotes to out until
	// ctx is cancelled. Run must NOT return on transient errors (a
	// venue being briefly unreachable is normal operation) -- retry
	// internally with backoff. Run returns only on context cancellation
	// or a permanent failure (bad URL, unparseable schema).
	Run(ctx context.Context, out chan<- quote.Quote) error
}
