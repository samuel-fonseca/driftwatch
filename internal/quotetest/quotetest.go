// Package quotetest builds quote.Quote values for tests.
//
// Most tests care about two or three fields of a Quote and are obscured by
// the rest: a buffer test wants a distinct Key, a dedupe test wants a price
// and a size, a divergence test wants an age. The constructors here take the
// fields that identify a quote positionally and leave the others to options,
// so each call site states only what the test is actually about.
//
// Nothing outside _test.go files imports this package.
package quotetest

import (
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

// Defaults applied before any option runs. A non-zero size keeps quotes
// distinguishable by fingerprint, and a real observation time keeps them
// clear of the zero-time handling the poller has to do for venues that
// publish no per-tick timestamp.
const defaultSize = 1.0

// An Option overrides a field the positional constructors do not take.
type Option func(*quote.Quote)

// Size sets the resting size at the quoted price.
func Size(s float64) Option {
	return func(q *quote.Quote) { q.Size = s }
}

// At pins both timestamps to t. Use it when a test depends on the exact
// instant -- a psql partition boundary, say -- rather than on recency.
func At(t time.Time) Option {
	return func(q *quote.Quote) { q.ObservedAt, q.ReceivedAt = t, t }
}

// Ago backdates the observation by d, for tests about staleness.
func Ago(d time.Duration) Option {
	return func(q *quote.Quote) { q.ObservedAt = q.ReceivedAt.Add(-d) }
}

// Sel builds a quote for an arbitrary selection. Prefer Bid or Ask; this
// exists for the tests that drive selections the detector should reject, and
// for those that use the selection purely as a key discriminator.
func Sel(venue, market, selection string, price float64, opts ...Option) quote.Quote {
	now := time.Now()
	q := quote.Quote{
		Venue:      venue,
		Market:     market,
		Selection:  selection,
		Price:      price,
		Size:       defaultSize,
		ObservedAt: now,
		ReceivedAt: now,
	}
	for _, opt := range opts {
		opt(&q)
	}
	return q
}

// Bid builds a bid-side quote for venue's market at price.
func Bid(venue, market string, price float64, opts ...Option) quote.Quote {
	return Sel(venue, market, "bid", price, opts...)
}

// Ask builds an ask-side quote for venue's market at price.
func Ask(venue, market string, price float64, opts ...Option) quote.Quote {
	return Sel(venue, market, "ask", price, opts...)
}
