package poller

import "time"

type Tick struct {
	Symbol            string
	BidPrice, BidSize float64
	AskPrice, AskSize float64
	ObservedAt        time.Time
}

// ParseFunc is a function that will parse the response from
// the market's ticks API into a new slice of Tick structs.
type ParseFunc func(body []byte) ([]Tick, error)
