package divergence

import (
	"math"
	"sync"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

type Signal struct {
	Market     string
	BidVenue   string
	BidPrice   float64
	AskVenue   string
	AskPrice   float64
	EdgeBps    float64
	StalestLeg time.Duration
	DetectedAt time.Time
}

type Detector struct {
	mu               sync.Mutex
	latest           map[string]map[string]map[string]quote.Quote
	edgeThresholdBps float64
	staleThreshold   time.Duration
	observed,
	emitted,
	suppressedInvalidSelection,
	suppressedIncompleteBook,
	suppressedNotCrossed,
	SuppressedSameVenue,
	suppressedBelowThreshold,
	suppressedStale int64
}

func New(edgeThresholdBps float64, staleThreshold time.Duration) *Detector {
	return &Detector{
		latest:           make(map[string]map[string]map[string]quote.Quote),
		edgeThresholdBps: edgeThresholdBps,
		staleThreshold:   staleThreshold,
	}
}

type Stats struct {
	Observed,
	Emitted,
	SuppressedInvalidSelection,
	SuppressedIncompleteBook,
	SuppressedNotCrossed,
	SuppressedSameVenue,
	SuppressedBelowThreshold,
	SuppressedStale int64
}

func (d *Detector) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()

	return Stats{
		Observed:                   d.observed,
		Emitted:                    d.emitted,
		SuppressedInvalidSelection: d.suppressedInvalidSelection,
		SuppressedIncompleteBook:   d.suppressedIncompleteBook,
		SuppressedNotCrossed:       d.suppressedNotCrossed,
		SuppressedSameVenue:        d.SuppressedSameVenue,
		SuppressedBelowThreshold:   d.suppressedBelowThreshold,
		SuppressedStale:            d.suppressedStale,
	}
}

func (d *Detector) Observe(q quote.Quote) *Signal {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.observed++

	// return early without storing
	if q.Selection != "bid" && q.Selection != "ask" {
		d.suppressedInvalidSelection++
		return nil
	}

	if d.latest[q.Market] == nil {
		d.latest[q.Market] = make(map[string]map[string]quote.Quote)
	}
	if d.latest[q.Market][q.Selection] == nil {
		d.latest[q.Market][q.Selection] = make(map[string]quote.Quote)
	}
	d.latest[q.Market][q.Selection][q.Venue] = q

	var bestBid, bestAsk quote.Quote
	var haveBid, haveAsk bool

	for _, bidQuote := range d.latest[q.Market]["bid"] {
		if !haveBid || bidQuote.Price > bestBid.Price {
			bestBid = bidQuote
			haveBid = true
		}
	}

	for _, askQuote := range d.latest[q.Market]["ask"] {
		if !haveAsk || askQuote.Price < bestAsk.Price {
			bestAsk = askQuote
			haveAsk = true
		}
	}

	if !haveBid || !haveAsk {
		d.suppressedIncompleteBook++
		return nil
	}

	if bestBid.Price <= bestAsk.Price {
		d.suppressedNotCrossed++
		return nil
	}

	if bestAsk.Venue == bestBid.Venue {
		d.SuppressedSameVenue++
		return nil
	}

	mid := (bestBid.Price + bestAsk.Price) / 2
	edgeBps := (bestBid.Price - bestAsk.Price) / mid * 10_000

	if edgeBps < d.edgeThresholdBps {
		d.suppressedBelowThreshold++
		return nil
	}

	bestAskTimeSinceObserved := time.Since(bestAsk.ObservedAt)
	bestBidTimeSinceObserved := time.Since(bestBid.ObservedAt)
	stalestLeg := time.Duration(math.Max(float64(bestAskTimeSinceObserved), float64(bestBidTimeSinceObserved)))

	if stalestLeg > d.staleThreshold {
		d.suppressedStale++
		return nil
	}

	d.emitted++

	return &Signal{
		Market:     q.Market,
		BidVenue:   bestBid.Venue,
		BidPrice:   bestBid.Price,
		AskVenue:   bestAsk.Venue,
		AskPrice:   bestAsk.Price,
		EdgeBps:    edgeBps,
		StalestLeg: stalestLeg,
		DetectedAt: time.Now(),
	}
}
