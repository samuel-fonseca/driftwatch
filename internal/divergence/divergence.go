package divergence

import (
	"slices"
	"sync"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

type Signal struct {
	Market string

	BidVenue          string
	BidPrice, BidSize float64

	AskVenue          string
	AskPrice, AskSize float64

	EdgeBps    float64
	StalestLeg time.Duration
	DetectedAt time.Time
}

type Detector struct {
	mu             sync.Mutex
	latest         map[string]map[string]map[string]quote.Quote
	aliveAt        map[string]time.Time
	collided       map[string]struct{}
	staleThreshold time.Duration

	edgeThresholdBps,
	collisionRatio float64

	observed,
	emitted,
	suppressedInvalidSelection,
	suppressedIncompleteBook,
	suppressedNotCrossed,
	suppressedSameVenue,
	suppressedBelowThreshold,
	suppressedStale,
	suppressedStaleArrival,
	suppressedCollision int64
}

func New(edgeThresholdBps float64, staleThreshold time.Duration, collisionRatio float64) *Detector {
	return &Detector{
		latest:           make(map[string]map[string]map[string]quote.Quote),
		aliveAt:          make(map[string]time.Time),
		collided:         make(map[string]struct{}),
		edgeThresholdBps: edgeThresholdBps,
		staleThreshold:   staleThreshold,
		collisionRatio:   collisionRatio,
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
	SuppressedStale,
	SuppressedStaleArrival,
	SuppressedCollision,
	MarketsTracked,
	MarketsCrossable,
	MarketsCollided int64
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
		SuppressedSameVenue:        d.suppressedSameVenue,
		SuppressedBelowThreshold:   d.suppressedBelowThreshold,
		SuppressedStale:            d.suppressedStale,
		SuppressedStaleArrival:     d.suppressedStaleArrival,
		SuppressedCollision:        d.suppressedCollision,
		MarketsTracked:             int64(len(d.latest)),
		MarketsCrossable:           d.marketsCrossable(time.Now()),
		MarketsCollided:            int64(len(d.collided)),
	}
}

func (d *Detector) Collided() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	markets := make([]string, 0, len(d.collided))
	for market := range d.collided {
		markets = append(markets, market)
	}
	slices.Sort(markets)
	return markets
}

// marketsCrossable counts the markets that could actually emit right now: a
// live bid on one venue and a live ask on a different one. A market that never
// clears this bar is structurally incapable of signalling, which the
// suppression counters alone do not distinguish from a market that simply
// never crossed.
func (d *Detector) marketsCrossable(now time.Time) int64 {
	var n int64
	for _, sides := range d.latest {
		bidVenue, bidCount := d.liveVenues(sides["bid"], now)
		askVenue, askCount := d.liveVenues(sides["ask"], now)
		if bidCount == 0 || askCount == 0 {
			continue
		}
		// Both sides quoted, but by the same lone venue: cannot cross.
		if bidCount == 1 && askCount == 1 && bidVenue == askVenue {
			continue
		}
		n++
	}
	return n
}

// liveVenues counts the live venues on one side of a book and returns one of
// their names, which is all marketsCrossable needs to tell a lone venue quoting
// both sides from two that can actually cross. Returning a count rather than a
// slice keeps this allocation-free: it runs for every market on every scrape,
// holding the same lock Observe needs.
func (d *Detector) liveVenues(byVenue map[string]quote.Quote, now time.Time) (venue string, n int) {
	for v := range byVenue {
		if d.isLive(v, now) {
			venue = v
			n++
		}
	}
	return venue, n
}

func (d *Detector) MarkAlive(quotes []quote.Quote) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, q := range quotes {
		if at, ok := d.aliveAt[q.Venue]; !ok || q.ObservedAt.After(at) {
			d.aliveAt[q.Venue] = q.ObservedAt
		}
	}
}

func (d *Detector) Observe(q quote.Quote) *Signal {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.observed++
	if q.Selection != "bid" && q.Selection != "ask" {
		d.suppressedInvalidSelection++
		return nil
	}
	if prev, ok := d.latest[q.Market][q.Selection][q.Venue]; ok &&
		q.ObservedAt.Before(prev.ObservedAt) {
		d.suppressedStaleArrival++
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
	var skippedStale bool
	var lowestBid, highestAsk float64

	now := time.Now()
	for venue, bidQuote := range d.latest[q.Market]["bid"] {
		if !d.isLive(venue, now) {
			skippedStale = true
			continue
		}
		if !haveBid || bidQuote.Price > bestBid.Price {
			bestBid = bidQuote
		}
		if !haveBid || bidQuote.Price < lowestBid {
			lowestBid = bidQuote.Price
		}
		haveBid = true
	}

	for venue, askQuote := range d.latest[q.Market]["ask"] {
		if !d.isLive(venue, now) {
			skippedStale = true
			continue
		}
		if !haveAsk || askQuote.Price < bestAsk.Price {
			bestAsk = askQuote
		}
		if !haveAsk || askQuote.Price > highestAsk {
			highestAsk = askQuote.Price
		}
		haveAsk = true
	}

	if !haveBid || !haveAsk {
		if skippedStale {
			d.suppressedStale++
		} else {
			d.suppressedIncompleteBook++
		}
		return nil
	}

	if dispersed(bestBid.Price, lowestBid, d.collisionRatio) ||
		dispersed(highestAsk, bestAsk.Price, d.collisionRatio) {
		d.suppressedCollision++
		d.collided[q.Market] = struct{}{}
		return nil
	}

	if bestBid.Price <= bestAsk.Price {
		d.suppressedNotCrossed++
		return nil
	}

	if bestAsk.Venue == bestBid.Venue {
		d.suppressedSameVenue++
		return nil
	}

	mid := (bestBid.Price + bestAsk.Price) / 2
	edgeBps := (bestBid.Price - bestAsk.Price) / mid * 10_000

	if edgeBps < d.edgeThresholdBps {
		d.suppressedBelowThreshold++
		return nil
	}

	d.emitted++
	stalestLeg := max(now.Sub(d.aliveAt[bestBid.Venue]), now.Sub(d.aliveAt[bestAsk.Venue]))

	return &Signal{
		Market:     q.Market,
		BidVenue:   bestBid.Venue,
		BidPrice:   bestBid.Price,
		BidSize:    bestBid.Size,
		AskVenue:   bestAsk.Venue,
		AskPrice:   bestAsk.Price,
		AskSize:    bestAsk.Size,
		EdgeBps:    edgeBps,
		StalestLeg: stalestLeg,
		DetectedAt: time.Now(),
	}
}

func dispersed(high, low, ratio float64) bool {
	return ratio > 1 && low > 0 && high/low > ratio
}

func (d *Detector) isLive(venue string, now time.Time) bool {
	at, ok := d.aliveAt[venue]
	return ok && now.Sub(at) <= d.staleThreshold
}
