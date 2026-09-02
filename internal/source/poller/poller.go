package poller

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/backoff"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
	"golang.org/x/sync/errgroup"
)

type Tuning struct {
	HTTPTimeout    time.Duration
	PollInterval   time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	SymbolsRefresh time.Duration
}

func (t Tuning) WithDefaults() Tuning {
	t.HTTPTimeout = cmp.Or(t.HTTPTimeout, 10*time.Second)
	t.PollInterval = cmp.Or(t.PollInterval, 2*time.Second)
	t.InitialBackoff = cmp.Or(t.InitialBackoff, 500*time.Millisecond)
	t.MaxBackoff = cmp.Or(t.MaxBackoff, 60*time.Second)
	t.SymbolsRefresh = cmp.Or(t.SymbolsRefresh, 24*time.Hour)
	return t
}

type Config struct {
	Venue, TickersURL string
	HTTP              *HTTP
	Loader            symbols.Loader
	Parse             ParseFunc
	Tuning            Tuning
}

type Poller struct {
	cfg      Config
	registry *symbols.Registry
}

func New(cfg Config) *Poller {
	cfg.Tuning = cfg.Tuning.WithDefaults()
	return &Poller{
		cfg:      cfg,
		registry: symbols.NewRegistry(cfg.Loader, cfg.Tuning.SymbolsRefresh),
	}
}

func (p *Poller) Name() string {
	return p.cfg.Venue
}

func (p *Poller) Run(ctx context.Context, out chan<- quote.Quote) error {
	if p.cfg.Venue == "" || p.cfg.TickersURL == "" || p.cfg.Parse == nil || p.cfg.Loader == nil {
		return fmt.Errorf("venue, tickersURL, parse, and loader are required")
	}
	if err := backoff.Sleep(ctx, backoff.Jitter(p.cfg.Tuning.PollInterval)); err != nil {
		return fmt.Errorf("sleeping %w", err)
	}
	if err := p.loadSymbols(ctx); err != nil {
		return fmt.Errorf("loading symbols table: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return p.registry.Run(ctx) })
	g.Go(func() error { return p.poll(ctx, out) })
	return g.Wait()
}

func (p *Poller) poll(ctx context.Context, out chan<- quote.Quote) error {
	tuning := p.cfg.Tuning
	backoffDuration := tuning.InitialBackoff
	for {
		body, err := p.cfg.HTTP.Get(ctx, p.cfg.TickersURL)
		if err != nil {
			log.Printf("fetching quotes: %v", err)
			if err := backoff.Sleep(ctx, backoff.Jitter(backoffDuration)); err != nil {
				return fmt.Errorf("sleeping %w", err)
			}
			backoffDuration = backoff.Next(backoffDuration, tuning.MaxBackoff)
			continue
		}

		ticks, err := p.cfg.Parse(body)
		if err != nil {
			log.Printf("parsing quotes: %v", err)
			if err := backoff.Sleep(ctx, backoff.Jitter(backoffDuration)); err != nil {
				return fmt.Errorf("sleeping %w", err)
			}
			backoffDuration = backoff.Next(backoffDuration, tuning.MaxBackoff)
			continue
		}

		quotes := p.toQuotes(ticks, time.Now())
		for _, q := range quotes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- q:
			}
		}

		if err := backoff.Sleep(ctx, backoff.Jitter(backoffDuration)); err != nil {
			return fmt.Errorf("sleeping %w", err)
		}
		backoffDuration = tuning.InitialBackoff
	}
}

func (p *Poller) loadSymbols(ctx context.Context) error {
	return p.registry.Load(ctx)
}

func (p *Poller) toQuotes(ticks []Tick, fetchedAt time.Time) []quote.Quote {
	var quotes []quote.Quote

	for _, tick := range ticks {
		inst, ok := p.registry.Lookup(tick.Symbol)
		if !ok {
			continue
		}

		observed := tick.ObservedAt
		if observed.IsZero() {
			observed = fetchedAt
		}
		if tick.BidPrice != 0 {
			quotes = append(quotes, quote.Quote{
				Venue:      p.cfg.Venue,
				Market:     inst.Market,
				Selection:  "bid",
				Price:      tick.BidPrice,
				Size:       tick.BidSize,
				ObservedAt: observed,
				ReceivedAt: fetchedAt,
			})
		}
		if tick.AskPrice != 0 {
			quotes = append(quotes, quote.Quote{
				Venue:      p.cfg.Venue,
				Market:     inst.Market,
				Selection:  "ask",
				Price:      tick.AskPrice,
				Size:       tick.AskSize,
				ObservedAt: observed,
				ReceivedAt: fetchedAt,
			})
		}
	}
	return quotes
}
