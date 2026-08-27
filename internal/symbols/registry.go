package symbols

import (
	"context"
	"log"
	"sync"
	"time"
)

// Instrument represents a normalized instrument.
type Instrument struct {
	Symbol string // native to the venue: `BTCUSDT`, `tBTCUST`
	Base   string // canonical: `BTC`, `ETH`
	Quote  string // canonical: `USDT`, `USDC`
	Market string // base + quote combo: `BTC-USDT`, `ETH-USDC`
}

// Table is a normalized map of instruments by symbol.
type Table map[string]Instrument

// The loader function to load table of instruments
type Loader func(context.Context) (Table, error)

// Normalizer is a function that normalizes a symbol string into an Instrument.
type Lookup func(string) (Instrument, bool)

type Registry struct {
	mu      sync.Mutex
	table   Table
	loader  Loader
	refresh time.Duration
}

func NewRegistry(loader Loader, refresh time.Duration) *Registry {
	return &Registry{
		loader:  loader,
		refresh: refresh,
	}
}

func (r *Registry) Lookup(symbol string) (Instrument, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.table == nil {
		return Instrument{}, false
	}

	inst, ok := r.table[symbol]
	return inst, ok
}

func (r *Registry) Load(ctx context.Context) error {
	table, err := r.loader(ctx)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.table = table
	return nil
}

func (r *Registry) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.refresh)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.Load(ctx); err != nil {
				log.Printf("symbols refresh failed: %v", err) // print & move on
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
