package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/backoff"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
	"golang.org/x/sync/errgroup"
)

const (
	venue      = "binance"
	tickersUrl = "https://data-api.binance.vision/api/v3/ticker/bookTicker"
)

type Adapter struct {
	client          *http.Client
	registry        *symbols.Registry
	baseURL         string
	exchangeInfoURL string
	pollInterval    time.Duration
	initialBackoff  time.Duration
	maxBackoff      time.Duration
}

type bookTicker struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bidPrice"`
	BidSize  string `json:"bidQty"`
	AskPrice string `json:"askPrice"`
	AskSize  string `json:"askQty"`
}

func New() *Adapter {
	a := &Adapter{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL:         tickersUrl,
		exchangeInfoURL: exchangeInfoURL,
		pollInterval:    2 * time.Second,
		initialBackoff:  500 * time.Millisecond,
		maxBackoff:      60 * time.Second,
	}
	a.registry = symbols.NewRegistry(a.tickerLoader, 24*time.Hour)
	return a
}

func (a *Adapter) Name() string { return venue }

func asFloat(v string) (float64, bool) {
	f, err := strconv.ParseFloat(v, 64)
	return f, err == nil
}

func decode(data []byte, fetchedAt time.Time, lookup symbols.Lookup) ([]quote.Quote, error) {
	var tickers []bookTicker
	var quotes []quote.Quote

	err := json.Unmarshal(data, &tickers)
	if err != nil {
		return nil, err
	}

	for _, t := range tickers {
		inst, ok := lookup(t.Symbol)
		if !ok {
			continue
		}

		bidPrice, ok := asFloat(t.BidPrice)
		if !ok {
			continue
		}
		bidSize, ok := asFloat(t.BidSize)
		if !ok {
			continue
		}
		if bidPrice != 0 {
			quotes = append(quotes, quote.Quote{
				Venue:      venue,
				Market:     inst.Market,
				Selection:  "bid",
				Price:      bidPrice,
				Size:       bidSize,
				ObservedAt: fetchedAt,
				ReceivedAt: fetchedAt,
			})
		}

		askPrice, ok := asFloat(t.AskPrice)
		if !ok {
			continue
		}
		askSize, ok := asFloat(t.AskSize)
		if !ok {
			continue
		}
		if askPrice != 0 {
			quotes = append(quotes, quote.Quote{
				Venue:      venue,
				Market:     inst.Market,
				Selection:  "ask",
				Price:      askPrice,
				Size:       askSize,
				ObservedAt: fetchedAt,
				ReceivedAt: fetchedAt,
			})
		}
	}

	return quotes, nil
}

func (a *Adapter) fetch(ctx context.Context) ([]byte, error) {
	return a.get(ctx, a.baseURL)
}

func (a *Adapter) get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching tickers: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance returned status %d", resp.StatusCode)
	}

	return body, nil
}

func (a *Adapter) Run(ctx context.Context, out chan<- quote.Quote) error {
	if err := backoff.Sleep(ctx, backoff.Jitter(a.pollInterval)); err != nil {
		return fmt.Errorf("sleeping: %w", err)
	}
	if err := a.loadSymbolsTable(ctx); err != nil {
		return fmt.Errorf("loading symbols: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return a.registry.Run(ctx) })
	g.Go(func() error { return a.poll(ctx, out) })
	return g.Wait()
}

func (a *Adapter) loadSymbolsTable(ctx context.Context) error {
	d := a.initialBackoff
	for {
		err := a.registry.Load(ctx)
		if err == nil {
			break
		}
		log.Printf("loading symbols: %v", err)
		if err := backoff.Sleep(ctx, backoff.Jitter(d)); err != nil {
			return fmt.Errorf("sleeping: %w", err)
		}
		d = backoff.Next(d, a.maxBackoff)
	}
	return nil
}

func (a *Adapter) poll(ctx context.Context, out chan<- quote.Quote) error {
	backoffDuration := a.initialBackoff
	for {
		body, err := a.fetch(ctx)
		if err != nil {
			log.Printf("fetching quotes: %v", err)
			if err := backoff.Sleep(ctx, backoff.Jitter(backoffDuration)); err != nil {
				return err
			}
			backoffDuration = backoff.Next(backoffDuration, a.maxBackoff)
			continue
		}

		quotes, err := decode(body, time.Now(), a.registry.Lookup)
		if err != nil {
			log.Printf("decoding quotes: %v", err)
			if err := backoff.Sleep(ctx, backoff.Jitter(backoffDuration)); err != nil {
				return err
			}
			backoffDuration = backoff.Next(backoffDuration, a.maxBackoff)
			continue
		}
		for _, q := range quotes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- q:
			}
		}

		if err := backoff.Sleep(ctx, backoff.Jitter(a.pollInterval)); err != nil {
			return err
		}
		backoffDuration = a.initialBackoff
	}
}
