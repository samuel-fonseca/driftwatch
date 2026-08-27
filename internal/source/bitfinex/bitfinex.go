package bitfinex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/backoff"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
	"golang.org/x/sync/errgroup"
)

const (
	venue      = "bitfinex"
	tickersUrl = "https://api-pub.bitfinex.com/v2/tickers?symbols=ALL"
)

type Adapter struct {
	client         *http.Client
	registry       *symbols.Registry
	baseURL        string
	confURL        string
	pollInterval   time.Duration
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

func New() *Adapter {
	a := &Adapter{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL:        tickersUrl,
		confURL:        confURL,
		pollInterval:   2 * time.Second,
		initialBackoff: 500 * time.Millisecond,
		maxBackoff:     60 * time.Second,
	}
	a.registry = symbols.NewRegistry(a.tickerLoader, 24*time.Hour)
	return a
}

func (a *Adapter) Name() string { return venue }

func asFloat(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

func decode(data []byte, fetchedAt time.Time, lookup symbols.Lookup) ([]quote.Quote, error) {
	var rows [][]any
	var quotes []quote.Quote

	err := json.Unmarshal(data, &rows)
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		if len(row) < 5 {
			continue
		}
		symbol, ok := asString(row[0])
		if !ok {
			continue
		}
		inst, ok := lookup(symbol)
		if !ok {
			continue
		}

		bidPrice, ok := asFloat(row[1])
		if !ok {
			continue
		}
		bidSize, ok := asFloat(row[2])
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

		askPrice, ok := asFloat(row[3])
		if !ok {
			continue
		}
		askSize, ok := asFloat(row[4])
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
		return nil, fmt.Errorf("bitfinex returned status %d", resp.StatusCode)
	}

	return body, nil
}

func (a *Adapter) Run(ctx context.Context, out chan<- quote.Quote) error {
	if err := backoff.Sleep(ctx, backoff.Jitter(a.pollInterval)); err != nil {
		return err
	}
	if err := a.loadSymbolsTable(ctx); err != nil {
		return fmt.Errorf("loading symbols table: %w", err)
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
		log.Printf("loading symbols table: %v", err)
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
