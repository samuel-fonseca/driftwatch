package bitfinex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/normalize"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

const (
	venue      = "bitfinex"
	tickersUrl = "https://api-pub.bitfinex.com/v2/tickers?symbols=ALL"
)

type Adapter struct {
	client         *http.Client
	baseURL        string
	pollInterval   time.Duration
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

func New() *Adapter {
	return &Adapter{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL:        tickersUrl,
		pollInterval:   2 * time.Second,
		initialBackoff: 500 * time.Millisecond,
		maxBackoff:     60 * time.Second,
	}
}

func (a *Adapter) Name() string { return "bitfinex" }

func asFloat(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

func decode(data []byte, fetchedAt time.Time) ([]quote.Quote, error) {
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
		normalizedSymbol, ok := normalize.Normalize(venue, symbol)
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
				Market:     normalizedSymbol,
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
				Market:     normalizedSymbol,
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL, nil)
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

func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(max)))
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	if current >= max {
		return max
	}

	next := current * 2
	if next > max {
		return max
	}

	return next
}

func (a *Adapter) Run(ctx context.Context, out chan<- quote.Quote) error {
	if err := sleep(ctx, jitter(a.pollInterval)); err != nil {
		return err
	}

	backoff := a.initialBackoff
	for {
		body, err := a.fetch(ctx)
		if err != nil {
			log.Printf("fetching quotes: %v", err)
			if err := sleep(ctx, jitter(backoff)); err != nil {
				return err
			}
			backoff = nextBackoff(backoff, a.maxBackoff)
			continue
		}

		quotes, err := decode(body, time.Now())
		if err != nil {
			log.Printf("decoding quotes: %v", err)
			if err := sleep(ctx, jitter(backoff)); err != nil {
				return err
			}
			backoff = nextBackoff(backoff, a.maxBackoff)
			continue
		}

		for _, q := range quotes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- q:
			}
		}

		if err := sleep(ctx, jitter(a.pollInterval)); err != nil {
			return err
		}
		backoff = a.initialBackoff
	}
}
