package bitfinex

import (
	"cmp"
	"context"
	"encoding/json"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/source"
	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

const (
	venue             = "bitfinex"
	defaultTickersURL = "https://api-pub.bitfinex.com/v2/tickers?symbols=ALL"
)

type Config struct {
	TickersURL, confURL string
	Tuning              poller.Tuning
}

func (c Config) withDefaults() Config {
	c.TickersURL = cmp.Or(c.TickersURL, defaultTickersURL)
	c.confURL = cmp.Or(c.confURL, defaultConfURL)
	c.Tuning = c.Tuning.WithDefaults()
	return c
}

type Adapter struct{ *poller.Poller }

var _ source.Source = (*Adapter)(nil) // assert our contract is fulfilled

func New(cfg Config) *Adapter {
	cfg = cfg.withDefaults()
	h := poller.NewHTTP(venue, cfg.Tuning.HTTPTimeout)
	return &Adapter{poller.New(poller.Config{
		Venue:      venue,
		TickersURL: cfg.TickersURL,
		HTTP:       h,
		Tuning:     cfg.Tuning,
		Parse:      parseTicks,
		Loader: func(ctx context.Context) (symbols.Table, error) {
			return tickerLoader(ctx, h, cfg.confURL)
		},
	})}
}

func asFloat(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

func parseTicks(data []byte) ([]poller.Tick, error) {
	var rows [][]any
	var ticks []poller.Tick

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
		bidPrice, ok := asFloat(row[1])
		if !ok {
			continue
		}
		bidSize, ok := asFloat(row[2])
		if !ok {
			continue
		}
		askPrice, ok := asFloat(row[3])
		if !ok {
			continue
		}
		askSize, ok := asFloat(row[4])
		if !ok {
			continue
		}
		ticks = append(ticks, poller.Tick{
			Symbol:     symbol,
			BidPrice:   bidPrice,
			BidSize:    bidSize,
			AskPrice:   askPrice,
			AskSize:    askSize,
			ObservedAt: time.Now(),
		})
	}

	return ticks, nil
}
