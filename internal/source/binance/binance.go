package binance

import (
	"cmp"
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/source"
	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

const (
	venue             = "binance"
	defaultTickersURL = "https://data-api.binance.vision/api/v3/ticker/bookTicker"
)

type Config struct {
	TickersURL, ExchangeInfoURL string
	Tuning                      poller.Tuning
}

func (c Config) withDefaults() Config {
	c.TickersURL = cmp.Or(c.TickersURL, defaultTickersURL)
	c.ExchangeInfoURL = cmp.Or(c.ExchangeInfoURL, defaultExchangeInfoURL)
	c.Tuning = c.Tuning.WithDefaults()
	return c
}

type Adapter struct{ *poller.Poller }

var _ source.Source = (*Adapter)(nil) // assert our contract is fulfilled

type bookTicker struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bidPrice"`
	BidSize  string `json:"bidQty"`
	AskPrice string `json:"askPrice"`
	AskSize  string `json:"askQty"`
}

func New(cfg Config) *Adapter {
	cfg = cfg.withDefaults()
	h := poller.NewHTTP(venue, cfg.Tuning.HTTPTimeout)
	return &Adapter{poller.New(poller.Config{
		Venue:      venue,
		TickersURL: cfg.TickersURL,
		HTTP:       h,
		Parse:      parseTicks,
		Tuning:     cfg.Tuning,
		Loader: func(ctx context.Context) (symbols.Table, error) {
			return tickerLoader(ctx, h, cfg.ExchangeInfoURL)
		},
	})}
}

func asFloat(v string) (float64, bool) {
	f, err := strconv.ParseFloat(v, 64)
	return f, err == nil
}

func parseTicks(data []byte) ([]poller.Tick, error) {
	var tickers []bookTicker
	var ticks []poller.Tick

	err := json.Unmarshal(data, &tickers)
	if err != nil {
		return nil, err
	}

	for _, t := range tickers {
		bidPrice, ok := asFloat(t.BidPrice)
		if !ok {
			continue
		}
		bidSize, ok := asFloat(t.BidSize)
		if !ok {
			continue
		}
		askPrice, ok := asFloat(t.AskPrice)
		if !ok {
			continue
		}
		askSize, ok := asFloat(t.AskSize)
		if !ok {
			continue
		}
		ticks = append(ticks, poller.Tick{
			Symbol:     t.Symbol,
			BidPrice:   bidPrice,
			BidSize:    bidSize,
			AskPrice:   askPrice,
			AskSize:    askSize,
			ObservedAt: time.Now(),
		})
	}

	return ticks, nil
}
