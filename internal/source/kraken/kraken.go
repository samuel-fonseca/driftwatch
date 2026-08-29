package kraken

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/samuel-fonseca/driftwatch/internal/source"
	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

const (
	venue             = "kraken"
	defaultTickersURL = "https://api.kraken.com/0/public/Ticker"
)

type Config struct {
	TickersURL, AssetPairsURL string
	Tuning                    poller.Tuning
}

func (c Config) withDefaults() Config {
	c.TickersURL = cmp.Or(c.TickersURL, defaultTickersURL)
	c.AssetPairsURL = cmp.Or(c.AssetPairsURL, defaultAssetPairsURL)
	c.Tuning = c.Tuning.WithDefaults()
	return c
}

type Adapter struct{ *poller.Poller }

var _ source.Source = (*Adapter)(nil)

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
			return tickerLoader(ctx, h, cfg.AssetPairsURL)
		},
	})}
}

type tickerEntry struct {
	Ask []string `json:"a"`
	Bid []string `json:"b"`
}

func asFloat(v string) (float64, bool) {
	f, err := strconv.ParseFloat(v, 64)
	return f, err == nil
}

func parseTicks(data []byte) ([]poller.Tick, error) {
	var resp struct {
		Error  []string               `json:"error"`
		Result map[string]tickerEntry `json:"result"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ticker response: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("Kraken returned: %v", resp.Error)
	}

	ticks := make([]poller.Tick, 0, len(resp.Result))
	for symbol, e := range resp.Result {
		if len(e.Bid) < 3 || len(e.Ask) < 3 {
			continue
		}
		bidPrice, ok := asFloat(e.Bid[0])
		if !ok {
			continue
		}
		bidSize, ok := asFloat(e.Bid[2])
		if !ok {
			continue
		}
		askPrice, ok := asFloat(e.Ask[0])
		if !ok {
			continue
		}
		askSize, ok := asFloat(e.Ask[2])
		if !ok {
			continue
		}

		ticks = append(ticks, poller.Tick{
			Symbol:   symbol,
			BidPrice: bidPrice, BidSize: bidSize,
			AskPrice: askPrice, AskSize: askSize,
		})
	}
	return ticks, nil
}
