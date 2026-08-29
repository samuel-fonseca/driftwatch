package binance

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/samuel-fonseca/driftwatch/internal/normalize"
	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

const defaultExchangeInfoURL = "https://data-api.binance.vision/api/v3/exchangeInfo?symbolStatus=TRADING"

type tickerSymbol struct {
	Symbol               string `json:"symbol"`     // e.g. ETHBTC
	Status               string `json:"status"`     // e.g. TRADING
	BaseAsset            string `json:"baseAsset"`  // e,g, ETH
	QuoteAsset           string `json:"quoteAsset"` // e.g. BTC
	IsSpotTradingAllowed bool   `json:"isSpotTradingAllowed"`
}

func tickerLoader(ctx context.Context, h *poller.HTTP, exchangeInfoURL string) (symbols.Table, error) {
	body, err := h.Get(ctx, exchangeInfoURL)
	if err != nil {
		return nil, fmt.Errorf("fetching tickers: %w", err)
	}

	table, err := parseTickers(body)
	if err != nil {
		return nil, fmt.Errorf("parsing tickers: %w", err)
	}

	return table, nil
}

func parseTickers(body []byte) (symbols.Table, error) {
	var resp struct {
		Symbols []tickerSymbol `json:"symbols"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshalling tickers: %w", err)
	}

	var table = make(symbols.Table)
	for _, s := range resp.Symbols {
		if s.Status != "TRADING" || !s.IsSpotTradingAllowed {
			continue
		}

		base := normalize.CanonicalAsset(s.BaseAsset)
		quoteAsset := normalize.CanonicalAsset(s.QuoteAsset)
		table[s.Symbol] = symbols.Instrument{
			Symbol: s.Symbol,
			Base:   base,
			Quote:  quoteAsset,
			Market: normalize.Market(base, quoteAsset),
		}
	}
	return table, nil
}
