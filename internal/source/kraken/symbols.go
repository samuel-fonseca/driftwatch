package kraken

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/samuel-fonseca/driftwatch/internal/normalize"
	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

const defaultAssetPairsURL = "https://api.kraken.com/0/public/AssetPairs"

func tickerLoader(ctx context.Context, h *poller.HTTP, assetPairsURL string) (symbols.Table, error) {
	body, err := h.Get(ctx, assetPairsURL)
	if err != nil {
		return nil, fmt.Errorf("fetching asset pairs: %w", err)
	}

	table, err := buildTable(body)
	if err != nil {
		return nil, fmt.Errorf("parsing pairs: %w", err)
	}
	return table, nil
}

func buildTable(body []byte) (symbols.Table, error) {
	type pair struct {
		WsName string `json:"wsname"`
		Status string `json:"status"`
	}
	var resp struct {
		Error  []string        `json:"error"`
		Result map[string]pair `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshalling asset pairs: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("Kraken returned: %v", resp.Error)
	}
	if len(resp.Result) == 0 {
		return nil, errors.New("asset pairs returned no results")
	}

	table := make(symbols.Table)
	for symbol, pair := range resp.Result {
		if pair.Status != "online" {
			continue
		}
		base, quote, ok := strings.Cut(pair.WsName, "/")
		if !ok || base == "" || quote == "" {
			continue
		}
		base = normalize.CanonicalAsset(base)
		quote = normalize.CanonicalAsset(quote)
		table[symbol] = symbols.Instrument{
			Symbol: symbol,
			Base:   base,
			Quote:  quote,
			Market: normalize.Market(base, quote),
		}
	}
	if len(table) == 0 {
		return nil, fmt.Errorf(
			"all %d asset pairs were dropped -- check wsname/status",
			len(resp.Result),
		)
	}
	return table, nil
}
