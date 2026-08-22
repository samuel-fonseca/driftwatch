package normalize

import (
	"strings"
)

var quoteAssets = []string{
	"USDT", "FDUSD", "TUSD", "BUSD", "USDC", "XAUT",
	"USD", "UST", "BTC", "ETH",
}

var stablecoins = map[string]string{
	"USDT":  "USD",
	"USDC":  "USD",
	"FDUSD": "USD",
	"TUSD":  "USD",
	"BUSD":  "USD",
	"UST":   "USD",
}

func Normalize(venue, symbol string) (market string, ok bool) {
	switch venue {
	case "binance":
		return normalizeBinance(symbol)
	case "bitfinex":
		return normalizeBitfinex(symbol)
	default:
		return "", false
	}
}

func normalizeBinance(symbol string) (market string, ok bool) {
	for _, asset := range quoteAssets {
		if base, found := strings.CutSuffix(symbol, asset); found {
			return base + "-" + collapseStablecoin(asset), true
		}
	}

	return "", false
}

func normalizeBitfinex(symbol string) (market string, ok bool) {
	if strings.HasPrefix(symbol, "f") {
		return "", false
	}

	if !strings.HasPrefix(symbol, "t") {
		return "", false
	}

	body := strings.TrimPrefix(symbol, "t")

	if base, asset, found := strings.Cut(body, ":"); found {
		return base + "-" + collapseStablecoin(asset), true
	}

	for _, asset := range quoteAssets {
		if base, found := strings.CutSuffix(body, asset); found {
			return base + "-" + collapseStablecoin(asset), true
		}
	}

	return "", false
}

func collapseStablecoin(quote string) string {
	if coin, found := stablecoins[quote]; found {
		return coin
	}
	return quote
}
