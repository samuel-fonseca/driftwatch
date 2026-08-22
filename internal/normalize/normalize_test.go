package normalize

import (
	"testing"
)

func TestNormalize(t *testing.T) {
	cases := []struct {
		name       string
		venue      string
		symbol     string
		wantMarket string
		wantOk     bool
	}{
		{
			name:       "binance simple USDT pair collapses to USD",
			venue:      "binance",
			symbol:     "BTCUSDT",
			wantMarket: "BTC-USD",
			wantOk:     true,
		},
		{
			name:       "bitfinex simple USD pair, t-prefix stripped",
			venue:      "bitfinex",
			symbol:     "tBTCUSD",
			wantMarket: "BTC-USD",
			wantOk:     true,
		},
		{
			name:       "bitfinex UST collapses to USD, matching binance USDT",
			venue:      "bitfinex",
			symbol:     "tBTCUST",
			wantMarket: "BTC-USD", // currently FAILS until you add UST to stablecoins
			wantOk:     true,
		},
		{
			name:       "bitfinex colon-separated pair",
			venue:      "bitfinex",
			symbol:     "tDOGE:USD",
			wantMarket: "DOGE-USD",
			wantOk:     true,
		},
		{
			name:       "bitfinex colon-separated pair, non-USD quote asset",
			venue:      "bitfinex",
			symbol:     "tBTC:XAUT",
			wantMarket: "BTC-XAUT",
			wantOk:     true,
		},
		{
			name:   "bitfinex funding row rejected outright",
			venue:  "bitfinex",
			symbol: "fUSD",
			wantOk: false,
		},
		{
			name:   "bitfinex funding row with longer symbol still rejected",
			venue:  "bitfinex",
			symbol: "fTESTUSDT",
			wantOk: false,
		},
		{
			name:   "unsplittable binance symbol dropped, not guessed",
			venue:  "binance",
			symbol: "BTCXYZ",
			wantOk: false,
		},
		{
			name:       "bitfinex perpetual future — documented gap, not a spot market",
			venue:      "bitfinex",
			symbol:     "tBTCF0:USTF0",
			wantMarket: "BTCF0-USTF0",
			wantOk:     true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotMarket, gotOk := Normalize(c.venue, c.symbol)
			if gotOk != c.wantOk {
				t.Errorf("ok = %v, want %v", gotOk, c.wantOk)
			}
			if gotMarket != c.wantMarket {
				t.Errorf("market = %v, want %v", gotMarket, c.wantMarket)
			}
		})
	}
}

func TextCrossRevenueAgreement(t *testing.T) {
	cases := []struct {
		name           string
		binanceSymbol  string
		bitfinexSymbol string
	}{
		{"USDT vs plain USD", "BTCUSDT", "tBTCUSD"},
		{"USDT vs UST (bitfinex's own tether ticker)", "BTCUSDT", "tBTCUST"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			binanceMarket, ok := Normalize("binance", c.binanceSymbol)
			if !ok {
				t.Fatalf("expected binance %s to normalize", c.binanceSymbol)
			}

			bitfinexMarket, ok := Normalize("bitfinex", c.bitfinexSymbol)
			if !ok {
				t.Fatalf("expected bitfinex %s to normalize", c.bitfinexSymbol)
			}

			if binanceMarket != bitfinexMarket {
				t.Errorf("venues disagree: binance=%v, bitfinex=%v", binanceMarket, bitfinexMarket)
			}
		})
	}
}
