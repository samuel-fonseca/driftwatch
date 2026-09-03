package normalize

import "testing"

func TestCanonicalAsset(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"kraken bitcoin shorthand", "XBT", "BTC"},
		{"kraken prefixed bitcoin", "XXBT", "BTC"},
		{"kraken prefixed dollar", "ZUSD", "USD"},

		// Codes with no mapping pass through untouched.
		{"plain bitcoin", "BTC", "BTC"},
		{"ether", "ETH", "ETH"},
		{"tether", "USDT", "USDT"},
		{"euro", "EUR", "EUR"},

		// Kraken lists 22 four-character X*/Z* codes and only some are
		// prefixed forms. These are real asset names, so the rule cannot be
		// "strip a leading X or Z" -- that would rewrite them into nonsense.
		{"tokenised gold, not prefixed", "XAUT", "XAUT"},
		{"XION, not prefixed", "XION", "XION"},
		{"ZAMA, not prefixed", "ZAMA", "ZAMA"},
		{"ZETA, not prefixed", "ZETA", "ZETA"},
		{"ZORA, not prefixed", "ZORA", "ZORA"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CanonicalAsset(c.in); got != c.want {
				t.Errorf("CanonicalAsset(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestMarket(t *testing.T) {
	cases := []struct{ base, quote, want string }{
		{"BTC", "USDT", "BTC-USDT"},
		{"BTC", "USD", "BTC-USD"},
		{"ETH", "BTC", "ETH-BTC"},
	}

	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			if got := Market(c.base, c.quote); got != c.want {
				t.Errorf("Market(%q, %q) = %q, want %q", c.base, c.quote, got, c.want)
			}
		})
	}
}

// Four Binance books -- BTCUSDT, BTCUSDC, BTCFDUSD, BTCUSD -- shared one
// market id while the stablecoin collapse was in place, so they overwrote
// each other and the detector compared prices across different assets.
func TestMarketKeepsQuoteAssetsDistinct(t *testing.T) {
	seen := map[string]string{}
	for _, quoteAsset := range []string{"USD", "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI"} {
		market := Market("BTC", quoteAsset)
		if prev, dup := seen[market]; dup {
			t.Errorf("Market(BTC, %q) = %q, colliding with quote asset %q", quoteAsset, market, prev)
		}
		seen[market] = quoteAsset
	}
}

func TestQuoteClass(t *testing.T) {
	t.Run("groups dollar stablecoins", func(t *testing.T) {
		for _, in := range []string{"USD", "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "PYUSD", "RLUSD", "DAI"} {
			if got := QuoteClass(in); got != "USD" {
				t.Errorf("QuoteClass(%q) = %q, want %q", in, got, "USD")
			}
		}
	})

	t.Run("passes through non-dollar quotes", func(t *testing.T) {
		for _, in := range []string{"BTC", "ETH", "EUR", "GBP", "JPY", "XAUT"} {
			if got := QuoteClass(in); got != in {
				t.Errorf("QuoteClass(%q) = %q, want it unchanged", in, got)
			}
		}
	})
}

// The division of labour between the two functions: Market keeps the books
// apart so nothing is silently overwritten, and QuoteClass is the single seam
// where a caller may deliberately decide USDT and USD are comparable.
func TestQuoteClassIsTheOnlyPlaceStablecoinsMeet(t *testing.T) {
	if usdt, usd := Market("BTC", "USDT"), Market("BTC", "USD"); usdt == usd {
		t.Fatalf("Market collapsed USDT and USD into the same id %q", usdt)
	}
	if got, want := QuoteClass("USDT"), QuoteClass("USD"); got != want {
		t.Errorf("QuoteClass(USDT) = %q, QuoteClass(USD) = %q; want them equal", got, want)
	}
}
