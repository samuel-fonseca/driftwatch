package normalize

import "testing"

func TestCanonicalAssetRewritesKnownCodes(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"kraken bitcoin shorthand", "XBT", "BTC"},
		{"kraken prefixed bitcoin", "XXBT", "BTC"},
		{"kraken prefixed dollar", "ZUSD", "USD"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CanonicalAsset(c.in); got != c.want {
				t.Errorf("CanonicalAsset(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestCanonicalAssetPassesThroughUnknownCodes(t *testing.T) {
	for _, in := range []string{"BTC", "ETH", "USDT", "USDC", "SOL", "EUR"} {
		if got := CanonicalAsset(in); got != in {
			t.Errorf("CanonicalAsset(%q) = %q, want it unchanged", in, got)
		}
	}
}

// Kraken lists 22 four-character X*/Z* codes; only some are prefixed forms.
// These are real asset names, so "strip a leading X or Z" is wrong.
func TestCanonicalAssetDoesNotStripLeadingXZ(t *testing.T) {
	for _, in := range []string{"XAUT", "XION", "ZAMA", "ZBCN", "ZETA", "ZEUS", "ZORA"} {
		if got := CanonicalAsset(in); got != in {
			t.Errorf("CanonicalAsset(%q) = %q, want it unchanged", in, got)
		}
	}
}

func TestMarketFormatsCanonicalPair(t *testing.T) {
	cases := []struct {
		base, quote, want string
	}{
		{"BTC", "USDT", "BTC-USDT"},
		{"BTC", "USD", "BTC-USD"},
		{"ETH", "BTC", "ETH-BTC"},
	}

	for _, c := range cases {
		if got := Market(c.base, c.quote); got != c.want {
			t.Errorf("Market(%q, %q) = %q, want %q", c.base, c.quote, got, c.want)
		}
	}
}

// Four Binance books (BTCUSDT/USDC/FDUSD/USD) shared one market id while the
// stablecoin collapse was in place, so they overwrote each other.
func TestMarketKeepsQuoteAssetsDistinct(t *testing.T) {
	seen := map[string]string{}
	for _, quoteAsset := range []string{"USD", "USDT", "USDC", "FDUSD", "TUSD"} {
		market := Market("BTC", quoteAsset)
		if prev, dup := seen[market]; dup {
			t.Errorf("Market(BTC, %q) = %q, which collides with quote asset %q", quoteAsset, market, prev)
		}
		seen[market] = quoteAsset
	}
}

func TestQuoteClassGroupsDollarStablecoins(t *testing.T) {
	for _, in := range []string{"USD", "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "PYUSD", "RLUSD", "DAI"} {
		if got := QuoteClass(in); got != "USD" {
			t.Errorf("QuoteClass(%q) = %q, want %q", in, got, "USD")
		}
	}
}

func TestQuoteClassPassesThroughNonDollarQuotes(t *testing.T) {
	for _, in := range []string{"BTC", "ETH", "EUR", "GBP", "JPY", "XAUT"} {
		if got := QuoteClass(in); got != in {
			t.Errorf("QuoteClass(%q) = %q, want it unchanged", in, got)
		}
	}
}

// Market keeps the books apart; QuoteClass is the one seam where they meet.
func TestQuoteClassIsTheOnlyPlaceStablecoinsMeet(t *testing.T) {
	usdt := Market("BTC", "USDT")
	usd := Market("BTC", "USD")

	if usdt == usd {
		t.Fatalf("Market collapsed %q and %q into the same id", "USDT", "USD")
	}
	if got, want := QuoteClass("USDT"), QuoteClass("USD"); got != want {
		t.Errorf("QuoteClass(USDT) = %q, QuoteClass(USD) = %q; want them equal so the class can cross them", got, want)
	}
}
