package quote

import "testing"

func TestKeys(t *testing.T) {
	q := Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid"}

	cases := []struct {
		name string
		got  string
		want string
	}{
		// Key identifies one book on one venue -- it is what the buffer
		// coalesces on and what dedupe tracks.
		{"Key", q.Key(), "binance|BTC-USD|bid"},
		// MarketKey drops the venue, naming the same book across venues.
		{"MarketKey", q.MarketKey(), "BTC-USD|bid"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
			}
		})
	}
}

// Key must separate books that Market alone would merge: one venue quoting
// BTCUSD and BTCUSDT is two books, and both sides of each is two more.
func TestKeyDistinguishesVenueMarketAndSelection(t *testing.T) {
	quotes := []Quote{
		{Venue: "binance", Market: "BTC-USD", Selection: "bid"},
		{Venue: "binance", Market: "BTC-USD", Selection: "ask"},
		{Venue: "binance", Market: "BTC-USDT", Selection: "bid"},
		{Venue: "kraken", Market: "BTC-USD", Selection: "bid"},
	}

	seen := make(map[string]Quote, len(quotes))
	for _, q := range quotes {
		if prev, dup := seen[q.Key()]; dup {
			t.Errorf("Key() collision %q: %+v and %+v", q.Key(), prev, q)
		}
		seen[q.Key()] = q
	}
}

// Fingerprint answers "has this book moved?", so it must depend on price and
// size and on nothing else. Asserting the properties rather than a hash
// constant keeps the test from pinning FNV's internals: swapping the hash
// function is not a behaviour change, but reordering the fields into it is.
func TestFingerprintDependsOnPriceAndSize(t *testing.T) {
	base := Quote{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1}

	cases := []struct {
		name  string
		quote Quote
		want  bool // want the fingerprint to equal base's
	}{
		{"identical", base, true},
		{"price differs", Quote{Price: 101, Size: 1}, false},
		{"size differs", Quote{Price: 100, Size: 2}, false},
		{"both differ", Quote{Price: 101, Size: 2}, false},
		// The identity fields live in Key, not in the fingerprint. Mixing
		// them in would make every venue's first quote look "changed"
		// against another venue's, which is not what dedupe asks.
		{"venue differs", Quote{Venue: "kraken", Price: 100, Size: 1}, true},
		{"market differs", Quote{Market: "ETH-USD", Price: 100, Size: 1}, true},
		{"selection differs", Quote{Selection: "ask", Price: 100, Size: 1}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.quote.Fingerprint() == base.Fingerprint(); got != c.want {
				t.Errorf("Fingerprint() == base: got %v, want %v", got, c.want)
			}
		})
	}
}

// A price change that swaps into the size field must not cancel itself out.
// Hashing "price|size" without a separator would make 1.0/23.0 and 1.02/3.0
// collide; this pins that the two fields stay distinguishable.
func TestFingerprintDoesNotConflatePriceAndSize(t *testing.T) {
	a := Quote{Price: 100, Size: 1}
	b := Quote{Price: 1, Size: 100}

	if a.Fingerprint() == b.Fingerprint() {
		t.Errorf("price/size swap produced the same fingerprint %x", a.Fingerprint())
	}
}

func TestFingerprintIsStableAcrossCalls(t *testing.T) {
	q := Quote{Venue: "bitfinex", Market: "BTC-USD", Selection: "ask", Price: 66703, Size: 3.59223206}

	first := q.Fingerprint()
	for range 10 {
		if got := q.Fingerprint(); got != first {
			t.Fatalf("Fingerprint() = %x on a repeat call, want the stable %x", got, first)
		}
	}
}
