package quote

import "testing"

func TestKey(t *testing.T) {
	q := Quote{
		Venue:     "binance",
		Market:    "BTC-USD",
		Selection: "bid",
	}

	expected := "binance|BTC-USD|bid"

	if q.Key() != expected {
		t.Errorf("Expected %s, got %s", expected, q.Key())
	}
}

func TestMarketKey(t *testing.T) {
	q := Quote{
		Venue:     "binance",
		Market:    "BTC-USD",
		Selection: "bid",
	}

	expected := "BTC-USD|bid"

	if q.MarketKey() != expected {
		t.Errorf("Expected %s, got %s", expected, q.MarketKey())
	}
}

func TestFingerprint(t *testing.T) {
	q := Quote{
		Venue:     "bitfinex",
		Market:    "tBTCUSD",
		Selection: "ask",
		Price:     66703.00,
		Size:      3.59223206,
	}

	expected := uint64(0xdd1d40a794458670)

	if q.Fingerprint() != expected {
		t.Errorf("Expected %x, got %x", expected, q.Fingerprint())
	}
}
