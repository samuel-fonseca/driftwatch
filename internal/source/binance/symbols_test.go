package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

// --- parseTickers tests ---

func TestParseTickersBuildsInstruments(t *testing.T) {
	body := `{"symbols":[
		{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","isSpotTradingAllowed":true},
		{"symbol":"ETHBTC","status":"TRADING","baseAsset":"ETH","quoteAsset":"BTC","isSpotTradingAllowed":true}
	]}`

	table, err := parseTickers([]byte(body))
	if err != nil {
		t.Fatalf("parseTickers() = %v, want nil", err)
	}
	if len(table) != 2 {
		t.Fatalf("len(table) = %d, want 2: %+v", len(table), table)
	}

	want := symbols.Instrument{Symbol: "BTCUSDT", Base: "BTC", Quote: "USDT", Market: "BTC-USDT"}
	if got := table["BTCUSDT"]; got != want {
		t.Errorf("table[\"BTCUSDT\"] = %+v, want %+v", got, want)
	}
	// The venue-native symbol is the key, and Market is the canonical pair --
	// the whole point of the normalization.
	if got := table["ETHBTC"].Market; got != "ETH-BTC" {
		t.Errorf("table[\"ETHBTC\"].Market = %q, want %q", got, "ETH-BTC")
	}
}

func TestParseTickersEmptySymbolsIsEmptyTable(t *testing.T) {
	table, err := parseTickers([]byte(`{"symbols":[]}`))
	if err != nil {
		t.Fatalf("parseTickers() = %v, want nil", err)
	}
	if table == nil {
		t.Fatal("parseTickers() returned a nil table, want a non-nil empty one")
	}
	if len(table) != 0 {
		t.Errorf("len(table) = %d, want 0", len(table))
	}
}

func TestParseTickersMalformedJSONIsError(t *testing.T) {
	if _, err := parseTickers([]byte(`{"symbols":[`)); err == nil {
		t.Error("parseTickers() = nil error for truncated JSON, want an error")
	}
}

func TestParseTickersIgnoresUnknownFields(t *testing.T) {
	body := `{"timezone":"UTC","serverTime":1700000000000,"symbols":[
		{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","isSpotTradingAllowed":true,"filters":[{"filterType":"PRICE_FILTER"}]}
	]}`

	table, err := parseTickers([]byte(body))
	if err != nil {
		t.Fatalf("parseTickers() = %v, want nil", err)
	}
	if _, ok := table["BTCUSDT"]; !ok {
		t.Error("BTCUSDT missing; extra exchangeInfo fields should be ignored, not fatal")
	}
}

// --- filter tests ---
//
// The intent is "list only symbols that are TRADING *and* spot-tradable".
// Each case below names which half of that rule it exercises.

func TestParseTickersKeepsTradingSpotSymbol(t *testing.T) {
	body := `{"symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","isSpotTradingAllowed":true}]}`

	table, err := parseTickers([]byte(body))
	if err != nil {
		t.Fatalf("parseTickers() = %v, want nil", err)
	}
	if _, ok := table["BTCUSDT"]; !ok {
		t.Error("BTCUSDT was filtered out, want it kept (TRADING and spot-allowed)")
	}
}

func TestParseTickersDropsHaltedNonSpotSymbol(t *testing.T) {
	body := `{"symbols":[{"symbol":"DEADUSDT","status":"BREAK","baseAsset":"DEAD","quoteAsset":"USDT","isSpotTradingAllowed":false}]}`

	table, err := parseTickers([]byte(body))
	if err != nil {
		t.Fatalf("parseTickers() = %v, want nil", err)
	}
	if _, ok := table["DEADUSDT"]; ok {
		t.Error("DEADUSDT was kept, want it dropped (neither TRADING nor spot-allowed)")
	}
}

func TestParseTickersDropsHaltedSpotSymbol(t *testing.T) {
	// Halted mid-session but still flagged spot-tradable: quoting it would
	// publish prices for a market that isn't trading.
	body := `{"symbols":[{"symbol":"HALTUSDT","status":"BREAK","baseAsset":"HALT","quoteAsset":"USDT","isSpotTradingAllowed":true}]}`

	table, err := parseTickers([]byte(body))
	if err != nil {
		t.Fatalf("parseTickers() = %v, want nil", err)
	}
	if _, ok := table["HALTUSDT"]; ok {
		t.Error("HALTUSDT (status BREAK) was kept, want it dropped -- status must be TRADING")
	}
}

func TestParseTickersDropsTradingNonSpotSymbol(t *testing.T) {
	// TRADING but margin/futures-only: not a spot market, so not ours.
	body := `{"symbols":[{"symbol":"MARGINUSDT","status":"TRADING","baseAsset":"MARGIN","quoteAsset":"USDT","isSpotTradingAllowed":false}]}`

	table, err := parseTickers([]byte(body))
	if err != nil {
		t.Fatalf("parseTickers() = %v, want nil", err)
	}
	if _, ok := table["MARGINUSDT"]; ok {
		t.Error("MARGINUSDT (isSpotTradingAllowed false) was kept, want it dropped -- spot only")
	}
}

// --- fetchTickers / tickerLoader tests ---

const exchangeInfoBody = `{"symbols":[
	{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","isSpotTradingAllowed":true}
]}`

// testAdapter points an adapter's symbol-table URL at a stub server, the
// same way the decode tests point a.baseURL at one.
func testAdapter(url string) *Adapter {
	a := New()
	a.exchangeInfoURL = url
	return a
}

func TestTickerLoaderReturnsTable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("request method = %q, want %q", r.Method, http.MethodGet)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(exchangeInfoBody))
	}))
	defer server.Close()

	table, err := testAdapter(server.URL).tickerLoader(context.Background())
	if err != nil {
		t.Fatalf("tickerLoader() = %v, want nil", err)
	}
	if _, ok := table["BTCUSDT"]; !ok {
		t.Errorf("table missing BTCUSDT: %+v", table)
	}
}

// TickerLoader must stay assignable to symbols.Loader -- this is what makes
// symbols.NewRegistry(a.tickerLoader, ...) compile in New().
func TestTickerLoaderSatisfiesLoader(t *testing.T) {
	var _ symbols.Loader = New().tickerLoader
}

func TestTickerLoaderNonOKStatusIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	if _, err := testAdapter(server.URL).tickerLoader(context.Background()); err == nil {
		t.Error("tickerLoader() = nil error for a 429, want an error")
	}
}

func TestTickerLoaderMalformedBodyIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not json at all`))
	}))
	defer server.Close()

	if _, err := testAdapter(server.URL).tickerLoader(context.Background()); err == nil {
		t.Error("tickerLoader() = nil error for a non-JSON body, want an error")
	}
}

func TestTickerLoaderRespectsCancelledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(exchangeInfoBody))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := testAdapter(server.URL).tickerLoader(ctx); err == nil {
		t.Error("tickerLoader() = nil error with a cancelled context, want an error")
	}
}

func TestTickerLoaderUnreachableHostIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := server.URL
	server.Close() // nothing is listening now

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := testAdapter(url).tickerLoader(ctx); err == nil {
		t.Error("tickerLoader() = nil error against a closed server, want an error")
	}
}

// --- integration with the registry ---

func TestRegistryLoadsFromTickerLoader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(exchangeInfoBody))
	}))
	defer server.Close()

	loader := func(ctx context.Context) (symbols.Table, error) {
		return testAdapter(server.URL).tickerLoader(ctx)
	}
	r := symbols.NewRegistry(loader, time.Hour)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}

	inst, ok := r.Lookup("BTCUSDT")
	if !ok {
		t.Fatal("Lookup(\"BTCUSDT\") = false, want true")
	}
	if inst.Market != "BTC-USDT" {
		t.Errorf("Market = %q, want %q", inst.Market, "BTC-USDT")
	}
}
