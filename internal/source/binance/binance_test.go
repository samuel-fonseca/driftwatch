package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

// --- test helpers ---

const exchangeInfoBody = `{"symbols":[
	{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","isSpotTradingAllowed":true}
]}`

func infoServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("request method = %q, want %q", r.Method, http.MethodGet)
		}
		w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

// testLoader mirrors how New binds tickerLoader: to a poller.HTTP and an
// exchangeInfo URL, yielding the symbols.Loader the poller ultimately calls.
func testLoader(url string) symbols.Loader {
	h := poller.NewHTTP(venue, 10*time.Second)
	return func(ctx context.Context) (symbols.Table, error) {
		return tickerLoader(ctx, h, url)
	}
}

// --- parseTicks tests ---
//
// Note the format difference from Bitfinex throughout: Binance's numeric
// fields are JSON STRINGS ("65432.10000000"), not JSON numbers, and the
// payload is a homogeneous array of objects, not positional arrays -- so
// there's no funding-row-shaped trap here, but there IS a string-parsing
// failure mode Bitfinex's parseTicks never had to handle.
//
// parseTicks does not filter by symbol; the poller's table lookup drops
// unlisted symbols, and those drops are covered in internal/source/poller.

func TestParseTicksReadsRow(t *testing.T) {
	body := `[
		{"symbol":"BTCUSDT","bidPrice":"65432.10000000","bidQty":"3.50000000","askPrice":"65433.00000000","askQty":"2.10000000"}
	]`

	ticks, err := parseTicks([]byte(body))
	if err != nil {
		t.Fatalf("parseTicks() = %v, want nil", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("len(ticks) = %d, want 1", len(ticks))
	}

	got := ticks[0]
	if got.Symbol != "BTCUSDT" {
		t.Errorf("Symbol = %q, want %q", got.Symbol, "BTCUSDT")
	}
	if got.BidPrice != 65432.10 || got.BidSize != 3.5 {
		t.Errorf("bid = %v @ %v, want 65432.10 @ 3.5", got.BidPrice, got.BidSize)
	}
	if got.AskPrice != 65433.00 || got.AskSize != 2.1 {
		t.Errorf("ask = %v @ %v, want 65433.00 @ 2.1", got.AskPrice, got.AskSize)
	}
	if got.ObservedAt.IsZero() {
		t.Error("ObservedAt is zero; the poller would substitute its own fetch time")
	}
}

// Binance's numbers arriving as JSON strings means strconv.ParseFloat can fail
// where a JSON number type assertion never could. "Skip, never guess": don't
// panic, don't treat it as zero, drop the row.
func TestParseTicksMalformedNumberSkipsRow(t *testing.T) {
	cases := []struct{ name, row string }{
		{"bid price", `{"symbol":"BTCUSDT","bidPrice":"not-a-number","bidQty":"3.5","askPrice":"65433.00","askQty":"2.1"}`},
		{"bid size", `{"symbol":"BTCUSDT","bidPrice":"65432.10","bidQty":"","askPrice":"65433.00","askQty":"2.1"}`},
		{"ask price", `{"symbol":"BTCUSDT","bidPrice":"65432.10","bidQty":"3.5","askPrice":"n/a","askQty":"2.1"}`},
		{"ask size", `{"symbol":"BTCUSDT","bidPrice":"65432.10","bidQty":"3.5","askPrice":"65433.00","askQty":"NaN-ish"}`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ticks, err := parseTicks([]byte("[" + c.row + "]"))
			if err != nil {
				t.Fatalf("parseTicks() = %v, want nil", err)
			}
			// The whole row goes, not just the bad side: a half-read book
			// would publish one side of a quote the venue never sent.
			if len(ticks) != 0 {
				t.Errorf("len(ticks) = %d, want 0 (row is unreadable): %+v", len(ticks), ticks)
			}
		})
	}
}

// Only unparseable rows are dropped here. Zero prices and unlisted symbols
// both survive parsing -- the poller decides what to do with them.
func TestParseTicksMixedBatch(t *testing.T) {
	body := `[
		{"symbol":"BTCUSDT","bidPrice":"65432.10","bidQty":"3.5","askPrice":"65433.00","askQty":"2.1"},
		{"symbol":"ETHUSDT","bidPrice":"0.0","bidQty":"0.0","askPrice":"3201.00","askQty":"8.0"},
		{"symbol":"BTCXYZ","bidPrice":"100.0","bidQty":"1.0","askPrice":"101.0","askQty":"1.0"},
		{"symbol":"SOLUSDT","bidPrice":"not-a-number","bidQty":"1.0","askPrice":"150.0","askQty":"1.0"}
	]`

	ticks, err := parseTicks([]byte(body))
	if err != nil {
		t.Fatalf("parseTicks() = %v, want nil", err)
	}
	if len(ticks) != 3 {
		t.Fatalf("len(ticks) = %d, want 3 (only the malformed SOLUSDT row dropped): %+v", len(ticks), ticks)
	}
	for _, tick := range ticks {
		if tick.Symbol == "SOLUSDT" {
			t.Error("the malformed row survived")
		}
	}
}

func TestParseTicksEmptyArray(t *testing.T) {
	ticks, err := parseTicks([]byte(`[]`))
	if err != nil {
		t.Fatalf("parseTicks() = %v, want nil", err)
	}
	if len(ticks) != 0 {
		t.Fatalf("expected 0 ticks for an empty array, got %+v", ticks)
	}
}

func TestParseTicksMalformedJSONIsError(t *testing.T) {
	if _, err := parseTicks([]byte(`not json at all`)); err == nil {
		t.Error("parseTicks() = nil error for a non-JSON body, want an error")
	}
}

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

// --- tickerLoader tests ---

func TestTickerLoaderReturnsTable(t *testing.T) {
	server := infoServer(t, exchangeInfoBody)

	table, err := testLoader(server.URL)(context.Background())
	if err != nil {
		t.Fatalf("tickerLoader() = %v, want nil", err)
	}
	if _, ok := table["BTCUSDT"]; !ok {
		t.Errorf("table missing BTCUSDT: %+v", table)
	}
}

func TestTickerLoaderNonOKStatusIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	if _, err := testLoader(server.URL)(context.Background()); err == nil {
		t.Error("tickerLoader() = nil error for a 429, want an error")
	}
}

func TestTickerLoaderMalformedBodyIsError(t *testing.T) {
	server := infoServer(t, `not json at all`)

	if _, err := testLoader(server.URL)(context.Background()); err == nil {
		t.Error("tickerLoader() = nil error for a non-JSON body, want an error")
	}
}

// --- registry integration ---

func TestRegistryLoadsFromTickerLoader(t *testing.T) {
	server := infoServer(t, exchangeInfoBody)

	r := symbols.NewRegistry(testLoader(server.URL), time.Hour)
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

// Binance-shaped: objects with numeric fields as JSON strings. Emitting
// Bitfinex's positional arrays here made the benchmark time an unmarshal error.
func buildLargeFixture(n int) []byte {
	rows := make([]string, n)
	for i := range n {
		rows[i] = fmt.Sprintf(
			`{"symbol":"BTCUSDT","bidPrice":"%f","bidQty":"3.5","askPrice":"%f","askQty":"2.1"}`,
			65000.0+float64(i%500), 65001.0+float64(i%500),
		)
	}
	return []byte("[" + strings.Join(rows, ",") + "]")
}

func BenchmarkParseTicks(b *testing.B) {
	data := buildLargeFixture(1000)

	b.ReportAllocs()

	for b.Loop() {
		parseTicks(data)
	}
}
