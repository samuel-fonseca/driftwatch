package binance

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/source"
	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
	"github.com/samuel-fonseca/driftwatch/internal/source/sourcetest"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

// --- test helpers ---

const exchangeInfoBody = `{"symbols":[
	{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","isSpotTradingAllowed":true}
]}`

// testLoader mirrors how New binds tickerLoader: to a poller.HTTP and an
// exchangeInfo URL, yielding the symbols.Loader the poller ultimately calls.
func testLoader(url string) symbols.Loader {
	h := poller.NewHTTP(venue, 10*time.Second)
	return func(ctx context.Context) (symbols.Table, error) {
		return tickerLoader(ctx, h, url)
	}
}

// --- parseTicks ---
//
// Binance's numeric fields are JSON STRINGS ("65432.10000000"), not JSON
// numbers, and the payload is a homogeneous array of objects rather than
// positional arrays. So there is no short-row trap here as there is for
// Bitfinex, but there IS a string-parsing failure mode the others never had.
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

// Numbers arriving as JSON strings mean strconv.ParseFloat can fail where a
// JSON number type assertion never could. Skip, never guess: don't panic,
// don't treat it as zero, drop the row -- a half-read book would publish one
// side of a quote the venue never sent.
func TestParseTicksMalformedNumberSkipsWholeRow(t *testing.T) {
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
			if len(ticks) != 0 {
				t.Errorf("len(ticks) = %d, want 0 (the row is unreadable): %+v", len(ticks), ticks)
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
		t.Fatalf("len(ticks) = %d, want 0: %+v", len(ticks), ticks)
	}
}

func TestParseTicksMalformedJSONIsError(t *testing.T) {
	if _, err := parseTicks([]byte(`not json at all`)); err == nil {
		t.Error("parseTicks() = nil error for a non-JSON body, want an error")
	}
}

// --- parseTickers ---

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
	// The venue-native symbol is the key and Market is the canonical pair --
	// the whole point of the normalisation.
	if got := table["ETHBTC"].Market; got != "ETH-BTC" {
		t.Errorf("table[\"ETHBTC\"].Market = %q, want %q", got, "ETH-BTC")
	}
}

// The rule is "TRADING *and* spot-tradable"; each case names the half it
// exercises, so a rule that decayed into a single check still fails here.
func TestParseTickersFiltersToTradableSpotSymbols(t *testing.T) {
	cases := []struct {
		name     string
		status   string
		spot     bool
		wantKept bool
		why      string
	}{
		{"trading and spot", "TRADING", true, true, "the only combination we quote"},
		{"halted but spot", "BREAK", true, false, "quoting it would publish prices for a market that is not trading"},
		{"trading but not spot", "TRADING", false, false, "margin- or futures-only is not a spot market"},
		{"halted and not spot", "BREAK", false, false, "neither half of the rule holds"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := fmt.Sprintf(
				`{"symbols":[{"symbol":"XUSDT","status":%q,"baseAsset":"X","quoteAsset":"USDT","isSpotTradingAllowed":%t}]}`,
				c.status, c.spot,
			)

			table, err := parseTickers([]byte(body))
			if err != nil {
				t.Fatalf("parseTickers() = %v, want nil", err)
			}
			if _, kept := table["XUSDT"]; kept != c.wantKept {
				t.Errorf("kept = %v, want %v -- %s", kept, c.wantKept, c.why)
			}
		})
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

// exchangeInfo carries far more than the five fields decoded here, and
// Binance adds to it over time.
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

// --- tickerLoader ---

func TestTickerLoaderReturnsTable(t *testing.T) {
	server := sourcetest.Server(t, exchangeInfoBody)

	table, err := testLoader(server.URL)(context.Background())
	if err != nil {
		t.Fatalf("tickerLoader() = %v, want nil", err)
	}
	if _, ok := table["BTCUSDT"]; !ok {
		t.Errorf("table missing BTCUSDT: %+v", table)
	}
}

func TestTickerLoaderErrors(t *testing.T) {
	cases := []struct {
		name  string
		serve func(t *testing.T) string
		why   string
	}{
		{
			name:  "rate limited",
			serve: func(t *testing.T) string { return sourcetest.StatusServer(t, http.StatusTooManyRequests).URL },
			why:   "a 429 must back the poller off, not empty the table",
		},
		{
			name:  "server error",
			serve: func(t *testing.T) string { return sourcetest.StatusServer(t, http.StatusInternalServerError).URL },
			why:   "an outage must not look like a venue with no symbols",
		},
		{
			name:  "unparseable body",
			serve: func(t *testing.T) string { return sourcetest.Server(t, `not json at all`).URL },
			why:   "a 200 of garbage is schema drift and must be reported",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := testLoader(c.serve(t))(context.Background()); err == nil {
				t.Errorf("tickerLoader() = nil error, want one -- %s", c.why)
			}
		})
	}
}

// --- adapter construction ---

var _ source.Source = (*Adapter)(nil)

func TestNewUsesVenueName(t *testing.T) {
	if got := New(Config{}).Name(); got != venue {
		t.Errorf("Name() = %q, want %q", got, venue)
	}
}

func TestConfigWithDefaults(t *testing.T) {
	got := Config{}.withDefaults()

	if got.TickersURL != defaultTickersURL {
		t.Errorf("TickersURL = %q, want %q", got.TickersURL, defaultTickersURL)
	}
	if got.ExchangeInfoURL != defaultExchangeInfoURL {
		t.Errorf("ExchangeInfoURL = %q, want %q", got.ExchangeInfoURL, defaultExchangeInfoURL)
	}
	// The two URLs address different endpoints; wiring one into the other
	// would leave the poller parsing tickers as exchangeInfo forever.
	if got.TickersURL == got.ExchangeInfoURL {
		t.Error("TickersURL and ExchangeInfoURL resolved to the same endpoint")
	}
	if got.Tuning.HTTPTimeout == 0 {
		t.Error("Tuning was not defaulted")
	}
}

func TestConfigWithDefaultsKeepsExplicitValues(t *testing.T) {
	cfg := Config{
		TickersURL:      "http://tickers.invalid",
		ExchangeInfoURL: "http://info.invalid",
		Tuning:          poller.Tuning{HTTPTimeout: time.Second},
	}

	got := cfg.withDefaults()
	if got.TickersURL != cfg.TickersURL {
		t.Errorf("TickersURL = %q, want the explicit %q", got.TickersURL, cfg.TickersURL)
	}
	if got.ExchangeInfoURL != cfg.ExchangeInfoURL {
		t.Errorf("ExchangeInfoURL = %q, want the explicit %q", got.ExchangeInfoURL, cfg.ExchangeInfoURL)
	}
	if got.Tuning.HTTPTimeout != time.Second {
		t.Errorf("Tuning.HTTPTimeout = %v, want the explicit 1s", got.Tuning.HTTPTimeout)
	}
}

// --- adapter end to end ---

// The one test that proves the whole venue adapter works: the loader closure
// New builds is bound to the right URL, the symbol table it returns resolves
// the ticker rows, and the poller turns them into canonical quotes. A
// withDefaults test can only check the URLs are distinct strings -- this
// checks they are wired to the endpoints that actually serve them.
func TestAdapterRunProducesQuotes(t *testing.T) {
	tickers := sourcetest.Server(t, `[{"symbol":"BTCUSDT","bidPrice":"65432.10","bidQty":"3.5","askPrice":"65433.00","askQty":"2.1"}]`)
	info := sourcetest.Server(t, exchangeInfoBody)

	adapter := New(Config{
		TickersURL:      tickers.URL,
		ExchangeInfoURL: info.URL,
		Tuning: poller.Tuning{
			PollInterval:   10 * time.Millisecond,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			SymbolsRefresh: time.Hour,
		},
	})

	if adapter.Name() != venue {
		t.Errorf("Name() = %q, want %q", adapter.Name(), venue)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan quote.Quote, 16)
	errCh := make(chan error, 1)
	go func() { errCh <- adapter.Run(ctx, out) }()

	select {
	case q := <-out:
		if q.Venue != venue {
			t.Errorf("Venue = %q, want %q", q.Venue, venue)
		}
		if q.Market != "BTC-USDT" {
			t.Errorf("Market = %q, want %q -- the symbol table did not resolve the ticker row", q.Market, "BTC-USDT")
		}
		if q.Selection != "bid" && q.Selection != "ask" {
			t.Errorf("Selection = %q, want bid or ask", q.Selection)
		}
		if q.Price <= 0 {
			t.Errorf("Price = %v, want a real price", q.Price)
		}
	case err := <-errCh:
		t.Fatalf("Run returned %v before producing a quote", err)
	case <-time.After(5 * time.Second):
		t.Fatal("no quote arrived from the adapter")
	}
}

// --- registry integration ---

func TestRegistryLoadsFromTickerLoader(t *testing.T) {
	server := sourcetest.Server(t, exchangeInfoBody)

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
