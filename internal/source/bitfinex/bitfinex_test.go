package bitfinex

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

// A trimmed capture of the real conf response. Sections arrive in the order
// they were requested -- pairs, currencies, aliases -- which parseConf relies on.
const confBody = `[
	["BTCUSD","BTCUST","ADAUST","AAVE:USD","UDCUSD","TESTBTC:TESTUSD","XAUT:USD"],
	["BTC","USD","UST","ADA","AAVE","UDC","TESTBTC","TESTUSD","XAUT"],
	[["UST","USDt"],["UDC","USDC"],["TESTBTC","BTC"],["XAUT","XAUt"]]
]`

// testLoader mirrors how New binds tickerLoader: to a poller.HTTP and a conf
// URL, yielding the symbols.Loader the poller ultimately calls.
func testLoader(url string) symbols.Loader {
	h := poller.NewHTTP(venue, 10*time.Second)
	return func(ctx context.Context) (symbols.Table, error) {
		return tickerLoader(ctx, h, url)
	}
}

func confServer(t *testing.T, body string) *httptest.Server {
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

// --- parseTicks tests ---
//
// parseTicks reads Bitfinex's positional ticker rows and nothing more. It does
// not filter by symbol: a funding row parses into a Tick just as a trading pair
// does, and it is the poller's table lookup that drops it. Those drops are
// covered in internal/source/poller.

func TestParseTicksReadsRow(t *testing.T) {
	body := `[
		["tBTCUSD", 65432.0, 3.5, 65433.0, 2.1, 120.5, 0.0019, 65433.0, 15234.7, 66000.0, 64000.0]
	]`

	ticks, err := parseTicks([]byte(body))
	if err != nil {
		t.Fatalf("parseTicks() = %v, want nil", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("len(ticks) = %d, want 1", len(ticks))
	}

	got := ticks[0]
	if got.Symbol != "tBTCUSD" {
		t.Errorf("Symbol = %q, want %q", got.Symbol, "tBTCUSD")
	}
	if got.BidPrice != 65432.0 || got.BidSize != 3.5 {
		t.Errorf("bid = %v @ %v, want 65432 @ 3.5", got.BidPrice, got.BidSize)
	}
	if got.AskPrice != 65433.0 || got.AskSize != 2.1 {
		t.Errorf("ask = %v @ %v, want 65433 @ 2.1", got.AskPrice, got.AskSize)
	}
	if got.ObservedAt.IsZero() {
		t.Error("ObservedAt is zero; the poller would substitute its own fetch time")
	}
}

// Bitfinex writes newer pairs with a colon between the halves. The separator is
// part of the symbol and must survive into the table key.
func TestParseTicksColonSymbolPreserved(t *testing.T) {
	body := `[
		["tDOGE:USD", 0.24, 5000.0, 0.241, 4800.0, 0.01, 0.04, 0.241, 900000.0, 0.25, 0.23]
	]`

	ticks, err := parseTicks([]byte(body))
	if err != nil {
		t.Fatalf("parseTicks() = %v, want nil", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("len(ticks) = %d, want 1", len(ticks))
	}
	if ticks[0].Symbol != "tDOGE:USD" {
		t.Errorf("Symbol = %q, want %q", ticks[0].Symbol, "tDOGE:USD")
	}
}

// Positional rows carry no field names, so a short row is unreadable rather
// than partially readable -- indexing it would yield a plausible wrong price.
func TestParseTicksSkipsShortRows(t *testing.T) {
	body := `[
		["tBTCUSD", 65432.0, 3.5],
		["tETHUSD", 3456.0, 8.2, 3457.0, 7.9]
	]`

	ticks, err := parseTicks([]byte(body))
	if err != nil {
		t.Fatalf("parseTicks() = %v, want nil", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("len(ticks) = %d, want 1 (the 3-element row skipped): %+v", len(ticks), ticks)
	}
	if ticks[0].Symbol != "tETHUSD" {
		t.Errorf("survivor Symbol = %q, want %q", ticks[0].Symbol, "tETHUSD")
	}
}

func TestParseTicksSkipsNonNumericFields(t *testing.T) {
	cases := []struct{ name, row string }{
		{"symbol not a string", `[65432.0, 65432.0, 3.5, 65433.0, 2.1]`},
		{"bid price a string", `["tBTCUSD", "65432.0", 3.5, 65433.0, 2.1]`},
		{"bid size null", `["tBTCUSD", 65432.0, null, 65433.0, 2.1]`},
		{"ask price a string", `["tBTCUSD", 65432.0, 3.5, "65433.0", 2.1]`},
		{"ask size an object", `["tBTCUSD", 65432.0, 3.5, 65433.0, {}]`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ticks, err := parseTicks([]byte("[" + c.row + "]"))
			if err != nil {
				t.Fatalf("parseTicks() = %v, want nil", err)
			}
			if len(ticks) != 0 {
				t.Errorf("len(ticks) = %d, want 0 (row is unreadable): %+v", len(ticks), ticks)
			}
		})
	}
}

func TestParseTicksMalformedJSONIsError(t *testing.T) {
	if _, err := parseTicks([]byte(`not json at all`)); err == nil {
		t.Error("parseTicks() = nil error for a non-JSON body, want an error")
	}
}

// --- parseConf tests ---

func TestParseConfReadsAllThreeSections(t *testing.T) {
	c, err := parseConf([]byte(confBody))
	if err != nil {
		t.Fatalf("parseConf() = %v, want nil", err)
	}
	if len(c.pairs) != 7 {
		t.Errorf("len(pairs) = %d, want 7", len(c.pairs))
	}
	if len(c.currencies) != 9 {
		t.Errorf("len(currencies) = %d, want 9", len(c.currencies))
	}
	if got, want := c.aliases["UST"], "USDt"; got != want {
		t.Errorf("aliases[\"UST\"] = %q, want %q", got, want)
	}
}

// Bitfinex answers an unknown conf key with HTTP 200 and [null], which decodes
// to a nil slice without erroring. Unchecked, that empties the table silently.
func TestParseConfRejectsNullSection(t *testing.T) {
	cases := []struct {
		name, body string
	}{
		{"all sections null", `[null,null,null]`},
		{"pairs null", `[null,["BTC","USD"],[["UST","USDt"]]]`},
		{"currencies null", `[["BTCUSD"],null,[["UST","USDt"]]]`},
		{"aliases null", `[["BTCUSD"],["BTC","USD"],null]`},
		{"empty arrays", `[[],[],[]]`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := parseConf([]byte(c.body)); err == nil {
				t.Errorf("parseConf(%s) = nil error, want an error", c.body)
			}
		})
	}
}

// The literal response a bad conf key returns.
func TestParseConfRejectsSingleNull(t *testing.T) {
	if _, err := parseConf([]byte(`[null]`)); err == nil {
		t.Error("parseConf(`[null]`) = nil error, want an error")
	}
}

func TestParseConfWrongSectionCountIsError(t *testing.T) {
	if _, err := parseConf([]byte(`[["BTCUSD"],["BTC","USD"]]`)); err == nil {
		t.Error("parseConf() = nil error for 2 sections, want an error")
	}
}

func TestParseConfMalformedJSONIsError(t *testing.T) {
	if _, err := parseConf([]byte(`not json at all`)); err == nil {
		t.Error("parseConf() = nil error for a non-JSON body, want an error")
	}
}

func TestParseConfSkipsMalformedAliasRows(t *testing.T) {
	body := `[
		["BTCUSD"],
		["BTC","USD"],
		[["UST","USDt"],["JUSTONE"],["TOO","MANY","FIELDS"]]
	]`

	c, err := parseConf([]byte(body))
	if err != nil {
		t.Fatalf("parseConf() = %v, want nil", err)
	}
	if len(c.aliases) != 1 {
		t.Errorf("len(aliases) = %d, want 1 (the two malformed rows skipped): %+v", len(c.aliases), c.aliases)
	}
}

// --- splitPair tests ---

func knownCurrencies(codes ...string) map[string]struct{} {
	known := make(map[string]struct{}, len(codes))
	for _, c := range codes {
		known[c] = struct{}{}
	}
	return known
}

// Requiring both halves to be listed currencies makes this exact, not a guess:
// all 98 concatenated pairs in the live list split with zero ambiguity.
func TestSplitPairConcatenated(t *testing.T) {
	known := knownCurrencies("BTC", "USD", "UST", "ADA", "AAVE", "UDC")

	cases := []struct{ pair, base, quote string }{
		{"BTCUSD", "BTC", "USD"},
		{"BTCUST", "BTC", "UST"},
		{"ADAUST", "ADA", "UST"},
		{"UDCUSD", "UDC", "USD"},
	}

	for _, c := range cases {
		t.Run(c.pair, func(t *testing.T) {
			base, quoteAsset, ok := splitPair(c.pair, known)
			if !ok {
				t.Fatalf("splitPair(%q) = false, want true", c.pair)
			}
			if base != c.base || quoteAsset != c.quote {
				t.Errorf("splitPair(%q) = %q, %q; want %q, %q", c.pair, base, quoteAsset, c.base, c.quote)
			}
		})
	}
}

func TestSplitPairColonDelimited(t *testing.T) {
	known := knownCurrencies("AAVE", "USD")

	base, quoteAsset, ok := splitPair("AAVE:USD", known)
	if !ok {
		t.Fatal("splitPair(\"AAVE:USD\") = false, want true")
	}
	if base != "AAVE" || quoteAsset != "USD" {
		t.Errorf("splitPair(\"AAVE:USD\") = %q, %q; want %q, %q", base, quoteAsset, "AAVE", "USD")
	}
}

func TestSplitPairUnknownHalvesAreSkipped(t *testing.T) {
	known := knownCurrencies("BTC", "USD")

	for _, pair := range []string{"BTCXYZ", "XYZUSD", "NONSENSE"} {
		if base, quoteAsset, ok := splitPair(pair, known); ok {
			t.Errorf("splitPair(%q) = %q, %q, true; want ok == false", pair, base, quoteAsset)
		}
	}
}

// --- buildTable tests ---

func buildTestTable(t *testing.T) symbols.Table {
	t.Helper()
	c, err := parseConf([]byte(confBody))
	if err != nil {
		t.Fatalf("parseConf() = %v, want nil", err)
	}
	return buildTable(c)
}

func TestBuildTableKeysCarryTPrefix(t *testing.T) {
	table := buildTestTable(t)

	// Ticker rows arrive as tBTCUSD; the conf pair list says BTCUSD.
	if _, ok := table["tBTCUSD"]; !ok {
		t.Errorf("table missing key %q: %+v", "tBTCUSD", table)
	}
	if _, ok := table["BTCUSD"]; ok {
		t.Error("table has an unprefixed key BTCUSD; ticker symbols will never match")
	}
}

func TestBuildTableAppliesAliases(t *testing.T) {
	table := buildTestTable(t)

	cases := []struct{ symbol, market string }{
		{"tBTCUST", "BTC-USDT"},   // UST -> USDt -> USDT, meeting Binance's BTCUSDT
		{"tUDCUSD", "USDC-USD"},   // UDC -> USDC
		{"tXAUT:USD", "XAUT-USD"}, // display casing must not leak into the id
		{"tBTCUSD", "BTC-USD"},    // no alias
	}

	for _, c := range cases {
		t.Run(c.symbol, func(t *testing.T) {
			inst, ok := table[c.symbol]
			if !ok {
				t.Fatalf("table missing %q: %+v", c.symbol, table)
			}
			if inst.Market != c.market {
				t.Errorf("table[%q].Market = %q, want %q", c.symbol, inst.Market, c.market)
			}
		})
	}
}

// Bitfinex lists 42 live paper-trading symbols, and its own alias map contains
// TESTBTC -> BTC. Filtering must happen before aliasing or fake prices land on
// the real BTC-USD market.
func TestBuildTableDropsPaperTradingPairs(t *testing.T) {
	table := buildTestTable(t)

	if inst, ok := table["tTESTBTC:TESTUSD"]; ok {
		t.Errorf("paper-trading pair survived as %+v, want it dropped", inst)
	}
	for symbol, inst := range table {
		if inst.Market == "BTC-USD" && symbol != "tBTCUSD" {
			t.Errorf("symbol %q resolved to the real market %q", symbol, inst.Market)
		}
	}
}

// Two symbols on one venue resolving to one market is the bug the un-collapse
// exists to prevent -- tBTCUSD and tBTCUST are the pair that used to collide.
func TestBuildTableHasNoMarketCollisions(t *testing.T) {
	table := buildTestTable(t)

	seen := make(map[string]string, len(table))
	for symbol, inst := range table {
		if prev, dup := seen[inst.Market]; dup {
			t.Errorf("market %q claimed by both %q and %q", inst.Market, prev, symbol)
		}
		seen[inst.Market] = symbol
	}
}

func TestBuildTableSkipsUnsplittablePairs(t *testing.T) {
	c, err := parseConf([]byte(`[
		["BTCUSD","MYSTERYPAIR"],
		["BTC","USD"],
		[["UST","USDt"]]
	]`))
	if err != nil {
		t.Fatalf("parseConf() = %v, want nil", err)
	}

	table := buildTable(c)
	if len(table) != 1 {
		t.Fatalf("len(table) = %d, want 1 (the unsplittable pair skipped): %+v", len(table), table)
	}
	if _, ok := table["tBTCUSD"]; !ok {
		t.Errorf("table missing tBTCUSD: %+v", table)
	}
}

// --- tickerLoader tests ---

func TestTickerLoaderReturnsTable(t *testing.T) {
	server := confServer(t, confBody)

	table, err := testLoader(server.URL)(context.Background())
	if err != nil {
		t.Fatalf("tickerLoader() = %v, want nil", err)
	}
	if _, ok := table["tBTCUSD"]; !ok {
		t.Errorf("table missing tBTCUSD: %+v", table)
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

// End-to-end form of the [null] trap: a 200 with a body that parses cleanly,
// so only parseConf's emptiness check makes it an error to back off from.
func TestTickerLoaderNullBodyIsError(t *testing.T) {
	server := confServer(t, `[null]`)

	if _, err := testLoader(server.URL)(context.Background()); err == nil {
		t.Error("tickerLoader() = nil error for a [null] body, want an error")
	}
}

func TestTickerLoaderMalformedBodyIsError(t *testing.T) {
	server := confServer(t, `not json at all`)

	if _, err := testLoader(server.URL)(context.Background()); err == nil {
		t.Error("tickerLoader() = nil error for a non-JSON body, want an error")
	}
}

// --- registry integration ---

func TestRegistryLoadsFromTickerLoader(t *testing.T) {
	server := confServer(t, confBody)

	r := symbols.NewRegistry(testLoader(server.URL), time.Hour)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}

	inst, ok := r.Lookup("tBTCUST")
	if !ok {
		t.Fatal("Lookup(\"tBTCUST\") = false, want true")
	}
	if inst.Market != "BTC-USDT" {
		t.Errorf("Market = %q, want %q", inst.Market, "BTC-USDT")
	}
}

func buildLargeFixture(n int) []byte {
	rows := make([]string, n)
	for i := range n {
		rows[i] = fmt.Sprintf(
			`["tBTCUSD", %f, 3.5, %f, 2.1, 120.5, 0.0019, 65433.0, 15234.7, 66000.0, 64000.0]`,
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
