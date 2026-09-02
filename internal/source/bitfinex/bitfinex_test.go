package bitfinex

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

func testTable(t *testing.T) symbols.Table {
	t.Helper()

	c, err := parseConf([]byte(confBody))
	if err != nil {
		t.Fatalf("parseConf() = %v, want nil", err)
	}
	return buildTable(c)
}

// --- parseTicks ---
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

// Bitfinex writes newer pairs with a colon between the halves. The separator
// is part of the symbol and must survive into the table key.
func TestParseTicksColonSymbolPreserved(t *testing.T) {
	body := `[["tDOGE:USD", 0.24, 5000.0, 0.241, 4800.0, 0.01, 0.04, 0.241, 900000.0, 0.25, 0.23]]`

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

// Positional rows carry no field names, so a row that is short or wrongly
// typed is unreadable rather than partially readable -- indexing it anyway
// would yield a plausible wrong price.
func TestParseTicksSkipsUnreadableRows(t *testing.T) {
	cases := []struct{ name, row string }{
		{"too short to index", `["tBTCUSD", 65432.0, 3.5]`},
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
				t.Errorf("len(ticks) = %d, want 0 (the row is unreadable): %+v", len(ticks), ticks)
			}
		})
	}
}

// A bad row must not take the good ones with it.
func TestParseTicksKeepsGoodRowsAlongsideBad(t *testing.T) {
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
		t.Errorf("survivor = %q, want %q", ticks[0].Symbol, "tETHUSD")
	}
}

func TestParseTicksMalformedJSONIsError(t *testing.T) {
	if _, err := parseTicks([]byte(`not json at all`)); err == nil {
		t.Error("parseTicks() = nil error for a non-JSON body, want an error")
	}
}

// --- parseConf ---

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
func TestParseConfRejectsUnusableSections(t *testing.T) {
	cases := []struct{ name, body string }{
		{"all sections null", `[null,null,null]`},
		{"pairs null", `[null,["BTC","USD"],[["UST","USDt"]]]`},
		{"currencies null", `[["BTCUSD"],null,[["UST","USDt"]]]`},
		{"aliases null", `[["BTCUSD"],["BTC","USD"],null]`},
		{"empty arrays", `[[],[],[]]`},
		// The literal response a bad conf key returns: one null section, so
		// the count check catches it before anything else can.
		{"single null section", `[null]`},
		{"too few sections", `[["BTCUSD"],["BTC","USD"]]`},
		{"not JSON", `not json at all`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := parseConf([]byte(c.body)); err == nil {
				t.Errorf("parseConf(%s) = nil error, want an error", c.body)
			}
		})
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

// --- splitPair ---

func knownCurrencies(codes ...string) map[string]struct{} {
	known := make(map[string]struct{}, len(codes))
	for _, c := range codes {
		known[c] = struct{}{}
	}
	return known
}

func TestSplitPair(t *testing.T) {
	known := knownCurrencies("BTC", "USD", "UST", "ADA", "AAVE", "UDC")

	cases := []struct {
		pair, base, quote string
		ok                bool
	}{
		// Requiring both halves to be listed currencies makes the split
		// exact rather than a guess: all 98 concatenated pairs in the live
		// list split with zero ambiguity.
		{pair: "BTCUSD", base: "BTC", quote: "USD", ok: true},
		{pair: "BTCUST", base: "BTC", quote: "UST", ok: true},
		{pair: "ADAUST", base: "ADA", quote: "UST", ok: true},
		{pair: "UDCUSD", base: "UDC", quote: "USD", ok: true},
		// Newer pairs are colon-delimited, which needs no guessing at all.
		{pair: "AAVE:USD", base: "AAVE", quote: "USD", ok: true},
		// Neither half resolves, so the pair is unusable.
		{pair: "BTCXYZ", ok: false},
		{pair: "XYZUSD", ok: false},
		{pair: "NONSENSE", ok: false},
	}

	for _, c := range cases {
		t.Run(c.pair, func(t *testing.T) {
			base, quoteAsset, ok := splitPair(c.pair, known)

			if ok != c.ok {
				t.Fatalf("splitPair(%q) ok = %v, want %v", c.pair, ok, c.ok)
			}
			if ok && (base != c.base || quoteAsset != c.quote) {
				t.Errorf("splitPair(%q) = %q, %q; want %q, %q", c.pair, base, quoteAsset, c.base, c.quote)
			}
		})
	}
}

// --- buildTable ---

func TestBuildTableKeysCarryTPrefix(t *testing.T) {
	table := testTable(t)

	// Ticker rows arrive as tBTCUSD; the conf pair list says BTCUSD.
	if _, ok := table["tBTCUSD"]; !ok {
		t.Errorf("table missing key %q: %+v", "tBTCUSD", table)
	}
	if _, ok := table["BTCUSD"]; ok {
		t.Error("table has an unprefixed key BTCUSD; ticker symbols will never match")
	}
}

func TestBuildTableAppliesAliases(t *testing.T) {
	table := testTable(t)

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
// TESTBTC -> BTC. Filtering must happen before aliasing, or fake prices land
// on the real BTC-USD market and cross against the other venues.
func TestBuildTableDropsPaperTradingPairs(t *testing.T) {
	table := testTable(t)

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
	table := testTable(t)

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

// --- tickerLoader ---

func TestTickerLoaderReturnsTable(t *testing.T) {
	server := sourcetest.Server(t, confBody)

	table, err := testLoader(server.URL)(context.Background())
	if err != nil {
		t.Fatalf("tickerLoader() = %v, want nil", err)
	}
	if _, ok := table["tBTCUSD"]; !ok {
		t.Errorf("table missing tBTCUSD: %+v", table)
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
			why:   "a 429 must back the poller off",
		},
		{
			// End-to-end form of the [null] trap: a 200 with a body that
			// decodes cleanly, so only parseConf's emptiness check makes it
			// an error to back off from.
			name:  "null body",
			serve: func(t *testing.T) string { return sourcetest.Server(t, `[null]`).URL },
			why:   "a bad conf key must not silently empty the table",
		},
		{
			name:  "unparseable body",
			serve: func(t *testing.T) string { return sourcetest.Server(t, `not json at all`).URL },
			why:   "schema drift must be reported, not swallowed",
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
	if got.confURL != defaultConfURL {
		t.Errorf("confURL = %q, want %q", got.confURL, defaultConfURL)
	}
	// Tickers and conf are different endpoints; wiring one into the other
	// would leave the poller parsing one as the other forever.
	if got.TickersURL == got.confURL {
		t.Error("TickersURL and confURL resolved to the same endpoint")
	}
	if got.Tuning.HTTPTimeout == 0 {
		t.Error("Tuning was not defaulted")
	}
}

func TestConfigWithDefaultsKeepsExplicitValues(t *testing.T) {
	cfg := Config{
		TickersURL: "http://tickers.invalid",
		confURL:    "http://conf.invalid",
		Tuning:     poller.Tuning{HTTPTimeout: time.Second},
	}

	got := cfg.withDefaults()
	if got.TickersURL != cfg.TickersURL || got.confURL != cfg.confURL {
		t.Errorf("URLs = %q / %q, want the explicit %q / %q",
			got.TickersURL, got.confURL, cfg.TickersURL, cfg.confURL)
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
	tickers := sourcetest.Server(t, `[["tBTCUSD", 65432.0, 3.5, 65433.0, 2.1, 120.5, 0.0019, 65433.0, 15234.7, 66000.0, 64000.0]]`)
	conf := sourcetest.Server(t, confBody)

	adapter := New(Config{
		TickersURL: tickers.URL,
		confURL:    conf.URL,
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
		if q.Market != "BTC-USD" {
			t.Errorf("Market = %q, want %q -- the symbol table did not resolve the ticker row", q.Market, "BTC-USD")
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
	server := sourcetest.Server(t, confBody)

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
