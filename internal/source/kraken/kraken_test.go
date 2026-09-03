package kraken

import (
	"context"
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

// assetPairsBody is a trimmed capture of the real AssetPairs response, keeping
// every field Kraken actually sends so a decode that depends on the extras
// would still be exercised. XXBTZUSD is the important row: Kraken calls
// Bitcoin XBT, and its key, altname, and wsname all differ from one another.
const assetPairsBody = `{"error":[],"result":{
	"XXBTZUSD":{"altname":"XBTUSD","wsname":"XBT/USD","aclass_base":"currency","base":"XXBT","aclass_quote":"currency","quote":"ZUSD","lot":"unit","cost_decimals":5,"pair_decimals":1,"lot_decimals":8,"lot_multiplier":1,"fees":[[0,0.4]],"fees_maker":[[0,0.25]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"0.00005","costmin":"0.5","tick_size":"0.1","status":"online","execution_venue":"international"},
	"XETHZUSD":{"altname":"ETHUSD","wsname":"ETH/USD","aclass_base":"currency","base":"XETH","aclass_quote":"currency","quote":"ZUSD","lot":"unit","cost_decimals":5,"pair_decimals":2,"lot_decimals":8,"lot_multiplier":1,"fees":[[0,0.4]],"fees_maker":[[0,0.25]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"0.002","costmin":"0.5","tick_size":"0.01","status":"online","execution_venue":"international"},
	"0GEUR":{"altname":"0GEUR","wsname":"0G/EUR","aclass_base":"currency","base":"0G","aclass_quote":"currency","quote":"ZEUR","lot":"unit5","cost_decimals":5,"pair_decimals":3,"lot_decimals":5,"lot_multiplier":1,"fees":[[0,0.4]],"fees_maker":[[0,0.23]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"35","costmin":"0.45","tick_size":"0.001","status":"online","execution_venue":"international"}
}}`

func testHTTP() *poller.HTTP { return poller.NewHTTP(venue, 10*time.Second) }

// tickBySymbol finds a tick in a parseTicks result. parseTicks ranges over a
// map, so the output order is not deterministic and indexing is not safe.
func tickBySymbol(ticks []poller.Tick, symbol string) (poller.Tick, bool) {
	for _, tick := range ticks {
		if tick.Symbol == symbol {
			return tick, true
		}
	}
	return poller.Tick{}, false
}

// --- parseTicks ---
//
// Kraken's ticker fields are positional arrays of strings: a/b are
// [price, wholeLotVolume, lotVolume], so the size lives at index 2, not 1.
// parseTicks does not filter by symbol; the poller's table lookup drops
// anything unlisted, and those drops are covered in internal/source/poller.

func TestParseTicks(t *testing.T) {
	body := []byte(`{"error":[],"result":{"0GEUR":{"a":["0.1390000","10456","10456.000"],"b":["0.1380000","1272","1272.000"],"c":["0.1380000","241.10222"],"v":["47716.79697","120414.57487"],"p":["0.1426522","0.1400757"],"t":[169,308],"l":["0.1380000","0.1310000"],"h":["0.1460000","0.1460000"],"o":"0.1430000"},"0GUSD":{"a":["0.1600000","11903","11903.000"],"b":["0.1590000","41019","41019.000"],"c":["0.1610000","158.73739"],"v":["338511.71862","774760.84622"],"p":["0.1657123","0.1623335"],"t":[506,889],"l":["0.1600000","0.1520000"],"h":["0.1710000","0.1710000"],"o":"0.1650000"},"1INCHEUR":{"a":["0.07560","4253","4253.000"],"b":["0.07550","4221","4221.000"],"c":["0.07560","27.59844000"],"v":["11667.76213764","58536.96665133"],"p":["0.07594","0.07576"],"t":[7,36],"l":["0.07550","0.07400"],"h":["0.07630","0.07780"],"o":"0.07630"}}}`)

	ticks, err := parseTicks(body)
	if err != nil {
		t.Fatalf("parseTicks() = %v, want nil", err)
	}
	if len(ticks) != 3 {
		t.Fatalf("len(ticks) = %d, want 3", len(ticks))
	}

	// Assert the values, not just the count: the fields are read by
	// position, so a reordering upstream would still yield three
	// well-formed ticks carrying the wrong numbers.
	got, ok := tickBySymbol(ticks, "0GEUR")
	if !ok {
		t.Fatalf("0GEUR missing from %+v", ticks)
	}
	if got.BidPrice != 0.1380000 || got.BidSize != 1272.000 {
		t.Errorf("bid = %v @ %v, want 0.138 @ 1272 (size is index 2, not 1)", got.BidPrice, got.BidSize)
	}
	if got.AskPrice != 0.1390000 || got.AskSize != 10456.000 {
		t.Errorf("ask = %v @ %v, want 0.139 @ 10456", got.AskPrice, got.AskSize)
	}
	// Kraken sends no per-pair timestamp, so the poller substitutes its
	// fetch time -- see the ObservedAt fallback in internal/source/poller.
	if !got.ObservedAt.IsZero() {
		t.Errorf("ObservedAt = %v, want the zero value", got.ObservedAt)
	}
}

func TestParseTicksSkipsUnreadableEntries(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string // the one symbol expected to survive
		why  string
	}{
		{
			name: "non-numeric fields",
			body: `{"error":[],"result":{"0GEUR":{"a":["non-float","10456","10456.000"],"b":["0.1380000","1272","1272.000"]},"0GUSD":{"a":["0.1600000","11903","non-float"],"b":["0.1590000","41019","41019.000"]},"1INCHEUR":{"a":["0.07560","4253","4253.000"],"b":["0.07550","4221","4221.000"]},"AAVEETH":{"a":["0.05057","16","16.000"],"b":["0.05007","19","non-float"]}}}`,
			want: "1INCHEUR",
			why:  "a field that will not parse makes the whole entry unusable",
		},
		{
			name: "short arrays",
			body: `{"error":[],"result":{"0GEUR":{"a":["0.139","10456"],"b":["0.138","1272","1272.000"]},"0GUSD":{"a":["0.160","11903","11903.000"],"b":[]},"1INCHEUR":{"a":["0.0756","4253","4253.000"],"b":["0.0755","4221","4221.000"]}}}`,
			want: "1INCHEUR",
			why:  "a short array cannot be read positionally; indexing it would panic",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ticks, err := parseTicks([]byte(c.body))
			if err != nil {
				t.Fatalf("parseTicks() = %v, want nil", err)
			}
			if len(ticks) != 1 {
				t.Fatalf("len(ticks) = %d, want 1 -- %s: %+v", len(ticks), c.why, ticks)
			}
			if ticks[0].Symbol != c.want {
				t.Errorf("survivor = %q, want %q", ticks[0].Symbol, c.want)
			}
		})
	}
}

// Kraken reports application errors in a 200 body, so the error array is the
// only signal that the result is not usable.
func TestParseTicksReportsVenueErrors(t *testing.T) {
	ticks, err := parseTicks([]byte(`{"error":["foo","bar"],"result":{}}`))
	if err == nil {
		t.Fatal("parseTicks() = nil error, want one")
	}
	if err.Error() != "Kraken returned: [foo bar]" {
		t.Errorf("error = %q, want %q", err, "Kraken returned: [foo bar]")
	}
	if len(ticks) != 0 {
		t.Errorf("len(ticks) = %d, want 0", len(ticks))
	}
}

func TestParseTicksMalformedJSONIsError(t *testing.T) {
	if _, err := parseTicks([]byte(`not json at all`)); err == nil {
		t.Error("parseTicks() = nil error for a non-JSON body, want an error")
	}
}

// --- buildTable ---

// The assertion this whole package exists for. Kraken calls Bitcoin XBT; if
// that is not canonicalised to BTC, every Kraken market becomes an island --
// it never joins Binance's or Bitfinex's BTC-USD, no cross-venue signal is
// ever possible, and nothing anywhere reports an error.
func TestBuildTableCanonicalisesXBTToBTC(t *testing.T) {
	table, err := buildTable([]byte(assetPairsBody))
	if err != nil {
		t.Fatalf("buildTable() = %v, want nil", err)
	}

	inst, ok := table["XXBTZUSD"]
	if !ok {
		t.Fatalf("table missing XXBTZUSD: %+v", table)
	}
	if inst.Market != "BTC-USD" {
		t.Errorf("Market = %q, want %q -- Kraken will not join the other venues", inst.Market, "BTC-USD")
	}
	if inst.Base != "BTC" || inst.Quote != "USD" {
		t.Errorf("Base/Quote = %q/%q, want BTC/USD", inst.Base, inst.Quote)
	}
}

// Ticker rows arrive keyed by the AssetPairs map key (XXBTZUSD), never by
// altname (XBTUSD) or wsname (XBT/USD). Keying on the wrong one produces a
// table that matches nothing.
func TestBuildTableKeysOnTheAssetPairsKey(t *testing.T) {
	table, err := buildTable([]byte(assetPairsBody))
	if err != nil {
		t.Fatalf("buildTable() = %v, want nil", err)
	}

	if inst := table["XXBTZUSD"]; inst.Symbol != "XXBTZUSD" {
		t.Errorf("Symbol = %q, want %q", inst.Symbol, "XXBTZUSD")
	}
	for _, wrong := range []string{"XBTUSD", "XBT/USD", "XBT-USD"} {
		if _, ok := table[wrong]; ok {
			t.Errorf("table has key %q; ticker symbols will never match it", wrong)
		}
	}
}

func TestBuildTableBuildsEveryOnlinePair(t *testing.T) {
	table, err := buildTable([]byte(assetPairsBody))
	if err != nil {
		t.Fatalf("buildTable() = %v, want nil", err)
	}
	if len(table) != 3 {
		t.Fatalf("len(table) = %d, want 3: %+v", len(table), table)
	}

	cases := []struct{ symbol, market string }{
		{"XXBTZUSD", "BTC-USD"}, // XBT -> BTC
		{"XETHZUSD", "ETH-USD"}, // no alias needed
		{"0GEUR", "0G-EUR"},     // a numeric-leading base must survive intact
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

// Two symbols on one venue resolving to one market is the collision the
// per-symbol table exists to prevent. Kraken's live list is clean today; this
// keeps it that way as pairs are added.
func TestBuildTableHasNoMarketCollisions(t *testing.T) {
	table, err := buildTable([]byte(assetPairsBody))
	if err != nil {
		t.Fatalf("buildTable() = %v, want nil", err)
	}

	seen := make(map[string]string, len(table))
	for symbol, inst := range table {
		if prev, dup := seen[inst.Market]; dup {
			t.Errorf("market %q claimed by both %q and %q", inst.Market, prev, symbol)
		}
		seen[inst.Market] = symbol
	}
}

func TestBuildTableSkipsUnusablePairs(t *testing.T) {
	cases := []struct {
		name string
		body string
		why  string
	}{
		{
			name: "not online",
			body: `{"error":[],"result":{
				"XXBTZUSD":{"wsname":"XBT/USD","status":"online"},
				"XETHZUSD":{"wsname":"ETH/USD","status":"cancel_only"}
			}}`,
			why: "a halted or cancel-only pair is not quotable",
		},
		{
			name: "unusable wsname",
			body: `{"error":[],"result":{
				"XXBTZUSD":{"wsname":"XBT/USD","status":"online"},
				"NOSLASH":{"wsname":"XBTUSD","status":"online"},
				"NOBASE":{"wsname":"/USD","status":"online"},
				"NOQUOTE":{"wsname":"XBT/","status":"online"},
				"EMPTY":{"wsname":"","status":"online"}
			}}`,
			why: "wsname is the only source of base/quote, so a bad one is unreadable",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			table, err := buildTable([]byte(c.body))
			if err != nil {
				t.Fatalf("buildTable() = %v, want nil", err)
			}
			if len(table) != 1 {
				t.Fatalf("len(table) = %d, want 1 -- %s: %+v", len(table), c.why, table)
			}
			for _, inst := range table {
				if strings.HasPrefix(inst.Market, "-") || strings.HasSuffix(inst.Market, "-") {
					t.Errorf("built a half-empty market %q", inst.Market)
				}
			}
		})
	}
}

// The silent-death cases. Without these guards the registry installs an empty
// table, every lookup misses, and Kraken publishes nothing forever without a
// single error or backoff to show for it.
func TestBuildTableEmptyOutcomeIsError(t *testing.T) {
	cases := []struct {
		name, body, wantIn string
	}{
		{
			name:   "venue reported an error",
			body:   `{"error":["foo","bar"],"result":{}}`,
			wantIn: "Kraken returned: [foo bar]",
		},
		{
			name: "no pairs at all",
			body: `{"error":[],"result":{}}`,
		},
		{
			// The realistic schema-drift case: status still parses, but
			// wsname has been renamed or reformatted, so every pair is
			// skipped and the table comes out empty.
			name: "every pair dropped",
			body: `{"error":[],"result":{
				"XXBTZUSD":{"wsname":"XBTUSD","status":"online"},
				"XETHZUSD":{"wsname":"ETHUSD","status":"online"}
			}}`,
			wantIn: "2",
		},
		{
			name: "not JSON",
			body: `not json at all`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			table, err := buildTable([]byte(c.body))
			if err == nil {
				t.Fatalf("buildTable() = nil error, want one (table was %+v)", table)
			}
			if table != nil {
				t.Errorf("buildTable() returned %+v alongside an error, want a nil table", table)
			}
			if c.wantIn != "" && !strings.Contains(err.Error(), c.wantIn) {
				t.Errorf("error %q does not mention %q", err, c.wantIn)
			}
		})
	}
}

// --- tickerLoader ---

// tickerLoader must stay usable as the symbols.Loader the poller calls; this
// is what makes the closure in New compile.
var _ symbols.Loader = func(ctx context.Context) (symbols.Table, error) {
	return tickerLoader(ctx, testHTTP(), defaultAssetPairsURL)
}

func TestTickerLoaderReturnsTable(t *testing.T) {
	server := sourcetest.Server(t, assetPairsBody)

	table, err := tickerLoader(context.Background(), testHTTP(), server.URL)
	if err != nil {
		t.Fatalf("tickerLoader() = %v, want nil", err)
	}
	if inst, ok := table["XXBTZUSD"]; !ok || inst.Market != "BTC-USD" {
		t.Errorf("table[\"XXBTZUSD\"] = %+v, want a BTC-USD instrument", inst)
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
			// End-to-end form of the empty-table trap: a 200 with a body
			// that parses cleanly, so only buildTable's emptiness check
			// makes it an error.
			name:  "empty result",
			serve: func(t *testing.T) string { return sourcetest.Server(t, `{"error":[],"result":{}}`).URL },
			why:   "an empty table would silence the venue without an error",
		},
		{
			name:  "unparseable body",
			serve: func(t *testing.T) string { return sourcetest.Server(t, `not json at all`).URL },
			why:   "schema drift must be reported, not swallowed",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := tickerLoader(context.Background(), testHTTP(), c.serve(t)); err == nil {
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
	if got.AssetPairsURL != defaultAssetPairsURL {
		t.Errorf("AssetPairsURL = %q, want %q", got.AssetPairsURL, defaultAssetPairsURL)
	}
	// Ticker and AssetPairs are different endpoints; wiring one into the
	// other would leave the poller parsing one as the other forever.
	if got.TickersURL == got.AssetPairsURL {
		t.Error("TickersURL and AssetPairsURL resolved to the same endpoint")
	}
	if got.Tuning.HTTPTimeout == 0 {
		t.Error("Tuning was not defaulted")
	}
}

func TestConfigWithDefaultsKeepsExplicitValues(t *testing.T) {
	cfg := Config{
		TickersURL:    "http://tickers.invalid",
		AssetPairsURL: "http://pairs.invalid",
		Tuning:        poller.Tuning{HTTPTimeout: time.Second},
	}

	got := cfg.withDefaults()
	if got.TickersURL != cfg.TickersURL || got.AssetPairsURL != cfg.AssetPairsURL {
		t.Errorf("URLs = %q / %q, want the explicit %q / %q",
			got.TickersURL, got.AssetPairsURL, cfg.TickersURL, cfg.AssetPairsURL)
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
	tickers := sourcetest.Server(t, `{"error":[],"result":{"XXBTZUSD":{"a":["65433.0","1","2.1"],"b":["65432.0","1","3.5"]}}}`)
	pairs := sourcetest.Server(t, assetPairsBody)

	adapter := New(Config{
		TickersURL:    tickers.URL,
		AssetPairsURL: pairs.URL,
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
	server := sourcetest.Server(t, assetPairsBody)
	loader := func(ctx context.Context) (symbols.Table, error) {
		return tickerLoader(ctx, testHTTP(), server.URL)
	}

	r := symbols.NewRegistry(loader, time.Hour)
	if err := r.Load(context.Background()); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}

	inst, ok := r.Lookup("XXBTZUSD")
	if !ok {
		t.Fatal("Lookup(\"XXBTZUSD\") = false, want true")
	}
	if inst.Market != "BTC-USD" {
		t.Errorf("Market = %q, want %q", inst.Market, "BTC-USD")
	}
}
