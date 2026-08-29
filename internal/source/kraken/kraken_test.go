package kraken

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
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

func testServer(t *testing.T, body string) *httptest.Server {
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

// --- parseTicks tests ---
//
// Kraken's ticker fields are positional arrays of strings: a/b are
// [price, wholeLotVolume, lotVolume], so the size lives at index 2, not 1.
// parseTicks does not filter by symbol; the poller's table lookup drops
// anything unlisted, and those drops are covered in internal/source/poller.

func TestParseTicks(t *testing.T) {
	serverResponse := []byte(`{"error":[],"result":{"0GEUR":{"a":["0.1390000","10456","10456.000"],"b":["0.1380000","1272","1272.000"],"c":["0.1380000","241.10222"],"v":["47716.79697","120414.57487"],"p":["0.1426522","0.1400757"],"t":[169,308],"l":["0.1380000","0.1310000"],"h":["0.1460000","0.1460000"],"o":"0.1430000"},"0GUSD":{"a":["0.1600000","11903","11903.000"],"b":["0.1590000","41019","41019.000"],"c":["0.1610000","158.73739"],"v":["338511.71862","774760.84622"],"p":["0.1657123","0.1623335"],"t":[506,889],"l":["0.1600000","0.1520000"],"h":["0.1710000","0.1710000"],"o":"0.1650000"},"1INCHEUR":{"a":["0.07560","4253","4253.000"],"b":["0.07550","4221","4221.000"],"c":["0.07560","27.59844000"],"v":["11667.76213764","58536.96665133"],"p":["0.07594","0.07576"],"t":[7,36],"l":["0.07550","0.07400"],"h":["0.07630","0.07780"],"o":"0.07630"}}}`)

	ticks, err := parseTicks(serverResponse)
	if err != nil {
		t.Fatalf("parseTicks: unexpected error %v", err)
	}
	if len(ticks) != 3 {
		t.Fatalf("parseTicks: not enough ticks in response: got %d, want 3", len(ticks))
	}

	// Assert the values, not just the count: the fields are read by position,
	// so a reordering upstream would still yield three well-formed ticks.
	got, ok := tickBySymbol(ticks, "0GEUR")
	if !ok {
		t.Fatalf("parseTicks: 0GEUR missing from %+v", ticks)
	}
	if got.BidPrice != 0.1380000 || got.BidSize != 1272.000 {
		t.Errorf("bid = %v @ %v, want 0.138 @ 1272", got.BidPrice, got.BidSize)
	}
	if got.AskPrice != 0.1390000 || got.AskSize != 10456.000 {
		t.Errorf("ask = %v @ %v, want 0.139 @ 10456", got.AskPrice, got.AskSize)
	}
	// Kraken sends no per-pair timestamp, so the poller substitutes its fetch
	// time -- see the ObservedAt fallback in internal/source/poller.
	if !got.ObservedAt.IsZero() {
		t.Errorf("ObservedAt = %v, want the zero value", got.ObservedAt)
	}
}

func TestParseTicksHandlesErrorsInResponse(t *testing.T) {
	serverResponse := []byte(`{"error":["foo","bar"],"result":{}}`)
	ticks, err := parseTicks(serverResponse)
	if err == nil {
		t.Fatalf("parseTicks: expected error, got nil")
	}
	if err.Error() != "Kraken returned: [foo bar]" {
		t.Errorf("parseTicks: got \"%v\", want \"Kraken returned: [foo bar]\"", err)
	}
	if len(ticks) != 0 {
		t.Errorf("parseTicks: got %d, want 0", len(ticks))
	}
}

func TestParseTicksSkipsBadFloatFields(t *testing.T) {
	serverResponse := []byte(`{"error":[],"result":{"0GEUR":{"a":["non-float","10456","10456.000"],"b":["0.1380000","1272","1272.000"]},"0GUSD":{"a":["0.1600000","11903","non-float"],"b":["0.1590000","41019","41019.000"]},"1INCHEUR":{"a":["0.07560","4253","4253.000"],"b":["non-float","4221","4221.000"]},"AAVEETH":{"a":["0.05057","16","16.000"],"b":["0.05007","19","non-float"]},"AAVEUSD":{"a":["122.60000","non-float","1.000"],"b":["122.54000","non-float","11.000"]}}}`)
	ticks, err := parseTicks(serverResponse)
	if err != nil {
		t.Fatalf("parseTicks: unexpected error %v", err)
	}
	if len(ticks) != 1 {
		t.Errorf("parseTicks: too many ticks in response: got %d, want 1", len(ticks))
	}
}

// A short array cannot be read positionally at all -- indexing it would panic,
// and guessing at the missing field would invent a price.
func TestParseTicksSkipsShortArrays(t *testing.T) {
	serverResponse := []byte(`{"error":[],"result":{"0GEUR":{"a":["0.139","10456"],"b":["0.138","1272","1272.000"]},"0GUSD":{"a":["0.160","11903","11903.000"],"b":[]},"1INCHEUR":{"a":["0.0756","4253","4253.000"],"b":["0.0755","4221","4221.000"]}}}`)
	ticks, err := parseTicks(serverResponse)
	if err != nil {
		t.Fatalf("parseTicks: unexpected error %v", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("parseTicks: got %d ticks, want 1: %+v", len(ticks), ticks)
	}
	if ticks[0].Symbol != "1INCHEUR" {
		t.Errorf("survivor = %q, want %q", ticks[0].Symbol, "1INCHEUR")
	}
}

func TestParseTicksMalformedJSONIsError(t *testing.T) {
	if _, err := parseTicks([]byte(`not json at all`)); err == nil {
		t.Error("parseTicks: expected an error for a non-JSON body, got nil")
	}
}

// --- buildTable tests ---

// The assertion this whole package exists for. Kraken calls Bitcoin XBT; if
// that is not canonicalised to BTC, every Kraken market becomes an island --
// it never joins Binance's or Bitfinex's BTC-USD, no cross-venue signal is
// ever possible, and nothing anywhere reports an error.
func TestBuildTableCanonicalisesXBTToBTC(t *testing.T) {
	table, err := buildTable([]byte(assetPairsBody))
	if err != nil {
		t.Fatalf("buildTable: unexpected error %v", err)
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
		t.Fatalf("buildTable: unexpected error %v", err)
	}

	if _, ok := table["XXBTZUSD"]; !ok {
		t.Errorf("table missing key %q: %+v", "XXBTZUSD", table)
	}
	for _, wrong := range []string{"XBTUSD", "XBT/USD", "XBT-USD"} {
		if _, ok := table[wrong]; ok {
			t.Errorf("table has key %q; ticker symbols will never match it", wrong)
		}
	}
	if inst := table["XXBTZUSD"]; inst.Symbol != "XXBTZUSD" {
		t.Errorf("Symbol = %q, want %q", inst.Symbol, "XXBTZUSD")
	}
}

func TestBuildTableBuildsEveryOnlinePair(t *testing.T) {
	table, err := buildTable([]byte(assetPairsBody))
	if err != nil {
		t.Fatalf("buildTable: unexpected error %v", err)
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
		t.Fatalf("buildTable: unexpected error %v", err)
	}

	seen := make(map[string]string, len(table))
	for symbol, inst := range table {
		if prev, dup := seen[inst.Market]; dup {
			t.Errorf("market %q claimed by both %q and %q", inst.Market, prev, symbol)
		}
		seen[inst.Market] = symbol
	}
}

func TestBuildTableHandlesErrorsInResponse(t *testing.T) {
	table, err := buildTable([]byte(`{"error":["foo","bar"],"result":{}}`))
	if err == nil {
		t.Fatalf("buildTable: expected error, got nil")
	}
	if err.Error() != "Kraken returned: [foo bar]" {
		t.Errorf("buildTable: got %q, want %q", err, "Kraken returned: [foo bar]")
	}
	if table != nil {
		t.Errorf("buildTable: got %+v, want a nil table", table)
	}
}

// A pair that is halted or cancel-only is not quotable; publishing prices for
// it would report a market that is not trading.
func TestBuildTableSkipsNonOnlineStatus(t *testing.T) {
	body := `{"error":[],"result":{
		"XXBTZUSD":{"wsname":"XBT/USD","base":"XXBT","quote":"ZUSD","status":"online"},
		"XETHZUSD":{"wsname":"ETH/USD","base":"XETH","quote":"ZUSD","status":"cancel_only"}
	}}`

	table, err := buildTable([]byte(body))
	if err != nil {
		t.Fatalf("buildTable: unexpected error %v", err)
	}
	if len(table) != 1 {
		t.Fatalf("len(table) = %d, want 1 (the cancel_only pair dropped): %+v", len(table), table)
	}
	if _, ok := table["XETHZUSD"]; ok {
		t.Error("the cancel_only pair survived, want it dropped")
	}
}

// wsname is the only source of base/quote now, so a row without a usable one
// is unreadable rather than partially readable.
func TestBuildTableSkipsUnusableWsName(t *testing.T) {
	body := `{"error":[],"result":{
		"XXBTZUSD":{"wsname":"XBT/USD","status":"online"},
		"NOSLASH":{"wsname":"XBTUSD","status":"online"},
		"NOBASE":{"wsname":"/USD","status":"online"},
		"NOQUOTE":{"wsname":"XBT/","status":"online"},
		"EMPTY":{"wsname":"","status":"online"}
	}}`

	table, err := buildTable([]byte(body))
	if err != nil {
		t.Fatalf("buildTable: unexpected error %v", err)
	}
	if len(table) != 1 {
		t.Fatalf("len(table) = %d, want 1 (four unusable wsnames dropped): %+v", len(table), table)
	}
	for _, inst := range table {
		if strings.HasPrefix(inst.Market, "-") || strings.HasSuffix(inst.Market, "-") {
			t.Errorf("built a half-empty market %q", inst.Market)
		}
	}
}

// A 200 with an empty result is the silent-death case: without this guard the
// registry installs an empty table, every lookup misses, and Kraken publishes
// nothing forever without a single error or backoff.
func TestBuildTableEmptyResultIsError(t *testing.T) {
	if _, err := buildTable([]byte(`{"error":[],"result":{}}`)); err == nil {
		t.Error("buildTable: expected an error for an empty result, got nil")
	}
}

// The realistic schema-drift case: status still parses, but wsname has been
// renamed or reformatted, so every pair is skipped and the table comes out
// empty. Must be an error to back off from, not a silently empty venue.
func TestBuildTableAllPairsDroppedIsError(t *testing.T) {
	body := `{"error":[],"result":{
		"XXBTZUSD":{"wsname":"XBTUSD","status":"online"},
		"XETHZUSD":{"wsname":"ETHUSD","status":"online"}
	}}`

	_, err := buildTable([]byte(body))
	if err == nil {
		t.Fatal("buildTable: expected an error when every pair is dropped, got nil")
	}
	if !strings.Contains(err.Error(), "2") {
		t.Errorf("error %q should report how many pairs were dropped", err)
	}
}

func TestBuildTableMalformedJSONIsError(t *testing.T) {
	if _, err := buildTable([]byte(`not json at all`)); err == nil {
		t.Error("buildTable: expected an error for a non-JSON body, got nil")
	}
}

// --- tickerLoader tests ---

func TestTickerLoaderReturnsTable(t *testing.T) {
	server := testServer(t, assetPairsBody)
	h := poller.NewHTTP(venue, 10*time.Second)

	table, err := tickerLoader(context.Background(), h, server.URL)
	if err != nil {
		t.Fatalf("tickerLoader: %v", err)
	}
	if inst, ok := table["XXBTZUSD"]; !ok || inst.Market != "BTC-USD" {
		t.Errorf("table[\"XXBTZUSD\"] = %+v, want a BTC-USD instrument", inst)
	}
}

func TestTickerLoaderNonOKStatusIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	h := poller.NewHTTP(venue, 10*time.Second)
	if _, err := tickerLoader(context.Background(), h, server.URL); err == nil {
		t.Error("tickerLoader: expected an error for a 429, got nil")
	}
}

// End-to-end form of the empty-table trap: a 200 with a body that parses
// cleanly, so only buildTable's emptiness check makes it an error.
func TestTickerLoaderEmptyResultIsError(t *testing.T) {
	server := testServer(t, `{"error":[],"result":{}}`)
	h := poller.NewHTTP(venue, 10*time.Second)

	if _, err := tickerLoader(context.Background(), h, server.URL); err == nil {
		t.Error("tickerLoader: expected an error for an empty result, got nil")
	}
}

func TestTickerLoaderMalformedBodyIsError(t *testing.T) {
	server := testServer(t, `not json at all`)
	h := poller.NewHTTP(venue, 10*time.Second)

	if _, err := tickerLoader(context.Background(), h, server.URL); err == nil {
		t.Error("tickerLoader: expected an error for a non-JSON body, got nil")
	}
}

// tickerLoader must stay usable as the symbols.Loader the poller calls -- this
// is what makes the closure in New() compile.
func TestTickerLoaderSatisfiesLoader(t *testing.T) {
	h := poller.NewHTTP(venue, 10*time.Second)
	var _ symbols.Loader = func(ctx context.Context) (symbols.Table, error) {
		return tickerLoader(ctx, h, defaultAssetPairsURL)
	}
}
