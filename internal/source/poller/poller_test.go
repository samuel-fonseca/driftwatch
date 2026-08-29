package poller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

// --- test helpers ---

// fixedTime gives every test a deterministic ObservedAt/ReceivedAt to assert
// against, rather than depending on time.Now() at test-run time.
var fixedTime = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

// testTable stands in for what a venue's loader resolves. Symbols are
// deliberately venue-neutral: the poller only ever sees the keys a Loader
// hands it. What is absent matters too -- a tick whose symbol is missing here
// must produce no quotes at all.
var testTable = symbols.Table{
	"BTCUSD":  {Symbol: "BTCUSD", Base: "BTC", Quote: "USD", Market: "BTC-USD"},
	"BTCUSDT": {Symbol: "BTCUSDT", Base: "BTC", Quote: "USDT", Market: "BTC-USDT"},
	"ETHUSD":  {Symbol: "ETHUSD", Base: "ETH", Quote: "USD", Market: "ETH-USD"},
}

// staticLoader serves table on every call. Run loads symbols before polling,
// so every Run test needs one; a func is all the poller wants, which keeps
// these tests free of a second httptest server.
func staticLoader(table symbols.Table) symbols.Loader {
	return func(context.Context) (symbols.Table, error) { return table, nil }
}

// testTicks parses the JSON array shape used by the tick fixtures below. It
// stands in for a venue's ParseFunc without pulling in a real venue package.
func testTicks(body []byte) ([]Tick, error) {
	var ticks []Tick
	if err := json.Unmarshal(body, &ticks); err != nil {
		return nil, err
	}
	return ticks, nil
}

// testPoller builds a Poller wired to url with timings short enough that a
// test does not spend seconds in backoff.
func testPoller(url string, loader symbols.Loader) *Poller {
	return New(Config{
		Venue:      "test",
		TickersURL: url,
		HTTP:       NewHTTP("test", 10*time.Second),
		Loader:     loader,
		Parse:      testTicks,
		Tuning: Tuning{
			PollInterval:   50 * time.Millisecond,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
			SymbolsRefresh: time.Hour,
		},
	})
}

// loadedPoller returns a Poller whose registry is already populated, for
// exercising toQuotes without going through Run.
func loadedPoller(t *testing.T) *Poller {
	t.Helper()
	p := testPoller("http://unused.invalid", staticLoader(testTable))
	if err := p.loadSymbols(context.Background()); err != nil {
		t.Fatalf("loadSymbols() = %v, want nil", err)
	}
	return p
}

type flakyServer struct {
	*httptest.Server
	requests  atomic.Int32
	failCount int32
}

func newFlakyServer(failCount int32, body string) *flakyServer {
	fs := &flakyServer{failCount: failCount}
	fs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := fs.requests.Add(1)
		if n <= fs.failCount {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	return fs
}

// --- toQuotes tests ---

func TestToQuotesSplitsBidAndAsk(t *testing.T) {
	p := loadedPoller(t)

	ticks := []Tick{{
		Symbol:   "BTCUSD",
		BidPrice: 65432.0, BidSize: 3.5,
		AskPrice: 65433.0, AskSize: 2.1,
		ObservedAt: fixedTime,
	}}

	quotes := p.toQuotes(ticks, fixedTime)
	if len(quotes) != 2 {
		t.Fatalf("len(quotes) = %d, want 2 (one bid, one ask)", len(quotes))
	}

	bySelection := map[string]quote.Quote{}
	for _, q := range quotes {
		bySelection[q.Selection] = q
	}

	bid, ok := bySelection["bid"]
	if !ok {
		t.Fatal("expected a bid quote")
	}
	if bid.Market != "BTC-USD" || bid.Venue != "test" || bid.Price != 65432.0 || bid.Size != 3.5 {
		t.Errorf("bid = %+v, unexpected fields", bid)
	}
	if !bid.ObservedAt.Equal(fixedTime) || !bid.ReceivedAt.Equal(fixedTime) {
		t.Errorf("bid timestamps = %v / %v, want both %v", bid.ObservedAt, bid.ReceivedAt, fixedTime)
	}

	ask, ok := bySelection["ask"]
	if !ok {
		t.Fatal("expected an ask quote")
	}
	if ask.Price != 65433.0 || ask.Size != 2.1 {
		t.Errorf("ask = %+v, unexpected fields", ask)
	}
}

func TestToQuotesZeroPriceSideDropped(t *testing.T) {
	p := loadedPoller(t)

	ticks := []Tick{{
		Symbol:   "ETHUSD",
		AskPrice: 3456.7, AskSize: 8.2,
		ObservedAt: fixedTime,
	}}

	quotes := p.toQuotes(ticks, fixedTime)
	if len(quotes) != 1 {
		t.Fatalf("len(quotes) = %d, want 1 (bid side is zero, ask side is not)", len(quotes))
	}
	if quotes[0].Selection != "ask" {
		t.Errorf("survivor Selection = %q, want %q", quotes[0].Selection, "ask")
	}
}

// A venue sends far more symbols than a table covers -- funding rows, dated
// futures, perpetuals. Anything the loader did not resolve must produce
// nothing rather than a quote on a guessed market.
func TestToQuotesUnknownSymbolDropped(t *testing.T) {
	p := loadedPoller(t)

	for _, symbol := range []string{"fUSD", "tGERMANY40IXF0:USTF0", "NOTLISTED"} {
		t.Run(symbol, func(t *testing.T) {
			ticks := []Tick{{
				Symbol:   symbol,
				BidPrice: 100.0, BidSize: 1.0,
				AskPrice: 101.0, AskSize: 1.0,
				ObservedAt: fixedTime,
			}}

			if quotes := p.toQuotes(ticks, fixedTime); len(quotes) != 0 {
				t.Fatalf("expected an unresolved symbol to produce zero quotes, got %+v", quotes)
			}
		})
	}
}

// Two symbols on one venue collapsing onto a single market is the bug the
// per-symbol table exists to prevent: BTCUSD and BTCUSDT are distinct books.
func TestToQuotesKeysAreDistinct(t *testing.T) {
	p := loadedPoller(t)

	ticks := []Tick{
		{Symbol: "BTCUSD", BidPrice: 65432.0, BidSize: 3.5, AskPrice: 65433.0, AskSize: 2.1},
		{Symbol: "BTCUSDT", BidPrice: 65400.0, BidSize: 3.0, AskPrice: 65401.0, AskSize: 2.0},
	}

	quotes := p.toQuotes(ticks, fixedTime)
	if len(quotes) != 4 {
		t.Fatalf("len(quotes) = %d, want 4 (two books, two sides each)", len(quotes))
	}

	seen := map[string]quote.Quote{}
	for _, q := range quotes {
		if prev, dup := seen[q.Key()]; dup {
			t.Errorf("Key() collision %q: %v @ %v and %v @ %v",
				q.Key(), prev.Market, prev.Price, q.Market, q.Price)
		}
		seen[q.Key()] = q
	}
}

func TestToQuotesMixedBatch(t *testing.T) {
	p := loadedPoller(t)

	ticks := []Tick{
		{Symbol: "BTCUSD", BidPrice: 65432.0, BidSize: 3.5, AskPrice: 65433.0, AskSize: 2.1},
		{Symbol: "fUSD", BidPrice: 0.00015, BidSize: 850000.0, AskPrice: 0.00016, AskSize: 620000.0},
		{Symbol: "ETHUSD", AskPrice: 3456.7, AskSize: 8.2},
	}

	quotes := p.toQuotes(ticks, fixedTime)
	if len(quotes) != 3 {
		t.Fatalf("len(quotes) = %d, want 3 (two from BTCUSD, none from fUSD, one from ETHUSD)", len(quotes))
	}
}

// Venues that publish no per-tick timestamp leave ObservedAt zero. Passing that
// through would store a year-1 observation, so the fetch time stands in.
func TestToQuotesObservedAtFallsBackToFetchedAt(t *testing.T) {
	p := loadedPoller(t)

	ticks := []Tick{{
		Symbol:   "BTCUSD",
		BidPrice: 65432.0, BidSize: 3.5,
	}} // ObservedAt deliberately zero

	quotes := p.toQuotes(ticks, fixedTime)
	if len(quotes) != 1 {
		t.Fatalf("len(quotes) = %d, want 1", len(quotes))
	}
	if !quotes[0].ObservedAt.Equal(fixedTime) {
		t.Errorf("ObservedAt = %v, want the fetch time %v", quotes[0].ObservedAt, fixedTime)
	}
}

// --- Run tests ---

// Run cannot poll without a venue, a URL, a parser, and a loader. Catching
// that up front turns a silent no-op poll loop into a startup failure.
func TestRunRequiresConfig(t *testing.T) {
	full := Config{
		Venue:      "test",
		TickersURL: "http://example.invalid",
		HTTP:       NewHTTP("test", time.Second),
		Loader:     staticLoader(testTable),
		Parse:      testTicks,
	}

	cases := []struct {
		name  string
		strip func(*Config)
	}{
		{"no venue", func(c *Config) { c.Venue = "" }},
		{"no tickers URL", func(c *Config) { c.TickersURL = "" }},
		{"no parse func", func(c *Config) { c.Parse = nil }},
		{"no loader", func(c *Config) { c.Loader = nil }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := full
			c.strip(&cfg)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			if err := New(cfg).Run(ctx, make(chan quote.Quote, 1)); err == nil {
				t.Error("Run() = nil error with an incomplete config, want an error")
			}
		})
	}
}

// A loader failure at startup is permanent as far as Run is concerned: without
// a symbol table every tick would be dropped, so it returns rather than
// polling into the void.
func TestRunReturnsLoaderError(t *testing.T) {
	sentinel := errors.New("venue unreachable")
	p := testPoller("http://example.invalid", func(context.Context) (symbols.Table, error) {
		return nil, sentinel
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := p.Run(ctx, make(chan quote.Quote, 1))
	if !errors.Is(err, sentinel) {
		t.Errorf("Run() = %v, want it to wrap %v", err, sentinel)
	}
}

func TestRunRecoversAfterTransientFailures(t *testing.T) {
	fs := newFlakyServer(3, `[{"Symbol":"BTCUSD","BidPrice":100.0,"BidSize":1.0,"AskPrice":101.0,"AskSize":1.0}]`)
	defer fs.Close()

	p := testPoller(fs.URL, staticLoader(testTable))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out := make(chan quote.Quote, 10)
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx, out)
	}()

	select {
	case q := <-out:
		if q.Market != "BTC-USD" {
			t.Errorf("got quote for %q, want BTC-USD", q.Market)
		}
	case err := <-errCh:
		t.Fatalf("Run returned early with err=%v before delivering any quote", err)
	case <-time.After(2 * time.Second):
		t.Fatal("no quote arrived within 2s despite the server recovering after 3 failures")
	}

	if got := fs.requests.Load(); got < 4 {
		t.Errorf("server only saw %d requests, want at least 4 (3 failures + 1 success)", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected a non-nil error (ctx.Err()) after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of cancellation")
	}
}

func TestRunReturnsPromptlyOnCancellation(t *testing.T) {
	fs := newFlakyServer(1_000_000, "")
	defer fs.Close()

	p := New(Config{
		Venue:      "test",
		TickersURL: fs.URL,
		HTTP:       NewHTTP("test", 10*time.Second),
		Loader:     staticLoader(testTable),
		Parse:      testTicks,
		Tuning: Tuning{
			PollInterval:   50 * time.Millisecond,
			InitialBackoff: 5 * time.Second, // deliberately long
			MaxBackoff:     5 * time.Second,
			SymbolsRefresh: time.Hour,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan quote.Quote, 10)

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx, out)
	}()

	time.Sleep(200 * time.Millisecond) // let it get into its first backoff sleep
	start := time.Now()
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected a non-nil error (ctx.Err()) on cancellation")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("Run took %v to return after cancellation during a long backoff sleep -- too slow", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run never returned after context cancellation")
	}
}

func BenchmarkToQuotes(b *testing.B) {
	p := New(Config{
		Venue:  "test",
		Loader: staticLoader(testTable),
		Tuning: Tuning{SymbolsRefresh: time.Hour},
	})
	if err := p.loadSymbols(context.Background()); err != nil {
		b.Fatalf("loadSymbols() = %v, want nil", err)
	}

	ticks := make([]Tick, 1000)
	for i := range ticks {
		ticks[i] = Tick{
			Symbol:   "BTCUSD",
			BidPrice: 65000.0 + float64(i%500), BidSize: 3.5,
			AskPrice: 65001.0 + float64(i%500), AskSize: 2.1,
		}
	}

	b.ReportAllocs()

	for b.Loop() {
		p.toQuotes(ticks, fixedTime)
	}
}
