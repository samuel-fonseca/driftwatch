package poller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
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

// fastTuning keeps a test out of multi-second backoffs.
func fastTuning() Tuning {
	return Tuning{
		PollInterval:   50 * time.Millisecond,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		SymbolsRefresh: time.Hour,
	}
}

func testConfig(url string, loader symbols.Loader) Config {
	return Config{
		Venue:      "test",
		TickersURL: url,
		HTTP:       NewHTTP("test", 10*time.Second),
		Loader:     loader,
		Parse:      testTicks,
		Tuning:     fastTuning(),
	}
}

func testPoller(url string, loader symbols.Loader) *Poller {
	return New(testConfig(url, loader))
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

// scriptedServer answers each request from respond, which sees the 1-based
// request number. It covers both "fails then recovers" and "serves garbage
// then recovers" without a second fake.
type scriptedServer struct {
	*httptest.Server
	requests atomic.Int32
}

func newScriptedServer(t *testing.T, respond func(n int32) (status int, body string)) *scriptedServer {
	t.Helper()

	s := &scriptedServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		status, body := respond(s.requests.Add(1))
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

// failThenServe returns 503 for the first failCount requests, then body.
func failThenServe(t *testing.T, failCount int32, body string) *scriptedServer {
	t.Helper()

	return newScriptedServer(t, func(n int32) (int, string) {
		if n <= failCount {
			return http.StatusServiceUnavailable, ""
		}
		return http.StatusOK, body
	})
}

// waitForRequests blocks until the server has been hit n times.
func waitForRequests(t *testing.T, s *scriptedServer, n int32) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.requests.Load() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("server saw %d requests, want at least %d", s.requests.Load(), n)
}

const tickBody = `[{"Symbol":"BTCUSD","BidPrice":100.0,"BidSize":1.0,"AskPrice":101.0,"AskSize":1.0}]`

// --- config ---

func TestName(t *testing.T) {
	if got := New(Config{Venue: "kraken"}).Name(); got != "kraken" {
		t.Errorf("Name() = %q, want %q", got, "kraken")
	}
}

func TestTuningWithDefaults(t *testing.T) {
	got := Tuning{}.WithDefaults()

	cases := []struct {
		name      string
		got, want time.Duration
	}{
		{"HTTPTimeout", got.HTTPTimeout, 10 * time.Second},
		{"PollInterval", got.PollInterval, 2 * time.Second},
		{"InitialBackoff", got.InitialBackoff, 500 * time.Millisecond},
		{"MaxBackoff", got.MaxBackoff, 60 * time.Second},
		{"SymbolsRefresh", got.SymbolsRefresh, 24 * time.Hour},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestTuningWithDefaultsKeepsExplicitValues(t *testing.T) {
	want := Tuning{
		HTTPTimeout:    time.Second,
		PollInterval:   2 * time.Second,
		InitialBackoff: 3 * time.Second,
		MaxBackoff:     4 * time.Second,
		SymbolsRefresh: 5 * time.Second,
	}

	if got := want.WithDefaults(); got != want {
		t.Errorf("WithDefaults() = %+v, want the explicit %+v", got, want)
	}
}

// backoff.Next doubles, and doubling zero stays zero -- a Poller built with a
// bare Tuning would retry a failing venue in a hot loop. New defends against
// that by defaulting the Tuning it is handed.
func TestNewAppliesTuningDefaults(t *testing.T) {
	p := New(Config{Venue: "test"})

	if p.cfg.Tuning.InitialBackoff == 0 {
		t.Error("InitialBackoff = 0, want a default -- backoff would never grow")
	}
	if p.cfg.Tuning.MaxBackoff == 0 {
		t.Error("MaxBackoff = 0, want a default")
	}
	if p.cfg.Tuning.SymbolsRefresh == 0 {
		t.Error("SymbolsRefresh = 0, want a default -- a zero ticker interval panics")
	}
}

// --- toQuotes ---

func TestToQuotes(t *testing.T) {
	cases := []struct {
		name     string
		tick     Tick
		wantKeys []string
		why      string
	}{
		{
			name:     "both sides quoted",
			tick:     Tick{Symbol: "BTCUSD", BidPrice: 65432, BidSize: 3.5, AskPrice: 65433, AskSize: 2.1},
			wantKeys: []string{"test|BTC-USD|ask", "test|BTC-USD|bid"},
			why:      "a two-sided tick becomes one quote per side",
		},
		{
			name:     "bid side missing",
			tick:     Tick{Symbol: "ETHUSD", AskPrice: 3456.7, AskSize: 8.2},
			wantKeys: []string{"test|ETH-USD|ask"},
			why:      "a zero price is absence, not a real price of zero",
		},
		{
			name:     "ask side missing",
			tick:     Tick{Symbol: "ETHUSD", BidPrice: 3456.7, BidSize: 8.2},
			wantKeys: []string{"test|ETH-USD|bid"},
			why:      "a zero price is absence, not a real price of zero",
		},
		{
			name:     "neither side quoted",
			tick:     Tick{Symbol: "ETHUSD"},
			wantKeys: nil,
			why:      "an empty book produces nothing at all",
		},
		{
			// Venues send far more symbols than a table covers -- funding
			// rows, dated futures, perpetuals. Anything the loader did not
			// resolve must produce nothing rather than a guessed market.
			name:     "symbol absent from the table",
			tick:     Tick{Symbol: "fUSD", BidPrice: 100, BidSize: 1, AskPrice: 101, AskSize: 1},
			wantKeys: nil,
			why:      "an unresolved symbol has no market to publish under",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := loadedPoller(t)

			got := p.toQuotes([]Tick{c.tick}, fixedTime)

			keys := make([]string, 0, len(got))
			for _, q := range got {
				keys = append(keys, q.Key())
			}
			slices.Sort(keys)

			if !slices.Equal(keys, c.wantKeys) {
				t.Errorf("keys = %v, want %v -- %s", keys, c.wantKeys, c.why)
			}
		})
	}
}

func TestToQuotesCopiesTickFieldsOntoBothSides(t *testing.T) {
	p := loadedPoller(t)

	ticks := []Tick{{
		Symbol:   "BTCUSD",
		BidPrice: 65432.0, BidSize: 3.5,
		AskPrice: 65433.0, AskSize: 2.1,
		ObservedAt: fixedTime,
	}}

	bySelection := map[string]quote.Quote{}
	for _, q := range p.toQuotes(ticks, fixedTime) {
		bySelection[q.Selection] = q
	}

	bid, ok := bySelection["bid"]
	if !ok {
		t.Fatal("no bid quote produced")
	}
	if bid.Venue != "test" || bid.Market != "BTC-USD" || bid.Price != 65432.0 || bid.Size != 3.5 {
		t.Errorf("bid = %+v, want the tick's bid side on market BTC-USD", bid)
	}
	if !bid.ObservedAt.Equal(fixedTime) || !bid.ReceivedAt.Equal(fixedTime) {
		t.Errorf("bid timestamps = %v / %v, want both %v", bid.ObservedAt, bid.ReceivedAt, fixedTime)
	}

	ask, ok := bySelection["ask"]
	if !ok {
		t.Fatal("no ask quote produced")
	}
	// The sides must not be crossed over: an ask carrying the bid's price
	// would manufacture a divergence signal out of a healthy book.
	if ask.Price != 65433.0 || ask.Size != 2.1 {
		t.Errorf("ask = %v @ %v, want 65433 @ 2.1", ask.Price, ask.Size)
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

	if got := p.toQuotes(ticks, fixedTime); len(got) != 3 {
		t.Fatalf("len(quotes) = %d, want 3 (two from BTCUSD, none from fUSD, one from ETHUSD)", len(got))
	}
}

// Venues that publish no per-tick timestamp leave ObservedAt zero. Passing
// that through would store a year-1 observation, so the fetch time stands in.
func TestToQuotesObservedAtFallsBackToFetchedAt(t *testing.T) {
	p := loadedPoller(t)

	ticks := []Tick{{Symbol: "BTCUSD", BidPrice: 65432.0, BidSize: 3.5}} // ObservedAt zero

	quotes := p.toQuotes(ticks, fixedTime)
	if len(quotes) != 1 {
		t.Fatalf("len(quotes) = %d, want 1", len(quotes))
	}
	if !quotes[0].ObservedAt.Equal(fixedTime) {
		t.Errorf("ObservedAt = %v, want the fetch time %v", quotes[0].ObservedAt, fixedTime)
	}
}

// --- Run ---

// Run cannot poll without a venue, a URL, a parser, and a loader. Catching
// that up front turns a silent no-op poll loop into a startup failure.
func TestRunRequiresConfig(t *testing.T) {
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
			cfg := testConfig("http://example.invalid", staticLoader(testTable))
			c.strip(&cfg)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			if err := New(cfg).Run(ctx, make(chan quote.Quote, 1)); err == nil {
				t.Error("Run() = nil error with an incomplete config, want an error")
			}
		})
	}
}

// A loader failure at startup is permanent as far as Run is concerned:
// without a symbol table every tick would be dropped, so it returns rather
// than polling into the void.
func TestRunReturnsLoaderError(t *testing.T) {
	sentinel := errors.New("venue unreachable")
	p := testPoller("http://example.invalid", func(context.Context) (symbols.Table, error) {
		return nil, sentinel
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Run(ctx, make(chan quote.Quote, 1)); !errors.Is(err, sentinel) {
		t.Errorf("Run() = %v, want it to wrap %v", err, sentinel)
	}
}

// Run jitters its first poll to stagger the venues at startup. A shutdown
// arriving during that initial wait must abandon it rather than fetch once
// on the way out.
func TestRunReturnsIfCancelledBeforeTheFirstPoll(t *testing.T) {
	server := newScriptedServer(t, func(int32) (int, string) {
		return http.StatusOK, tickBody
	})

	cfg := testConfig(server.URL, staticLoader(testTable))
	cfg.Tuning.PollInterval = 5 * time.Second // the startup jitter to interrupt

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := New(cfg).Run(ctx, make(chan quote.Quote, 1)); err == nil {
		t.Error("Run() = nil error with a cancelled context, want one")
	}
	if got := server.requests.Load(); got != 0 {
		t.Errorf("server saw %d requests, want 0 -- Run should not fetch after cancellation", got)
	}
}

// It is a poller: polling once and stopping would look identical to every
// other test here, since they all assert on the first quote to arrive.
func TestRunPollsRepeatedly(t *testing.T) {
	server := newScriptedServer(t, func(int32) (int, string) {
		return http.StatusOK, tickBody
	})
	p := testPoller(server.URL, staticLoader(testTable))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Drained continuously so the poll loop is never blocked on the send.
	out := make(chan quote.Quote, 128)
	go func() {
		for range out {
		}
	}()
	go p.Run(ctx, out)

	waitForRequests(t, server, 3)
}

// A venue being briefly unreachable is normal operation, not a reason to give
// up on it for the life of the process.
func TestRunRecoversAfterTransientFailures(t *testing.T) {
	server := failThenServe(t, 3, tickBody)
	p := testPoller(server.URL, staticLoader(testTable))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan quote.Quote, 10)
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx, out) }()

	select {
	case q := <-out:
		if q.Market != "BTC-USD" {
			t.Errorf("got a quote for %q, want BTC-USD", q.Market)
		}
	case err := <-errCh:
		t.Fatalf("Run returned %v before delivering any quote", err)
	case <-time.After(3 * time.Second):
		t.Fatal("no quote arrived despite the server recovering after 3 failures")
	}

	if got := server.requests.Load(); got < 4 {
		t.Errorf("server saw %d requests, want at least 4 (3 failures + 1 success)", got)
	}
}

// A 200 whose body will not parse is the schema-drift case. It must be
// treated like an outage -- backed off and retried -- not swallowed as an
// empty tick list, which would look like a venue with no markets.
func TestRunRecoversAfterUnparseableBodies(t *testing.T) {
	server := newScriptedServer(t, func(n int32) (int, string) {
		if n <= 2 {
			return http.StatusOK, `{"unexpected":"shape"}`
		}
		return http.StatusOK, tickBody
	})
	p := testPoller(server.URL, staticLoader(testTable))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan quote.Quote, 10)
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx, out) }()

	select {
	case q := <-out:
		if q.Market != "BTC-USD" {
			t.Errorf("got a quote for %q, want BTC-USD", q.Market)
		}
	case err := <-errCh:
		t.Fatalf("Run returned %v instead of retrying past the unparseable bodies", err)
	case <-time.After(3 * time.Second):
		t.Fatal("no quote arrived after the body became parseable again")
	}
}

// Shutdown must not wait out a long backoff sleep.
func TestRunReturnsPromptlyOnCancellation(t *testing.T) {
	server := failThenServe(t, 1_000_000, "")

	cfg := testConfig(server.URL, staticLoader(testTable))
	cfg.Tuning.InitialBackoff = 5 * time.Second // deliberately long
	cfg.Tuning.MaxBackoff = 5 * time.Second
	p := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(ctx, make(chan quote.Quote, 10)) }()

	// One request means the fetch failed and the poller is now parked in
	// its backoff sleep -- a deterministic stand-in for a fixed wait.
	waitForRequests(t, server, 1)

	start := time.Now()
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("Run() = nil, want the context error")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("Run took %v to return during a 5s backoff sleep", elapsed)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Run never returned after cancellation")
	}
}

// The consumer can be slower than the venue. Run must abandon a blocked send
// on cancellation rather than holding the quote forever.
func TestRunStopsWhileBlockedSendingToAFullChannel(t *testing.T) {
	server := newScriptedServer(t, func(int32) (int, string) {
		return http.StatusOK, tickBody
	})
	p := testPoller(server.URL, staticLoader(testTable))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	// Unbuffered and never read: the first send blocks immediately.
	go func() { errCh <- p.Run(ctx, make(chan quote.Quote)) }()

	waitForRequests(t, server, 1)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("Run() = nil, want the context error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run never returned while blocked on a full output channel")
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
