package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

var fixedTime = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

// --- decode tests ---
// Note the format difference from Bitfinex throughout: Binance's numeric
// fields are JSON STRINGS ("65432.10000000"), not JSON numbers, and the
// payload is a homogeneous array of objects, not positional arrays --
// so there's no funding-row-shaped trap here, but there IS a
// string-parsing failure mode Bitfinex's decode never had to handle.

func TestDecodeSimpleTradingPair(t *testing.T) {
	body := `[
		{"symbol":"BTCUSDT","bidPrice":"65432.10000000","bidQty":"3.50000000","askPrice":"65433.00000000","askQty":"2.10000000"}
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 2 {
		t.Fatalf("len(quotes) = %d, want 2 (one bid, one ask)", len(quotes))
	}

	byWant := map[string]quote.Quote{}
	for _, q := range quotes {
		byWant[q.Selection] = q
	}

	bid, ok := byWant["bid"]
	if !ok {
		t.Fatal("expected a bid quote")
	}
	if bid.Market != "BTC-USD" || bid.Venue != "binance" || bid.Price != 65432.10 || bid.Size != 3.5 {
		t.Errorf("bid = %+v, unexpected fields", bid)
	}
	if !bid.ObservedAt.Equal(fixedTime) || !bid.ReceivedAt.Equal(fixedTime) {
		t.Errorf("bid timestamps = %v / %v, want both %v", bid.ObservedAt, bid.ReceivedAt, fixedTime)
	}

	ask, ok := byWant["ask"]
	if !ok {
		t.Fatal("expected an ask quote")
	}
	if ask.Price != 65433.00 || ask.Size != 2.1 {
		t.Errorf("ask = %+v, unexpected fields", ask)
	}
}

// TestDecodeLongestMatchFirst: regression coverage for the exact trap
// the PRD calls out for Binance specifically -- "USDT" must be matched
// before the shorter "USD"/"UST" suffixes, or "ETHUSDT" gets split
// wrong. This exercises it through the REAL decode path, not just
// normalize in isolation.
func TestDecodeLongestMatchFirst(t *testing.T) {
	body := `[
		{"symbol":"ETHUSDT","bidPrice":"3200.50000000","bidQty":"10.00000000","askPrice":"3201.00000000","askQty":"8.00000000"}
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 2 || quotes[0].Market != "ETH-USD" {
		t.Fatalf("expected ETH-USD market from ETHUSDT, got %+v", quotes)
	}
}

// TestDecodeZeroPriceSideDropped: same PRD 7.1 rule as Bitfinex -- a
// zero price means no resting order on that side, not a price of zero.
func TestDecodeZeroPriceSideDropped(t *testing.T) {
	body := `[
		{"symbol":"ETHUSDT","bidPrice":"0.00000000","bidQty":"0.00000000","askPrice":"3201.00000000","askQty":"8.00000000"}
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 1 {
		t.Fatalf("len(quotes) = %d, want 1 (bid side is zero, ask side is not)", len(quotes))
	}
	if quotes[0].Selection != "ask" {
		t.Errorf("survivor Selection = %q, want %q", quotes[0].Selection, "ask")
	}
}

// TestDecodeMalformedNumberSkipsRow: Binance's numbers are JSON strings,
// which means a NEW failure mode Bitfinex's decode never had --
// strconv.ParseFloat can fail on a malformed string in a way a JSON
// number type assertion never could. "Skip, never guess" applies here
// too: don't panic, don't treat it as zero, just drop the row.
func TestDecodeMalformedNumberSkipsRow(t *testing.T) {
	body := `[
		{"symbol":"BTCUSDT","bidPrice":"not-a-number","bidQty":"3.5","askPrice":"65433.00","askQty":"2.1"}
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 0 {
		t.Fatalf("expected malformed row to be skipped entirely, got %+v", quotes)
	}
}

// TestDecodeUnnormalizableSymbolDropped: same normalize.Normalize
// integration as Bitfinex's decode -- a symbol with no known quote
// asset suffix should be dropped, not guessed at.
func TestDecodeUnnormalizableSymbolDropped(t *testing.T) {
	body := `[
		{"symbol":"BTCXYZ","bidPrice":"100.0","bidQty":"1.0","askPrice":"101.0","askQty":"1.0"}
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 0 {
		t.Fatalf("expected unnormalizable symbol to be dropped, got %+v", quotes)
	}
}

func TestDecodeMixedBatch(t *testing.T) {
	body := `[
		{"symbol":"BTCUSDT","bidPrice":"65432.10","bidQty":"3.5","askPrice":"65433.00","askQty":"2.1"},
		{"symbol":"ETHUSDT","bidPrice":"0.0","bidQty":"0.0","askPrice":"3201.00","askQty":"8.0"},
		{"symbol":"BTCXYZ","bidPrice":"100.0","bidQty":"1.0","askPrice":"101.0","askQty":"1.0"},
		{"symbol":"SOLUSDT","bidPrice":"not-a-number","bidQty":"1.0","askPrice":"150.0","askQty":"1.0"}
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// BTCUSDT -> 2, ETHUSDT -> 1 (zero bid dropped), BTCXYZ -> 0
	// (unnormalizable), SOLUSDT -> 0 (malformed) = 3 total.
	if len(quotes) != 3 {
		t.Fatalf("len(quotes) = %d, want 3", len(quotes))
	}
}

func TestDecodeEmptyArray(t *testing.T) {
	quotes, err := decode([]byte(`[]`), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 0 {
		t.Fatalf("expected 0 quotes for an empty array, got %+v", quotes)
	}
}

// --- fetch tests ---

func TestFetchReturnsBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"symbol":"BTCUSDT","bidPrice":"100.0","bidQty":"1.0","askPrice":"101.0","askQty":"1.0"}]`))
	}))
	defer server.Close()

	a := New()
	a.baseURL = server.URL

	body, err := a.fetch(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(body) == 0 {
		t.Error("expected a non-empty body")
	}
}

func TestFetchNonOKStatusIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	a := New()
	a.baseURL = server.URL

	_, err := a.fetch(context.Background())
	if err == nil {
		t.Error("expected an error for a non-200 status, got nil")
	}
}

// --- Run tests ---

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

func TestRunRecoversAfterTransientFailures(t *testing.T) {
	fs := newFlakyServer(3, `[{"symbol":"BTCUSDT","bidPrice":"100.0","bidQty":"1.0","askPrice":"101.0","askQty":"1.0"}]`)
	defer fs.Close()

	a := New()
	a.baseURL = fs.URL
	a.pollInterval = 50 * time.Millisecond
	a.initialBackoff = 10 * time.Millisecond
	a.maxBackoff = 100 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out := make(chan quote.Quote, 10)
	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run(ctx, out)
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

	a := New()
	a.baseURL = fs.URL
	a.initialBackoff = 5 * time.Second
	a.maxBackoff = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan quote.Quote, 10)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run(ctx, out)
	}()

	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected a non-nil error (ctx.Err()) on cancellation")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("Run took %v to return -- too slow", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run never returned after context cancellation")
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

func BenchmarkDecode(b *testing.B) {
	data := buildLargeFixture(1000)
	fixedTime := time.Now()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		decode(data, fixedTime)
	}
}
