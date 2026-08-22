package bitfinex

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

// fixedTime gives every test a deterministic ObservedAt/ReceivedAt to
// assert against, rather than depending on time.Now() at test-run time.
var fixedTime = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

func TestDecodeSimpleTradingPair(t *testing.T) {
	body := `[
		["tBTCUSD", 65432.0, 3.5, 65433.0, 2.1, 120.5, 0.0019, 65433.0, 15234.7, 66000.0, 64000.0]
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 2 {
		t.Fatalf("len(quotes) = %d, want 2 (one bid, one ask)", len(quotes))
	}

	bywant := map[string]quote.Quote{}
	for _, q := range quotes {
		bywant[q.Selection] = q
	}

	bid, ok := bywant["bid"]
	if !ok {
		t.Fatal("expected a bid quote")
	}
	if bid.Market != "BTC-USD" || bid.Venue != "bitfinex" || bid.Price != 65432.0 || bid.Size != 3.5 {
		t.Errorf("bid = %+v, unexpected fields", bid)
	}
	if !bid.ObservedAt.Equal(fixedTime) || !bid.ReceivedAt.Equal(fixedTime) {
		t.Errorf("bid timestamps = %v / %v, want both %v", bid.ObservedAt, bid.ReceivedAt, fixedTime)
	}

	ask, ok := bywant["ask"]
	if !ok {
		t.Fatal("expected an ask quote")
	}
	if ask.Price != 65433.0 || ask.Size != 2.1 {
		t.Errorf("ask = %+v, unexpected fields", ask)
	}
}

func TestDecodeColonSeparatedPair(t *testing.T) {
	body := `[
		["tDOGE:USD", 0.24, 5000.0, 0.241, 4800.0, 0.01, 0.04, 0.241, 900000.0, 0.25, 0.23]
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 2 {
		t.Fatalf("len(quotes) = %d, want 2", len(quotes))
	}
	if quotes[0].Market != "DOGE-USD" {
		t.Errorf("Market = %q, want %q", quotes[0].Market, "DOGE-USD")
	}
}

func TestDecodeUSTCollapsesToUSD(t *testing.T) {
	body := `[
		["tBTCUST", 65400.0, 3.0, 65401.0, 2.0, 100.0, 0.001, 65401.0, 10000.0, 66000.0, 64000.0]
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 2 || quotes[0].Market != "BTC-USD" {
		t.Fatalf("expected BTC-USD market from tBTCUST, got %+v", quotes)
	}
}

func TestDecodeFundingRowRejected(t *testing.T) {
	body := `[
		["fUSD", 0.00015, 0.00014, 2, 850000.0, 0.00016, 30, 620000.0, 0.00001, 0.07, 0.00015, 12000000.0, 0.00020, 0.00010, 45000000.0]
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 0 {
		t.Fatalf("expected funding row to produce zero quotes, got %+v", quotes)
	}
}

func TestDecodeZeroPriceSideDropped(t *testing.T) {
	body := `[
		["tETHUSD", 0, 0, 3456.7, 8.2, 12.0, 0.0035, 3456.7, 5000.0, 3500.0, 3400.0]
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

func TestDecodeOutOfScopeSymbolDropped(t *testing.T) {
	body := `[
		["tGERMANY40IXF0:USTF0", 100.0, 1.0, 101.0, 1.0, 0.0, 0.0, 100.5, 0.0, 105.0, 95.0]
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("out-of-scope symbol produced %d quotes: %+v", len(quotes), quotes)
}

func TestDecodeMixedBatch(t *testing.T) {
	body := `[
		["tBTCUSD", 65432.0, 3.5, 65433.0, 2.1, 120.5, 0.0019, 65433.0, 15234.7, 66000.0, 64000.0],
		["fUSD", 0.00015, 0.00014, 2, 850000.0, 0.00016, 30, 620000.0, 0.00001, 0.07, 0.00015, 12000000.0, 0.00020, 0.00010, 45000000.0],
		["tETHUSD", 0, 0, 3456.7, 8.2, 12.0, 0.0035, 3456.7, 5000.0, 3500.0, 3400.0]
	]`

	quotes, err := decode([]byte(body), fixedTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(quotes) != 3 {
		t.Fatalf("len(quotes) = %d, want 3", len(quotes))
	}
}

func TestFetchReturnsBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[["tBTCUSD", 100.0, 1.0, 101.0, 1.0]]`))
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
	fs := newFlakyServer(3, `[["tBTCUSD", 100.0, 1.0, 101.0, 1.0]]`)
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
	a.initialBackoff = 5 * time.Second // deliberately long
	a.maxBackoff = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan quote.Quote, 10)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Run(ctx, out)
	}()

	time.Sleep(50 * time.Millisecond) // let it get into its first backoff sleep
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
