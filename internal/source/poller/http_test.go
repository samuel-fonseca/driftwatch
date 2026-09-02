package poller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newHTTP() *HTTP { return NewHTTP("test", 10*time.Second) }

func TestNewHTTPAppliesTimeout(t *testing.T) {
	h := NewHTTP("binance", 3*time.Second)

	if h.Venue != "binance" {
		t.Errorf("Venue = %q, want %q", h.Venue, "binance")
	}
	// A client with no timeout waits forever on a venue that accepts the
	// connection and then goes quiet, wedging the poll loop for good.
	if h.Client.Timeout != 3*time.Second {
		t.Errorf("Client.Timeout = %v, want 3s", h.Client.Timeout)
	}
}

func TestGetReturnsBodyVerbatim(t *testing.T) {
	const body = `[{"symbol":"BTCUSDT","bidPrice":"100.0"}]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want %q", r.Method, http.MethodGet)
		}
		w.Write([]byte(body))
	}))
	defer server.Close()

	got, err := newHTTP().Get(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("Get() = %v, want nil", err)
	}
	// The parsers downstream are byte-exact, so Get must not trim or
	// re-encode anything.
	if string(got) != body {
		t.Errorf("Get() = %q, want %q", got, body)
	}
}

func TestGetErrors(t *testing.T) {
	cases := []struct {
		name string
		// url returns the target and any context to use for the request.
		setup func(t *testing.T) (url string, ctx context.Context)
		why   string
	}{
		{
			name: "server error status",
			setup: func(t *testing.T) (string, context.Context) {
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusServiceUnavailable)
				}))
				t.Cleanup(s.Close)
				return s.URL, context.Background()
			},
			why: "a 503 body is not tickers, and parsing it would be worse than backing off",
		},
		{
			name: "rate limited",
			setup: func(t *testing.T) (string, context.Context) {
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusTooManyRequests)
				}))
				t.Cleanup(s.Close)
				return s.URL, context.Background()
			},
			why: "a 429 must reach the caller so it backs off rather than hammering",
		},
		{
			name: "cancelled context",
			setup: func(t *testing.T) (string, context.Context) {
				s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
				t.Cleanup(s.Close)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return s.URL, ctx
			},
			why: "shutdown must abandon an in-flight fetch",
		},
		{
			name: "nothing listening",
			setup: func(t *testing.T) (string, context.Context) {
				s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
				url := s.URL
				s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				t.Cleanup(cancel)
				return url, ctx
			},
			why: "a venue that is simply down must surface as an error",
		},
		{
			name: "malformed url",
			setup: func(t *testing.T) (string, context.Context) {
				return "://not-a-url", context.Background()
			},
			why: "a bad URL is a permanent config fault, not a transient one",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			url, ctx := c.setup(t)

			if _, err := newHTTP().Get(ctx, url); err == nil {
				t.Errorf("Get() = nil error, want one -- %s", c.why)
			}
		})
	}
}

// The venue name is the only thing distinguishing one poller's log line from
// another's when several are backing off at once.
func TestGetStatusErrorNamesTheVenue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	_, err := NewHTTP("kraken", time.Second).Get(context.Background(), server.URL)
	if err == nil {
		t.Fatal("Get() = nil error, want one")
	}
	if !strings.Contains(err.Error(), "kraken") {
		t.Errorf("error %q does not name the venue", err)
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error %q does not report the status code", err)
	}
}

// A venue that accepts the connection then stalls must not hold the poll loop
// open past the configured timeout.
func TestGetRespectsClientTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)

	start := time.Now()
	if _, err := NewHTTP("test", 50*time.Millisecond).Get(context.Background(), server.URL); err == nil {
		t.Error("Get() = nil error against a stalled server, want a timeout")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Get took %v to time out, want it near the 50ms limit", elapsed)
	}
}
