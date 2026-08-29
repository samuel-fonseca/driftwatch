package poller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGetReturnsBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"symbol":"BTCUSDT","bidPrice":"100.0"}]`))
	}))
	defer server.Close()

	h := NewHTTP("test", 10*time.Second)

	body, err := h.Get(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(body) == 0 {
		t.Error("expected a non-empty body")
	}
}

func TestGetNonOKStatusIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	h := NewHTTP("test", 10*time.Second)

	if _, err := h.Get(context.Background(), server.URL); err == nil {
		t.Error("expected an error for a non-200 status, got nil")
	}
}

func TestGetRespectsCancelledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h := NewHTTP("test", 10*time.Second)

	if _, err := h.Get(ctx, server.URL); err == nil {
		t.Error("Get() = nil error with a cancelled context, want an error")
	}
}

func TestGetUnreachableHostIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := server.URL
	server.Close() // nothing is listening now

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	h := NewHTTP("test", 10*time.Second)

	if _, err := h.Get(ctx, url); err == nil {
		t.Error("Get() = nil error against a closed server, want an error")
	}
}
