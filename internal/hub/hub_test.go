package hub

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestServeHTTPDeliversEvents(t *testing.T) {
	h := New()
	server := httptest.NewServer(h)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	// First line should be the retry preamble.
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "retry:") {
		t.Fatalf("first line = %q, err=%v, want a retry: preamble", line, err)
	}

	// Give ServeHTTP a moment to actually register the subscriber
	// before we publish -- otherwise we might publish before anyone's
	// listening.
	time.Sleep(50 * time.Millisecond)

	h.Publish(Event{Name: "signal", Data: []byte(`{"market":"BTC-USD"}`)})

	// Scan forward until we see our event's data line, or time out.
	found := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, `"market":"BTC-USD"`) {
			found = true
			break
		}
	}
	if !found {
		t.Error("published event never appeared in the SSE stream")
	}
}

func TestWedgedSubscriberNeverBlocksPublish(t *testing.T) {
	h := New()
	h.subscribe()

	const publishCount = subscriberBufferSize * 4

	h.mu.Lock()
	healthyID := h.nextID
	h.nextID++
	healthy := &subscriber{
		ch:   make(chan Event, publishCount),
		kill: make(chan struct{}),
	}
	h.subscribers[healthyID] = healthy
	h.mu.Unlock()

	start := time.Now()
	for range publishCount {
		h.Publish(Event{Data: []byte("tick")})
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Errorf("publishing %d events took %v -- Publish should never block on a wedge subscriber", publishCount, elapsed)
	}

	if got := len(healthy.ch); got != publishCount {
		t.Errorf("healthy subscriber received %d events, want %d", got, publishCount)
	}

	stats := h.Stats()
	if stats.Dropped == 0 {
		t.Errorf("expected dropped events, got 0")
	}
}
