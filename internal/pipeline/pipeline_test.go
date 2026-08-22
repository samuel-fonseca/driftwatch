package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/divergence"
	"github.com/samuel-fonseca/driftwatch/internal/hub"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/source"
	"github.com/samuel-fonseca/driftwatch/internal/store/ndjson"
)

type fakeSource struct {
	name   string
	quotes []quote.Quote
}

func (f *fakeSource) Name() string { return f.name }

func (f *fakeSource) Run(ctx context.Context, out chan<- quote.Quote) error {
	for _, q := range f.quotes {
		select {
		case out <- q:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done() // stay "running" (like a real source would) until told to stop
	return ctx.Err()
}

func TestPipelineEndToEnd(t *testing.T) {
	now := time.Now()

	venueA := &fakeSource{
		name: "venueA",
		quotes: []quote.Quote{
			{Venue: "venueA", Market: "BTC-USD", Selection: "bid", Price: 101, Size: 1, ObservedAt: now, ReceivedAt: now},
		},
	}
	venueB := &fakeSource{
		name: "venueB",
		quotes: []quote.Quote{
			{Venue: "venueB", Market: "BTC-USD", Selection: "ask", Price: 100, Size: 1, ObservedAt: now, ReceivedAt: now},
		},
	}

	dbPath := filepath.Join(t.TempDir(), "quotes.ndjson")
	st, err := ndjson.Open(dbPath)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}

	h := hub.New()

	p := New(Config{
		Sources:          []source.Source{venueA, venueB},
		Store:            st,
		Hub:              h,
		EdgeThresholdBps: 5,
		StaleThreshold:   2 * time.Second,
	})

	streamServer := httptest.NewServer(h)
	defer streamServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, streamServer.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("subscribing to stream: %v", err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	// consume the preamble line
	reader.ReadString('\n')

	runCtx, runCancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		p.Run(runCtx)
	}()

	// Poll the stream for a divergence signal.
	var sig divergence.Signal
	found := false
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			continue
		}
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimPrefix(line, "data: ")
			if json.Unmarshal([]byte(payload), &sig) == nil && sig.Market == "BTC-USD" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatal("no divergence signal arrived on the SSE stream within the deadline")
	}
	if sig.BidVenue != "venueA" || sig.AskVenue != "venueB" {
		t.Errorf("signal legs = bid:%s ask:%s, want bid:venueA ask:venueB", sig.BidVenue, sig.AskVenue)
	}

	runCancel()
	<-runDone
	st.Close()

	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("reading store file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("store file has %d lines, want 2 (one bid, one ask)", len(lines))
	}
}
