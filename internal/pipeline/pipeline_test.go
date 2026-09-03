package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samuel-fonseca/driftwatch/internal/divergence"
	"github.com/samuel-fonseca/driftwatch/internal/hub"
	"github.com/samuel-fonseca/driftwatch/internal/hub/hubtest"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/quotetest"
	"github.com/samuel-fonseca/driftwatch/internal/source"
	"github.com/samuel-fonseca/driftwatch/internal/store/ndjson"
)

const wait = 5 * time.Second

// --- fakes ---

// fakeSource emits a fixed set of quotes and then stays running, as a real
// venue poller does, until the context is cancelled.
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
	<-ctx.Done()
	return ctx.Err()
}

// failingSource returns immediately with a permanent error, the way a source
// does when its config is unusable.
type failingSource struct{ name string }

func (f *failingSource) Name() string { return f.name }

func (f *failingSource) Run(context.Context, chan<- quote.Quote) error {
	return errors.New("permanent source failure")
}

// recordingStore captures the batches the workers write. err, when set, makes
// every write fail.
type recordingStore struct {
	mu      sync.Mutex
	quotes  []quote.Quote
	batches int
	err     error
}

func (s *recordingStore) WriteBatch(_ context.Context, batch []quote.Quote) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return s.err
	}
	s.quotes = append(s.quotes, batch...)
	s.batches++
	return nil
}

func (s *recordingStore) Close() error { return nil }

func (s *recordingStore) written() []quote.Quote {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]quote.Quote(nil), s.quotes...)
}

// --- harness ---

// harness wires a Pipeline to a private metrics registry so any number of
// them can exist in one test binary, and ties the run's lifetime to the test.
type harness struct {
	t     *testing.T
	p     *Pipeline
	hub   *hub.Hub
	store *recordingStore

	cancel context.CancelFunc
	done   chan error
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()

	h := &harness{t: t, hub: hub.New(), store: &recordingStore{}}

	cfg.Registry = prometheus.NewPedanticRegistry()
	if cfg.Hub == nil {
		cfg.Hub = h.hub
	}
	if cfg.Store == nil {
		cfg.Store = h.store
	}
	// Short enough that a test never waits on a real venue's timings.
	if cfg.StaleThreshold == 0 {
		cfg.StaleThreshold = 2 * time.Second
	}

	h.hub = cfg.Hub
	h.p = New(cfg)
	return h
}

// run starts the pipeline and stops it when the test ends.
func (h *harness) run() {
	h.t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan error, 1)

	go func() { h.done <- h.p.Run(ctx) }()
	h.t.Cleanup(func() { h.stop() })
}

// stop cancels the run and waits for Run to return. Safe to call twice.
func (h *harness) stop() error {
	h.t.Helper()

	if h.done == nil {
		return nil
	}
	h.cancel()
	select {
	case err := <-h.done:
		h.done = nil
		return err
	case <-time.After(wait):
		h.t.Fatal("Run did not return within the deadline after cancellation")
		return nil
	}
}

// subscribe attaches an SSE client to the harness's hub.
func (h *harness) subscribe() *hubtest.Stream {
	h.t.Helper()

	server := httptest.NewServer(h.hub)
	h.t.Cleanup(server.Close)

	stream := hubtest.Subscribe(h.t, server.URL)
	waitFor(h.t, func() bool { return h.hub.Stats().Subscribers == 1 },
		"the SSE subscriber to register")
	return stream
}

// waitFor polls until cond holds, failing the test with what it was waiting
// for. Polling a predicate keeps these tests off fixed sleeps.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// crossedVenues is a pair of sources whose books cross: venueA bids 101 while
// venueB offers at 100, an edge of about 99 bps.
func crossedVenues() []source.Source {
	return []source.Source{
		&fakeSource{name: "venueA", quotes: []quote.Quote{quotetest.Bid("venueA", "BTC-USD", 101)}},
		&fakeSource{name: "venueB", quotes: []quote.Quote{quotetest.Ask("venueB", "BTC-USD", 100)}},
	}
}

// --- config ---

func TestApplyDefaults(t *testing.T) {
	var cfg Config
	cfg.ApplyDefaults()

	cases := []struct {
		name      string
		got, want any
	}{
		{"BufferCapacity", cfg.BufferCapacity, 16384},
		{"DedupeCapacity", cfg.DedupeCapacity, 16384},
		{"EdgeThresholdBps", cfg.EdgeThresholdBps, 5.0},
		{"CollisionRatio", cfg.CollisionRatio, 4.0},
		{"StaleThreshold", cfg.StaleThreshold, 30 * time.Second},
		{"NumWorkers", cfg.NumWorkers, 4},
		{"BatchSize", cfg.BatchSize, 256},
		{"RawChannelSize", cfg.RawChannelSize, 4096},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	if cfg.Registry == nil {
		t.Error("Registry = nil, want it defaulted to the process registry")
	}
}

func TestApplyDefaultsKeepsExplicitValues(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	cfg := Config{
		BufferCapacity:   1,
		DedupeCapacity:   2,
		EdgeThresholdBps: 3,
		CollisionRatio:   4,
		StaleThreshold:   5 * time.Second,
		NumWorkers:       6,
		BatchSize:        7,
		RawChannelSize:   8,
		Registry:         registry,
	}
	want := cfg
	cfg.ApplyDefaults()

	if cfg.BufferCapacity != want.BufferCapacity || cfg.NumWorkers != want.NumWorkers ||
		cfg.BatchSize != want.BatchSize || cfg.RawChannelSize != want.RawChannelSize ||
		cfg.StaleThreshold != want.StaleThreshold || cfg.EdgeThresholdBps != want.EdgeThresholdBps ||
		cfg.CollisionRatio != want.CollisionRatio || cfg.DedupeCapacity != want.DedupeCapacity {
		t.Errorf("ApplyDefaults overwrote explicit values: got %+v, want %+v", cfg, want)
	}
	if cfg.Registry != prometheus.Registerer(registry) {
		t.Error("ApplyDefaults replaced the caller's registry")
	}
}

// Each Pipeline registers a collector, and a registry rejects a duplicate.
// Taking the registry from Config is what lets a test build more than one.
func TestPipelinesCanCoexistOnSeparateRegistries(t *testing.T) {
	for range 2 {
		newHarness(t, Config{})
	}
}

// --- behaviour ---

func TestPipelinePublishesSignalToSubscribers(t *testing.T) {
	h := newHarness(t, Config{Sources: crossedVenues(), EdgeThresholdBps: 5})
	stream := h.subscribe()
	h.run()

	frame := stream.NextEvent(wait)
	if frame.Event != "signal" {
		t.Errorf("event name = %q, want %q", frame.Event, "signal")
	}

	var sig divergence.Signal
	if err := json.Unmarshal([]byte(frame.Data), &sig); err != nil {
		t.Fatalf("decoding signal %q: %v", frame.Data, err)
	}
	if sig.Market != "BTC-USD" {
		t.Errorf("Market = %q, want %q", sig.Market, "BTC-USD")
	}
	if sig.BidVenue != "venueA" || sig.AskVenue != "venueB" {
		t.Errorf("legs = bid:%s ask:%s, want bid:venueA ask:venueB", sig.BidVenue, sig.AskVenue)
	}
}

func TestPipelineWritesQuotesToStore(t *testing.T) {
	h := newHarness(t, Config{Sources: crossedVenues()})
	h.run()

	waitFor(t, func() bool { return len(h.store.written()) == 2 }, "both quotes to reach the store")

	seen := map[string]bool{}
	for _, q := range h.store.written() {
		seen[q.Key()] = true
	}
	for _, key := range []string{"venueA|BTC-USD|bid", "venueB|BTC-USD|ask"} {
		if !seen[key] {
			t.Errorf("store never received %q, got %v", key, seen)
		}
	}
}

// The dedupe stage sits between the buffer and the store, so a venue
// repeating an unchanged price must not produce a second row.
func TestPipelineDedupesBeforeWriting(t *testing.T) {
	repeated := quotetest.Bid("venueA", "BTC-USD", 101)
	src := &fakeSource{name: "venueA", quotes: []quote.Quote{repeated, repeated, repeated}}

	h := newHarness(t, Config{Sources: []source.Source{src}})
	h.run()

	waitFor(t, func() bool { return len(h.store.written()) >= 1 }, "the first quote to be stored")

	// Give any duplicate a generous chance to show up before concluding it
	// was suppressed.
	time.Sleep(200 * time.Millisecond)

	if got := h.store.written(); len(got) != 1 {
		t.Errorf("store received %d quotes, want 1 -- the repeats should be deduped: %+v", len(got), got)
	}
}

// A store outage must not stall the pipeline: the write is logged and the
// worker carries on, so signals keep flowing to subscribers.
func TestPipelineSurvivesStoreFailures(t *testing.T) {
	h := newHarness(t, Config{Sources: crossedVenues(), EdgeThresholdBps: 5})
	h.store.err = errors.New("store is down")

	stream := h.subscribe()
	h.run()

	if frame := stream.NextEvent(wait); frame.Event != "signal" {
		t.Errorf("event name = %q, want a signal despite the store failing", frame.Event)
	}
}

// One misconfigured venue must not take the others down with it.
func TestPipelineSurvivesSourceFailure(t *testing.T) {
	sources := append(crossedVenues(), &failingSource{name: "brokenVenue"})

	h := newHarness(t, Config{Sources: sources, EdgeThresholdBps: 5})
	stream := h.subscribe()
	h.run()

	if frame := stream.NextEvent(wait); frame.Event != "signal" {
		t.Errorf("event name = %q, want the healthy venues to keep working", frame.Event)
	}
}

// Run owns the workers' lifetime, so it must not return while one could still
// be writing -- main closes the store as soon as it does.
func TestRunBlocksUntilCancelledThenReturns(t *testing.T) {
	h := newHarness(t, Config{Sources: crossedVenues()})
	h.run()

	select {
	case err := <-h.done:
		t.Fatalf("Run returned %v while the context was still live", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := h.stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Run() = %v, want %v", err, context.Canceled)
	}
}

// Run must return even with no sources at all, or shutdown would hang.
func TestRunWithNoSourcesStillStopsCleanly(t *testing.T) {
	h := newHarness(t, Config{})
	h.run()

	if err := h.stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Run() = %v, want %v", err, context.Canceled)
	}
}

func TestStatsReportsBufferHubAndCollisions(t *testing.T) {
	h := newHarness(t, Config{Sources: crossedVenues()})
	stream := h.subscribe()
	h.run()

	stream.NextEvent(wait)
	waitFor(t, func() bool { return h.p.Stats().Buffer.Pushed > 0 }, "the buffer to record a push")

	got := h.p.Stats()
	if got.Buffer.Capacity != 16384 {
		t.Errorf("Buffer.Capacity = %d, want the default 16384", got.Buffer.Capacity)
	}
	if got.Buffer.Taken == 0 {
		t.Error("Buffer.Taken = 0, want the workers' takes counted")
	}
	if got.Hub.Subscribers != 1 {
		t.Errorf("Hub.Subscribers = %d, want 1", got.Hub.Subscribers)
	}
	if got.Hub.Published == 0 {
		t.Error("Hub.Published = 0, want the emitted signal counted")
	}
	if got.CollidedMarkets == nil {
		t.Error("CollidedMarkets = nil, want an empty slice so /stats renders []")
	}
}

// Stats is serialised straight onto the /stats endpoint.
func TestStatsMarshalsToJSON(t *testing.T) {
	h := newHarness(t, Config{})

	data, err := json.Marshal(h.p.Stats())
	if err != nil {
		t.Fatalf("marshalling Stats: %v", err)
	}
	for _, key := range []string{`"buffer"`, `"hub"`, `"collided_markets"`} {
		if !strings.Contains(string(data), key) {
			t.Errorf("Stats JSON %s is missing %s", data, key)
		}
	}
}

// --- integration ---

// The same path as the focused tests above, but against the real ndjson store
// on disk, so the batching and the file format are exercised together.
func TestPipelineEndToEndWithNDJSONStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quotes.ndjson")
	st, err := ndjson.Open(path)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}

	h := newHarness(t, Config{Sources: crossedVenues(), Store: st, EdgeThresholdBps: 5})
	stream := h.subscribe()
	h.run()

	stream.NextEvent(wait) // a signal proves both quotes made it through

	if err := h.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want %v", err, context.Canceled)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("closing store: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading store file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("store holds %d lines, want 2 (one bid, one ask): %q", len(lines), data)
	}
	for i, line := range lines {
		var q quote.Quote
		if err := json.Unmarshal([]byte(line), &q); err != nil {
			t.Errorf("line %d is not valid JSON: %v (%q)", i, err, line)
		}
	}
}
