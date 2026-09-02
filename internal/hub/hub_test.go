package hub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/hub/hubtest"
)

const wait = 2 * time.Second

// --- test helpers ---

// serve starts an httptest server for h, shut down with the test.
func serve(t *testing.T, h *Hub) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	return server
}

// waitForSubscribers blocks until the hub reports n subscribers. ServeHTTP
// registers a subscriber only after writing the preamble, so publishing the
// instant a client connects can race the registration -- this replaces a
// sleep that merely made the race unlikely.
func waitForSubscribers(t *testing.T, h *Hub, n int) {
	t.Helper()

	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if got := h.Stats().Subscribers; got == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("hub has %d subscribers, want %d", h.Stats().Subscribers, n)
}

// nonFlusher hides the Flush method an httptest recorder would otherwise
// provide. Embedding the interface rather than the concrete recorder means
// only ResponseWriter's methods are promoted.
type nonFlusher struct{ http.ResponseWriter }

// --- ServeHTTP ---

func TestServeHTTPSetsStreamingHeaders(t *testing.T) {
	server := serve(t, New())
	stream := hubtest.Subscribe(t, server.URL)

	cases := []struct{ header, want string }{
		{"Content-Type", "text/event-stream"},
		{"Cache-Control", "no-cache"},
		{"Connection", "keep-alive"},
		// Without this, an nginx in front of the app buffers the stream and
		// events arrive in bursts minutes late, or not at all.
		{"X-Accel-Buffering", "no"},
	}
	for _, c := range cases {
		if got := stream.Resp.Header.Get(c.header); got != c.want {
			t.Errorf("%s = %q, want %q", c.header, got, c.want)
		}
	}
}

// The preamble tells a reconnecting browser how long to wait and flushes
// immediately, so the client sees an open stream before the first event.
func TestServeHTTPSendsPreamble(t *testing.T) {
	server := serve(t, New())
	stream := hubtest.Subscribe(t, server.URL)

	frame := stream.Next(wait)
	if frame.Retry != "2000" {
		t.Errorf("retry = %q, want %q", frame.Retry, "2000")
	}
	if len(frame.Comments) != 1 || frame.Comments[0] != "connected" {
		t.Errorf("comments = %v, want [connected]", frame.Comments)
	}
}

func TestServeHTTPDeliversPublishedEvents(t *testing.T) {
	h := New()
	server := serve(t, h)
	stream := hubtest.Subscribe(t, server.URL)
	waitForSubscribers(t, h, 1)

	h.Publish(Event{Name: "signal", Data: []byte(`{"market":"BTC-USD"}`)})

	frame := stream.NextEvent(wait)
	if frame.Event != "signal" {
		t.Errorf("event = %q, want %q", frame.Event, "signal")
	}
	if frame.Data != `{"market":"BTC-USD"}` {
		t.Errorf("data = %q, want the published payload", frame.Data)
	}
}

// An unnamed event must not emit an empty "event:" line -- a browser would
// then dispatch it under the empty name rather than as a default message.
func TestServeHTTPOmitsEventLineWhenUnnamed(t *testing.T) {
	h := New()
	server := serve(t, h)
	stream := hubtest.Subscribe(t, server.URL)
	waitForSubscribers(t, h, 1)

	h.Publish(Event{Data: []byte("bare")})

	frame := stream.NextEvent(wait)
	if frame.Event != "" {
		t.Errorf("event = %q, want it absent", frame.Event)
	}
	if frame.Data != "bare" {
		t.Errorf("data = %q, want %q", frame.Data, "bare")
	}
}

// Heartbeats keep idle connections from being reaped by proxies. The interval
// is a field precisely so this does not take 15 seconds to observe.
func TestServeHTTPSendsHeartbeats(t *testing.T) {
	h := New()
	h.heartbeat = 10 * time.Millisecond
	server := serve(t, h)
	stream := hubtest.Subscribe(t, server.URL)

	stream.Next(wait) // preamble

	for range 2 {
		frame := stream.Next(wait)
		if len(frame.Comments) != 1 || frame.Comments[0] != "heartbeat" {
			t.Fatalf("frame = %+v, want a heartbeat comment", frame)
		}
	}
}

// SSE is meaningless over a writer that cannot flush: the events would sit in
// a buffer indefinitely, so say so rather than accept the connection.
func TestServeHTTPRejectsNonFlushingWriter(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)

	New().ServeHTTP(nonFlusher{rec}, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

// A closed browser tab must not leave its subscriber behind: Publish walks
// every subscriber, so leaked ones cost work on every event forever.
func TestClientDisconnectUnsubscribes(t *testing.T) {
	h := New()
	server := serve(t, h)

	stream := hubtest.Subscribe(t, server.URL)
	waitForSubscribers(t, h, 1)

	stream.Close()
	waitForSubscribers(t, h, 0)
}

// blockingWriter serves the SSE preamble and then stalls, standing in for a
// client whose socket has stopped draining. A recorder cannot do this: it
// accepts writes instantly, which makes ServeHTTP a perfectly healthy reader
// that can never fall behind.
type blockingWriter struct {
	header http.Header

	mu      sync.Mutex
	writes  int
	blocked chan struct{} // closed once a write has actually stalled
	release chan struct{}
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		header:  http.Header{},
		blocked: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *blockingWriter) Header() http.Header { return w.header }
func (w *blockingWriter) WriteHeader(int)     {}
func (w *blockingWriter) Flush()              {}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	// The preamble is two writes; stall on everything after it, so the
	// subscriber is registered before it wedges.
	stall := w.writes > 2
	first := w.writes == 3
	w.mu.Unlock()

	if stall {
		if first {
			close(w.blocked)
		}
		<-w.release
	}
	return len(p), nil
}

// Evicting a hopeless subscriber from the map is only half the job: without
// the kill channel its ServeHTTP would stay parked forever, holding the
// connection and the goroutine that a full drop budget just proved are dead.
func TestEvictedSubscribersHandlerReturns(t *testing.T) {
	h := New()
	w := newBlockingWriter()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)

	served := make(chan struct{})
	go func() {
		defer close(served)
		h.ServeHTTP(w, req)
	}()

	waitForSubscribers(t, h, 1)

	// One event gets picked up and stalls in Write; everything after it
	// backs up in the subscriber's channel and then starts dropping.
	for range subscriberBufferSize + dropBudget + 2 {
		h.Publish(Event{Data: []byte("tick")})
	}

	select {
	case <-w.blocked:
	case <-time.After(wait):
		t.Fatal("the writer never stalled, so the subscriber was never wedged")
	}

	if got := h.Stats().Evicted; got != 1 {
		t.Fatalf("Evicted = %d, want 1", got)
	}

	// Let the stalled write finish; ServeHTTP should then see the kill
	// signal on its next trip round the select and return.
	close(w.release)

	select {
	case <-served:
	case <-time.After(wait):
		t.Error("ServeHTTP never returned after its subscriber was evicted -- the connection leaks")
	}
}

// --- Publish ---

func TestPublishWithNoSubscribersStillCounts(t *testing.T) {
	h := New()
	h.Publish(Event{Data: []byte("tick")})

	got := h.Stats()
	if got.Published != 1 {
		t.Errorf("Published = %d, want 1", got.Published)
	}
	if got.Dropped != 0 {
		t.Errorf("Dropped = %d, want 0 -- nobody was listening to drop for", got.Dropped)
	}
}

// The whole point of the buffered channel and the default case: one stuck
// client must never stall the pipeline worker that calls Publish, nor cost
// the healthy subscribers their events.
func TestWedgedSubscriberNeitherBlocksNorStarvesOthers(t *testing.T) {
	h := New()

	const publishCount = subscriberBufferSize * 4

	h.subscribe()                                     // wedged: never read
	_, healthy := h.subscribeWithBuffer(publishCount) // cannot be made to drop

	start := time.Now()
	for range publishCount {
		h.Publish(Event{Data: []byte("tick")})
	}

	if elapsed := time.Since(start); elapsed > wait {
		t.Errorf("publishing %d events took %v, want Publish never to block on a wedged subscriber",
			publishCount, elapsed)
	}
	if got := len(healthy.ch); got != publishCount {
		t.Errorf("healthy subscriber received %d events, want %d", got, publishCount)
	}
	if got := h.Stats().Dropped; got == 0 {
		t.Error("Dropped = 0, want the wedged subscriber's misses recorded")
	}
}

// A subscriber that has been dropping continuously is not coming back, and
// keeping it costs a channel send attempt on every event. Past the budget it
// is killed so ServeHTTP can return and free the connection.
func TestHopelessSubscriberIsEvicted(t *testing.T) {
	h := New()
	_, wedged := h.subscribe()

	// Fill the buffer, then miss dropBudget times over.
	for range subscriberBufferSize + dropBudget + 1 {
		h.Publish(Event{Data: []byte("tick")})
	}

	got := h.Stats()
	if got.Evicted != 1 {
		t.Errorf("Evicted = %d, want 1", got.Evicted)
	}
	if got.Subscribers != 0 {
		t.Errorf("Subscribers = %d, want 0 after the eviction", got.Subscribers)
	}

	select {
	case <-wedged.kill:
	default:
		t.Error("evicted subscriber's kill channel is still open, so ServeHTTP would never return")
	}
}

// The budget counts *consecutive* drops. A subscriber that catches up has
// proved it is alive, so its earlier misses must not accumulate toward an
// eviction it no longer deserves -- a client on a slow link would otherwise
// be killed for a burst it recovered from.
func TestRecoveredSubscriberIsNotEvicted(t *testing.T) {
	h := New()
	_, sub := h.subscribe()

	drain := func() {
		for len(sub.ch) > 0 {
			<-sub.ch
		}
	}

	// Two bursts, each large enough to be fatal on its own if the counter
	// never reset, separated by the subscriber catching up.
	for range 2 {
		for range subscriberBufferSize + dropBudget - 1 {
			h.Publish(Event{Data: []byte("tick")})
		}
		drain()
		h.Publish(Event{Data: []byte("tick")}) // lands, resetting the run
		drain()
	}

	got := h.Stats()
	if got.Evicted != 0 {
		t.Errorf("Evicted = %d, want 0 -- the subscriber recovered between bursts", got.Evicted)
	}
	if got.Subscribers != 1 {
		t.Errorf("Subscribers = %d, want 1", got.Subscribers)
	}
	if got.Dropped == 0 {
		t.Error("Dropped = 0, want the misses still counted")
	}
}

func TestStatsReportsSubscriberCount(t *testing.T) {
	h := New()
	server := serve(t, h)

	for i := range 3 {
		hubtest.Subscribe(t, server.URL)
		waitForSubscribers(t, h, i+1)
	}

	if got := h.Stats().Subscribers; got != 3 {
		t.Errorf("Subscribers = %d, want 3", got)
	}
}

// Publish fans out to everyone, so an event must reach every live subscriber
// rather than only the first the map happens to yield.
func TestPublishFansOutToEverySubscriber(t *testing.T) {
	h := New()
	subs := make([]*subscriber, 3)
	for i := range subs {
		_, subs[i] = h.subscribe()
	}

	h.Publish(Event{Name: "signal", Data: []byte("payload")})

	for i, sub := range subs {
		select {
		case got := <-sub.ch:
			if got.Name != "signal" || string(got.Data) != "payload" {
				t.Errorf("subscriber %d got %+v, want the published event", i, got)
			}
		default:
			t.Errorf("subscriber %d received nothing", i)
		}
	}

	if got := h.Stats().Published; got != 1 {
		t.Errorf("Published = %d, want 1 -- it counts events, not deliveries", got)
	}
}
