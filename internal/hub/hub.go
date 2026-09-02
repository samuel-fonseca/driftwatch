package hub

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dropBudget           = 256
	subscriberBufferSize = 64
	heartbeatInterval    = 15 * time.Second
)

type Event struct {
	Name string
	Data []byte
}

type subscriber struct {
	ch    chan Event
	kill  chan struct{}
	drops atomic.Int32
}

type Hub struct {
	mu          sync.Mutex
	subscribers map[int64]*subscriber
	nextID      int64
	published   int64
	dropped     int64
	evicted     int64
	heartbeat   time.Duration
}

func New() *Hub {
	return &Hub{
		subscribers: make(map[int64]*subscriber),
		heartbeat:   heartbeatInterval,
	}
}

type Stats struct {
	Subscribers                 int
	Published, Dropped, Evicted int64
}

func (h *Hub) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	return Stats{
		Subscribers: len(h.subscribers),
		Published:   h.published,
		Dropped:     h.dropped,
		Evicted:     h.evicted,
	}
}

func (h *Hub) subscribe() (int64, *subscriber) {
	return h.subscribeWithBuffer(subscriberBufferSize)
}

func (h *Hub) subscribeWithBuffer(n int) (int64, *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()

	id := h.nextID
	h.nextID++

	sub := &subscriber{
		ch:   make(chan Event, n),
		kill: make(chan struct{}),
	}
	h.subscribers[id] = sub
	return id, sub
}

func (h *Hub) unsubscribe(id int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subscribers, id)
}

func (h *Hub) Publish(e Event) {
	h.mu.Lock()
	h.published++
	ids := make([]int64, 0, len(h.subscribers))
	subs := make([]*subscriber, 0, len(h.subscribers))
	for id, sub := range h.subscribers {
		ids, subs = append(ids, id), append(subs, sub)
	}
	h.mu.Unlock()

	var dropped int64
	var doomed []int64
	for i, sub := range subs {
		select {
		case sub.ch <- e:
			sub.drops.Store(0)
		default:
			dropped++
			if sub.drops.Add(1) >= dropBudget {
				doomed = append(doomed, ids[i])
			}
		}
	}

	if dropped == 0 {
		return
	}

	h.mu.Lock()
	h.dropped += dropped
	for _, id := range doomed {
		if sub, ok := h.subscribers[id]; ok {
			close(sub.kill)
			delete(h.subscribers, id)
			h.evicted++
		}
	}
	h.mu.Unlock()
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	fmt.Fprintf(w, "retry: 2000\n")
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	id, sub := h.subscribe()
	defer h.unsubscribe(id)

	ticker := time.NewTicker(h.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			// client disconnected
			return
		case <-sub.kill:
			// hopeless, kill.
			return
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()

		case e := <-sub.ch:
			if e.Name != "" {
				fmt.Fprintf(w, "event: %s\n", e.Name)
			}
			fmt.Fprintf(w, "data: %s\n\n", e.Data)
			flusher.Flush()
		}
	}
}
