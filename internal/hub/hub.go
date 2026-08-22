package hub

import (
	"fmt"
	"net/http"
	"sync"
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
	drops int
}

type Hub struct {
	mu          sync.Mutex
	subscribers map[int64]*subscriber
	nextID      int64
	published   int64
	dropped     int64
	evicted     int64
}

func New() *Hub {
	return &Hub{
		subscribers: make(map[int64]*subscriber),
	}
}

type Stats struct {
	Subscribers int
	Published   int64
	Dropped     int64
	Evicted     int64
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
	h.mu.Lock()
	defer h.mu.Unlock()

	id := h.nextID
	h.nextID++

	sub := &subscriber{
		ch:   make(chan Event, subscriberBufferSize),
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
	defer h.mu.Unlock()

	h.published++

	for id, sub := range h.subscribers {
		select {
		case sub.ch <- e:
		default:
			h.dropped++
			sub.drops++

			if sub.drops >= dropBudget {
				close(sub.kill)
				delete(h.subscribers, id)
				h.evicted++
			}
		}
	}
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

	ticker := time.NewTicker(heartbeatInterval)
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
