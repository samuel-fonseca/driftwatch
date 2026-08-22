package dedupe

import (
	"container/list"
	"sync"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

type seenEntry struct {
	key         string
	fingerprint uint64
}

type Detector struct {
	mu       sync.Mutex
	capacity int
	order    *list.List
	index    map[string]*list.Element
}

func New(capacity int) *Detector {
	return &Detector{
		capacity: capacity,
		order:    list.New(),
		index:    make(map[string]*list.Element, capacity),
	}
}

func (d *Detector) Changed(q quote.Quote) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if entry, ok := d.index[q.Key()]; ok {
		seen := entry.Value.(*seenEntry)
		d.order.MoveToFront(entry)
		if seen.fingerprint == q.Fingerprint() {
			return false
		}

		seen.fingerprint = q.Fingerprint()
		return true
	}

	if d.order.Len() >= d.capacity {
		stale := d.order.Back()
		staleEntry := stale.Value.(*seenEntry)
		delete(d.index, staleEntry.key)
		d.order.Remove(stale)
	}

	newEntry := &seenEntry{key: q.Key(), fingerprint: q.Fingerprint()}
	d.order.PushFront(newEntry)
	d.index[q.Key()] = d.order.Front()
	return true
}

func (d *Detector) FilterChanged(quotes []quote.Quote) []quote.Quote {
	out := make([]quote.Quote, 0, len(quotes))
	for _, q := range quotes {
		if d.Changed(q) {
			out = append(out, q)
		}
	}
	return out
}
