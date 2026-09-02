package buffer

import (
	"container/list"
	"context"
	"sync"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

type entry struct {
	key   string
	quote quote.Quote
}

type Buffer struct {
	mu                                          sync.Mutex
	capacity                                    int
	order                                       *list.List
	index                                       map[string]*list.Element
	pushed, coalesced, evicted, taken, maxDepth int
	notify                                      chan struct{}
}

func New(capacity int) *Buffer {
	return &Buffer{
		capacity: capacity,
		order:    list.New(),
		index:    make(map[string]*list.Element, capacity),
		notify:   make(chan struct{}, 1),
	}
}

func (b *Buffer) Push(q quote.Quote) {
	b.insert(q)

	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (b *Buffer) insert(q quote.Quote) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushed++
	defer func() {
		b.maxDepth = max(b.maxDepth, b.order.Len())
	}()

	key := q.Key()
	if element, found := b.index[key]; found {
		element.Value.(*entry).quote = q
		b.coalesced++
		return
	}

	if b.order.Len() >= b.capacity {
		oldest := b.order.Front()
		oldestEntry := oldest.Value.(*entry)
		delete(b.index, oldestEntry.key)
		b.order.Remove(oldest)
		b.evicted++
	}

	b.index[key] = b.order.PushBack(&entry{key: key, quote: q})
}

func (b *Buffer) TakeBatch(ctx context.Context, max int) ([]quote.Quote, error) {
	for {
		b.mu.Lock()
		if b.order.Len() > 0 {
			quotes := make([]quote.Quote, 0, max)
			for range max {
				if b.order.Len() == 0 {
					break
				}

				oldest := b.order.Front()
				oldestEntry := oldest.Value.(*entry)
				quotes = append(quotes, oldestEntry.quote)
				b.order.Remove(oldest)
				delete(b.index, oldestEntry.key)
				b.taken++
			}
			b.mu.Unlock()
			return quotes, nil
		}
		b.mu.Unlock()

		select {
		case <-b.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

type Stats struct {
	Pushed, Coalesced, Evicted, Taken, Depth, MaxDepth, Capacity int
}

func (b *Buffer) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{
		Pushed:    b.pushed,
		Coalesced: b.coalesced,
		Evicted:   b.evicted,
		Taken:     b.taken,
		Depth:     b.order.Len(),
		MaxDepth:  b.maxDepth,
		Capacity:  b.capacity,
	}
}
