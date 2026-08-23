package psql

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

// newBenchStore mirrors newTestStore but for benchmarks: it resets the
// quotes table against the shared container and opens a fresh Store.
func newBenchStore(b *testing.B) (*Store, time.Time) {
	b.Helper()
	if testDSN == "" {
		b.Skip("postgres test container not available")
	}

	ctx := context.Background()

	reset, err := Open(testDSN)
	if err != nil {
		b.Fatalf("Open (reset): %v", err)
	}
	if _, err := reset.pool.Exec(ctx, "DROP TABLE IF EXISTS quotes CASCADE"); err != nil {
		b.Fatalf("dropping quotes table before benchmark: %v", err)
	}
	if err := reset.Close(); err != nil {
		b.Fatalf("Close (reset): %v", err)
	}

	s, err := Open(testDSN)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	b.Cleanup(func() {
		if _, err := s.pool.Exec(context.Background(), "DROP TABLE IF EXISTS quotes CASCADE"); err != nil {
			b.Errorf("dropping quotes table after benchmark: %v", err)
		}
	})

	day := time.Now().UTC().Truncate(24 * time.Hour)
	return s, day
}

// makeBatch builds a batch of size n with unique (venue, market, selection,
// observed_at) keys so every row is a genuine insert rather than an
// ON CONFLICT DO NOTHING no-op, and offset so consecutive calls (across b.N
// iterations) never collide with each other either.
func makeBatch(day time.Time, n, offset int) []quote.Quote {
	batch := make([]quote.Quote, n)
	for i := range n {
		idx := offset + i
		batch[i] = quote.Quote{
			Venue:      "binance",
			Market:     "BTC-USD",
			Selection:  fmt.Sprintf("sel-%d", idx),
			Price:      100 + float64(idx)*0.01,
			Size:       1.5,
			ObservedAt: day.Add(time.Duration(idx) * time.Microsecond),
		}
	}
	return batch
}

// BenchmarkWriteBatch measures Store.WriteBatch latency and throughput
// against a real Postgres instance across a range of batch sizes.
func BenchmarkWriteBatch(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			s, day := newBenchStore(b)
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := makeBatch(day, size, i*size)
				if err := s.WriteBatch(ctx, batch); err != nil {
					b.Fatalf("WriteBatch: %v", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(size), "rows/batch")
			b.ReportMetric(float64(size)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}

// BenchmarkWriteBatchConcurrent measures throughput under concurrent
// writers, each submitting fixed-size batches, to surface contention (e.g.
// deadlock retries) that a single-writer benchmark can't.
func BenchmarkWriteBatchConcurrent(b *testing.B) {
	const batchSize = 100

	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			s, day := newBenchStore(b)
			ctx := context.Background()

			b.SetParallelism(workers)
			b.ResetTimer()

			var counter atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					offset := int(counter.Add(int64(batchSize)) - int64(batchSize))
					batch := makeBatch(day, batchSize, offset)
					if err := s.WriteBatch(ctx, batch); err != nil {
						b.Fatalf("WriteBatch: %v", err)
					}
				}
			})
			b.StopTimer()

			b.ReportMetric(float64(batchSize), "rows/batch")
			b.ReportMetric(float64(batchSize)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}
