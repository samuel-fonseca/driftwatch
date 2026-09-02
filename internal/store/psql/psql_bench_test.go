package psql

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/quotetest"
)

// makeBatch builds a batch of n quotes with unique (venue, market, selection,
// observed_at) keys, so every row is a genuine insert rather than an
// ON CONFLICT DO NOTHING no-op. The offset keeps consecutive calls -- across
// b.N iterations and across parallel writers -- from colliding with each
// other either, which would otherwise quietly benchmark the conflict path.
func makeBatch(day time.Time, n, offset int) []quote.Quote {
	batch := make([]quote.Quote, n)
	for i := range n {
		idx := offset + i
		batch[i] = quotetest.Sel(
			"binance", "BTC-USD",
			fmt.Sprintf("sel-%d", idx),
			100+float64(idx)*0.01,
			quotetest.Size(1.5),
			quotetest.At(day.Add(time.Duration(idx)*time.Microsecond)),
		)
	}
	return batch
}

// BenchmarkWriteBatch measures WriteBatch latency and throughput against a
// real Postgres instance across a range of batch sizes.
func BenchmarkWriteBatch(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			s, day := newStore(b)
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.WriteBatch(ctx, makeBatch(day, size, i*size)); err != nil {
					b.Fatalf("WriteBatch() = %v, want nil", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(size), "rows/batch")
			b.ReportMetric(float64(size)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}

// BenchmarkWriteBatchConcurrent measures throughput under concurrent writers,
// each submitting fixed-size batches, to surface contention (deadlock retries,
// pool starvation) that a single-writer benchmark cannot.
func BenchmarkWriteBatchConcurrent(b *testing.B) {
	const batchSize = 100

	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			s, day := newStore(b)
			ctx := context.Background()

			b.SetParallelism(workers)
			b.ResetTimer()

			var counter atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					offset := int(counter.Add(batchSize) - batchSize)
					if err := s.WriteBatch(ctx, makeBatch(day, batchSize, offset)); err != nil {
						b.Fatalf("WriteBatch() = %v, want nil", err)
					}
				}
			})
			b.StopTimer()

			b.ReportMetric(float64(batchSize), "rows/batch")
			b.ReportMetric(float64(batchSize)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}
