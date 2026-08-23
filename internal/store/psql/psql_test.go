package psql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/store"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var _ store.Store = (*Store)(nil)

var testDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("driftwatch"),
		tcpostgres.WithUsername("driftwatch"),
		tcpostgres.WithPassword("driftwatch"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		fmt.Printf("skipping psql tests: could not start postgres container: %v\n", err)
		return
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Printf("failed to terminate postgres container: %v\n", err)
		}
	}()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Printf("failed to get postgres connection string: %v\n", err)
		return
	}
	testDSN = dsn

	m.Run()
}

// newTestStore resets the quotes table against the shared test container,
// then opens a fresh Store (which creates the base table plus today's and
// tomorrow's partitions as part of Open), registering cleanup to close the
// store and drop the table when the test finishes.
func newTestStore(t *testing.T) (*Store, time.Time) {
	t.Helper()
	if testDSN == "" {
		t.Skip("postgres test container not available")
	}

	ctx := context.Background()

	reset, err := Open(testDSN)
	if err != nil {
		t.Fatalf("Open (reset): %v", err)
	}
	if _, err := reset.pool.Exec(ctx, "DROP TABLE IF EXISTS quotes CASCADE"); err != nil {
		t.Fatalf("dropping quotes table before test: %v", err)
	}
	if err := reset.Close(); err != nil {
		t.Fatalf("Close (reset): %v", err)
	}

	s, err := Open(testDSN)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	t.Cleanup(func() {
		if _, err := s.pool.Exec(context.Background(), "DROP TABLE IF EXISTS quotes CASCADE"); err != nil {
			t.Errorf("dropping quotes table after test: %v", err)
		}
	})

	day := time.Now().UTC().Truncate(24 * time.Hour)
	return s, day
}

// partitionExists reports whether quotes_<suffix> is a partition of quotes.
func partitionExists(t *testing.T, s *Store, suffix string) bool {
	t.Helper()

	var exists bool
	err := s.pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM pg_inherits i
			JOIN pg_class parent ON parent.oid = i.inhparent
			JOIN pg_class child ON child.oid = i.inhrelid
			WHERE parent.relname = 'quotes' AND child.relname = $1
		)`, "quotes_"+suffix).Scan(&exists)
	if err != nil {
		t.Fatalf("querying pg_inherits for quotes_%s: %v", suffix, err)
	}
	return exists
}

func TestOpenSucceedsAndPingsServer(t *testing.T) {
	if testDSN == "" {
		t.Skip("postgres test container not available")
	}

	s, err := Open(testDSN)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if s == nil || s.pool == nil {
		t.Fatal("Open returned a Store with a nil pool")
	}
}

func TestOpenFailsOnUnreachableServer(t *testing.T) {
	_, err := Open("postgres://driftwatch:driftwatch@127.0.0.1:1/driftwatch?sslmode=disable&connect_timeout=1")
	if err == nil {
		t.Fatal("expected Open to fail against an unreachable server, got nil error")
	}
}

func TestEnsureBaseTablesExistCreatesPartitionedTable(t *testing.T) {
	if testDSN == "" {
		t.Skip("postgres test container not available")
	}

	ctx := context.Background()
	s, err := Open(testDSN)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	if _, err := s.pool.Exec(ctx, "DROP TABLE IF EXISTS quotes CASCADE"); err != nil {
		t.Fatalf("dropping quotes table before test: %v", err)
	}
	t.Cleanup(func() {
		if _, err := s.pool.Exec(context.Background(), "DROP TABLE IF EXISTS quotes CASCADE"); err != nil {
			t.Errorf("dropping quotes table after test: %v", err)
		}
	})

	if err := ensureBaseTablesExist(ctx, s.pool); err != nil {
		t.Fatalf("ensureBaseTablesExist: %v", err)
	}

	// Calling it again should be idempotent (CREATE TABLE IF NOT EXISTS).
	if err := ensureBaseTablesExist(ctx, s.pool); err != nil {
		t.Fatalf("ensureBaseTablesExist (2nd call): %v", err)
	}

	var isPartitioned bool
	err = s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_partitioned_table pt
			JOIN pg_class c ON c.oid = pt.partrelid
			WHERE c.relname = 'quotes'
		)`).Scan(&isPartitioned)
	if err != nil {
		t.Fatalf("querying pg_partitioned_table: %v", err)
	}
	if !isPartitioned {
		t.Error("quotes table was not created as a partitioned table")
	}
}

func TestEnsurePartitionIsIdempotent(t *testing.T) {
	s, day := newTestStore(t)
	ctx := context.Background()

	if err := ensurePartition(ctx, s.pool, day); err != nil {
		t.Fatalf("ensurePartition (2nd call for today): %v", err)
	}
}

func TestOpenCreatesTodayAndTomorrowPartitions(t *testing.T) {
	s, day := newTestStore(t)

	todaySuffix := day.Format("20060102")
	tomorrowSuffix := day.Add(24 * time.Hour).Format("20060102")

	if !partitionExists(t, s, todaySuffix) {
		t.Errorf("expected quotes_%s to be a partition of quotes", todaySuffix)
	}
	if !partitionExists(t, s, tomorrowSuffix) {
		t.Errorf("expected quotes_%s to be a partition of quotes", tomorrowSuffix)
	}
}

func TestRollOnceRecreatesTomorrowsPartition(t *testing.T) {
	s, day := newTestStore(t)
	ctx := context.Background()

	tomorrowSuffix := day.Add(24 * time.Hour).Format("20060102")

	if _, err := s.pool.Exec(ctx, "DROP TABLE quotes_"+tomorrowSuffix); err != nil {
		t.Fatalf("dropping tomorrow's partition: %v", err)
	}
	if partitionExists(t, s, tomorrowSuffix) {
		t.Fatalf("quotes_%s should not exist after being dropped", tomorrowSuffix)
	}

	if err := s.rollOnce(ctx); err != nil {
		t.Fatalf("rollOnce: %v", err)
	}

	if !partitionExists(t, s, tomorrowSuffix) {
		t.Errorf("expected rollOnce to recreate quotes_%s", tomorrowSuffix)
	}
}

func TestNextMidnightUTCReturnsUpcomingMidnight(t *testing.T) {
	now := time.Date(2026, 8, 22, 15, 30, 0, 0, time.UTC)
	want := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)

	got := nextMidnightUTC(now)
	if !got.Equal(want) {
		t.Errorf("nextMidnightUTC(%v) = %v, want %v", now, got, want)
	}
}

func TestNextMidnightUTCAtExactMidnightReturnsNextDay(t *testing.T) {
	now := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	want := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)

	got := nextMidnightUTC(now)
	if !got.Equal(want) {
		t.Errorf("nextMidnightUTC(%v) = %v, want %v", now, got, want)
	}
	if !got.After(now) {
		t.Errorf("nextMidnightUTC(%v) = %v, want a time strictly after now", now, got)
	}
}

func TestWriteBatchInsertsRowsQueryableFromQuotes(t *testing.T) {
	s, day := newTestStore(t)
	ctx := context.Background()

	observedAt := day.Add(3 * time.Hour).Truncate(time.Microsecond)
	batch := []quote.Quote{
		{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100.5, Size: 1.25, ObservedAt: observedAt},
		{Venue: "binance", Market: "BTC-USD", Selection: "ask", Price: 101.5, Size: 2.5, ObservedAt: observedAt},
	}

	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT venue, market, selection, price, size, observed_at
		FROM quotes ORDER BY selection`)
	if err != nil {
		t.Fatalf("querying quotes: %v", err)
	}
	defer rows.Close()

	var got []quote.Quote
	for rows.Next() {
		var q quote.Quote
		if err := rows.Scan(&q.Venue, &q.Market, &q.Selection, &q.Price, &q.Size, &q.ObservedAt); err != nil {
			t.Fatalf("scanning row: %v", err)
		}
		got = append(got, q)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating rows: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}

	want := batch[1] // "ask" sorts before "bid" alphabetically
	if got[0].Venue != want.Venue || got[0].Market != want.Market || got[0].Selection != want.Selection {
		t.Errorf("row 0 = %+v, want venue/market/selection matching %+v", got[0], want)
	}
	if got[0].Price != want.Price {
		t.Errorf("row 0 price = %v, want %v", got[0].Price, want.Price)
	}
	if got[0].Size != want.Size {
		t.Errorf("row 0 size = %v, want %v", got[0].Size, want.Size)
	}
	if !got[0].ObservedAt.Equal(want.ObservedAt) {
		t.Errorf("row 0 observed_at = %v, want %v", got[0].ObservedAt, want.ObservedAt)
	}
}

func TestWriteBatchInsertsIntoCorrectDailyPartition(t *testing.T) {
	s, day := newTestStore(t)
	ctx := context.Background()

	suffix := day.Format("20060102")

	if err := s.WriteBatch(ctx, []quote.Quote{
		{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1, ObservedAt: day.Add(time.Hour)},
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	var count int
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM quotes_"+suffix).Scan(&count); err != nil {
		t.Fatalf("querying child partition directly: %v", err)
	}
	if count != 1 {
		t.Errorf("got %d rows in quotes_%s, want 1", count, suffix)
	}
}

func TestWriteBatchEmptyBatchIsNoOp(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	if err := s.WriteBatch(ctx, nil); err != nil {
		t.Fatalf("WriteBatch(nil) should be a no-op, got error: %v", err)
	}

	var count int
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM quotes").Scan(&count); err != nil {
		t.Fatalf("querying quotes: %v", err)
	}
	if count != 0 {
		t.Errorf("got %d rows from an empty batch, want 0", count)
	}
}

func TestWriteBatchFailsWithoutMatchingPartition(t *testing.T) {
	s, day := newTestStore(t)
	ctx := context.Background()

	// A week outside the today/tomorrow partitions newTestStore creates.
	outOfRange := day.AddDate(0, 0, 7)

	err := s.WriteBatch(ctx, []quote.Quote{
		{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1, ObservedAt: outOfRange},
	})
	if err == nil {
		t.Fatal("expected WriteBatch to fail for a row with no matching partition, got nil error")
	}
}

func TestWriteBatchRespectsContextCancellation(t *testing.T) {
	s, day := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.WriteBatch(ctx, []quote.Quote{
		{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1, ObservedAt: day.Add(time.Hour)},
	})
	if err == nil {
		t.Fatal("expected WriteBatch to fail with an already-cancelled context, got nil error")
	}
}

func TestCloseIsIdempotentAndUsable(t *testing.T) {
	if testDSN == "" {
		t.Skip("postgres test container not available")
	}

	s, err := Open(testDSN)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = s.WriteBatch(context.Background(), []quote.Quote{
		{Venue: "binance", Market: "BTC-USD", Selection: "bid"},
	})
	if err == nil {
		t.Fatal("expected WriteBatch after Close to return an error, got nil")
	}
}

func TestWriteBatchConcurrentIsSafe(t *testing.T) {
	s, day := newTestStore(t)
	ctx := context.Background()

	const goroutines = 10
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		observedAt := day.Add(time.Duration(i) * time.Minute)
		batch := []quote.Quote{
			{Venue: "binance", Market: "BTC-USD", Selection: "ask", Price: 100 * float64(i), Size: 24, ObservedAt: observedAt},
			{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 90 * float64(i), Size: 51, ObservedAt: observedAt},
		}
		go func(batch []quote.Quote) {
			defer wg.Done()
			if err := s.WriteBatch(ctx, batch); err != nil {
				t.Errorf("WriteBatch: %v", err)
			}
		}(batch)
	}
	wg.Wait()

	var count int
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM quotes").Scan(&count); err != nil {
		t.Fatalf("querying quotes: %v", err)
	}
	if count != goroutines*2 {
		t.Errorf("got %d rows, want %d", count, goroutines*2)
	}
}
