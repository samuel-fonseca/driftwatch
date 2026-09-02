package psql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/quotetest"
	"github.com/samuel-fonseca/driftwatch/internal/store"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var _ store.Store = (*Store)(nil)

// --- container lifecycle ---

var (
	testDSN string
	// adminPool resets the schema between tests without paying for a second
	// Store -- opening one starts a partition-rolling goroutine, and the old
	// helper opened, dropped, closed and reopened for every single test.
	adminPool *pgxpool.Pool
	// dbErr records why the database is unavailable, so each test skips with
	// a reason instead of the package reporting a hollow "ok, 0 tests".
	dbErr error
)

func TestMain(m *testing.M) { os.Exit(runTests(m)) }

func runTests(m *testing.M) int {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("driftwatch"),
		tcpostgres.WithUsername("driftwatch"),
		tcpostgres.WithPassword("driftwatch"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		// The pure-function tests still run; everything else skips.
		dbErr = fmt.Errorf("starting postgres container: %w", err)
		return m.Run()
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Printf("terminating postgres container: %v\n", err)
		}
	}()

	testDSN, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		dbErr = fmt.Errorf("building connection string: %w", err)
		return m.Run()
	}

	adminPool, err = pgxpool.New(ctx, testDSN)
	if err != nil {
		dbErr = fmt.Errorf("opening admin pool: %w", err)
		return m.Run()
	}
	defer adminPool.Close()

	return m.Run()
}

// --- test helpers ---

func requireDB(tb testing.TB) {
	tb.Helper()
	if dbErr != nil {
		tb.Skipf("postgres test container unavailable: %v", dbErr)
	}
}

// dropQuotes clears the schema through the admin pool, so it works before a
// Store exists and after one has been closed.
func dropQuotes(tb testing.TB) {
	tb.Helper()
	if _, err := adminPool.Exec(context.Background(), "DROP TABLE IF EXISTS quotes CASCADE"); err != nil {
		tb.Fatalf("dropping quotes table: %v", err)
	}
}

// newStore resets the schema and opens a Store against the shared container.
// Open creates the base table plus today's and tomorrow's partitions. The
// returned time is the UTC day those partitions cover. It serves benchmarks
// as well as tests, which is why it takes a testing.TB.
func newStore(tb testing.TB) (*Store, time.Time) {
	tb.Helper()
	requireDB(tb)

	dropQuotes(tb)

	s, err := Open(testDSN)
	if err != nil {
		tb.Fatalf("Open() = %v, want nil", err)
	}
	tb.Cleanup(func() {
		if err := s.Close(); err != nil {
			tb.Errorf("Close() = %v, want nil", err)
		}
		dropQuotes(tb)
	})

	return s, time.Now().UTC().Truncate(24 * time.Hour)
}

// partitionExists reports whether quotes_<suffix> is a partition of quotes.
func partitionExists(tb testing.TB, suffix string) bool {
	tb.Helper()

	var exists bool
	err := adminPool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM pg_inherits i
			JOIN pg_class parent ON parent.oid = i.inhparent
			JOIN pg_class child ON child.oid = i.inhrelid
			WHERE parent.relname = 'quotes' AND child.relname = $1
		)`, "quotes_"+suffix).Scan(&exists)
	if err != nil {
		tb.Fatalf("querying pg_inherits for quotes_%s: %v", suffix, err)
	}
	return exists
}

func countQuotes(t *testing.T, s *Store, query string) int {
	t.Helper()

	var count int
	if err := s.pool.QueryRow(context.Background(), query).Scan(&count); err != nil {
		t.Fatalf("counting via %q: %v", query, err)
	}
	return count
}

func suffix(day time.Time) string { return day.Format("20060102") }

// --- pure functions (no container needed) ---

func TestNextMidnightUTC(t *testing.T) {
	cases := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{
			name: "mid-afternoon",
			now:  time.Date(2026, 8, 22, 15, 30, 0, 0, time.UTC),
			want: time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
		},
		{
			// Strictly after, or the partition roller would wake in a tight
			// loop at exactly midnight.
			name: "exactly midnight",
			now:  time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC),
			want: time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "last second of the year",
			now:  time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC),
			want: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := nextMidnightUTC(c.now)
			if !got.Equal(c.want) {
				t.Errorf("nextMidnightUTC(%v) = %v, want %v", c.now, got, c.want)
			}
			if !got.After(c.now) {
				t.Errorf("nextMidnightUTC(%v) = %v, want a time strictly after now", c.now, got)
			}
		})
	}
}

// deadlockErr is what Postgres returns when concurrent writers deadlock on
// the unique index; it is the only error WriteBatch is allowed to retry.
func deadlockErr() error { return &pgconn.PgError{Code: deadlockCode, Message: "deadlock detected"} }

func TestRetryOnDeadlock(t *testing.T) {
	t.Run("returns immediately on success", func(t *testing.T) {
		calls := 0
		err := retryOnDeadlock(context.Background(), func(context.Context) error {
			calls++
			return nil
		})

		if err != nil {
			t.Errorf("retryOnDeadlock() = %v, want nil", err)
		}
		if calls != 1 {
			t.Errorf("called fn %d times, want 1", calls)
		}
	})

	t.Run("retries a deadlock until it clears", func(t *testing.T) {
		calls := 0
		err := retryOnDeadlock(context.Background(), func(context.Context) error {
			calls++
			if calls < 3 {
				return deadlockErr()
			}
			return nil
		})

		if err != nil {
			t.Errorf("retryOnDeadlock() = %v, want nil once the deadlock cleared", err)
		}
		if calls != 3 {
			t.Errorf("called fn %d times, want 3", calls)
		}
	})

	// Retrying a constraint violation or a dead connection just delays the
	// report; only a deadlock is worth another attempt.
	t.Run("does not retry other errors", func(t *testing.T) {
		sentinel := errors.New("constraint violation")
		calls := 0

		err := retryOnDeadlock(context.Background(), func(context.Context) error {
			calls++
			return sentinel
		})

		if !errors.Is(err, sentinel) {
			t.Errorf("retryOnDeadlock() = %v, want %v", err, sentinel)
		}
		if calls != 1 {
			t.Errorf("called fn %d times, want 1 -- a non-deadlock must not be retried", calls)
		}
	})

	t.Run("does not retry a different postgres error", func(t *testing.T) {
		calls := 0
		err := retryOnDeadlock(context.Background(), func(context.Context) error {
			calls++
			return &pgconn.PgError{Code: "23505", Message: "unique violation"}
		})

		if err == nil {
			t.Error("retryOnDeadlock() = nil, want the unique violation")
		}
		if calls != 1 {
			t.Errorf("called fn %d times, want 1", calls)
		}
	})

	t.Run("gives up after the retry budget", func(t *testing.T) {
		calls := 0
		err := retryOnDeadlock(context.Background(), func(context.Context) error {
			calls++
			return deadlockErr()
		})

		if err == nil {
			t.Fatal("retryOnDeadlock() = nil after persistent deadlocks, want an error")
		}
		if calls != maxDeadlockRetries {
			t.Errorf("called fn %d times, want %d", calls, maxDeadlockRetries)
		}
	})

	// Shutdown must not be held open by the backoff between attempts.
	t.Run("abandons the backoff when the context ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		calls := 0
		err := retryOnDeadlock(ctx, func(context.Context) error {
			calls++
			cancel() // cancelled by the time the first backoff starts
			return deadlockErr()
		})

		if !errors.Is(err, context.Canceled) {
			t.Errorf("retryOnDeadlock() = %v, want %v", err, context.Canceled)
		}
		if calls != 1 {
			t.Errorf("called fn %d times, want 1", calls)
		}
	})
}

// --- Open ---

// A DSN that will not parse is a config fault, and it must surface at Open
// rather than as a nil-pool panic on the first write.
func TestOpenFailsOnMalformedDSN(t *testing.T) {
	if s, err := Open("://not a dsn"); err == nil {
		s.Close()
		t.Error("Open() = nil error for an unparseable DSN, want one")
	}
}

func TestOpenFailsOnUnreachableServer(t *testing.T) {
	_, err := Open("postgres://driftwatch:driftwatch@127.0.0.1:1/driftwatch?sslmode=disable&connect_timeout=1")
	if err == nil {
		t.Fatal("Open() = nil error against an unreachable server, want one")
	}
}

func TestOpenCreatesPartitionedTable(t *testing.T) {
	s, _ := newStore(t)

	var isPartitioned bool
	err := s.pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM pg_partitioned_table pt
			JOIN pg_class c ON c.oid = pt.partrelid
			WHERE c.relname = 'quotes'
		)`).Scan(&isPartitioned)
	if err != nil {
		t.Fatalf("querying pg_partitioned_table: %v", err)
	}
	if !isPartitioned {
		t.Error("quotes was not created as a partitioned table")
	}
}

// Open runs on every start, so its DDL has to be safe to repeat.
func TestEnsureBaseTablesExistIsIdempotent(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	for i := range 3 {
		if err := ensureBaseTablesExist(ctx, s.pool); err != nil {
			t.Fatalf("ensureBaseTablesExist call %d = %v, want nil", i+1, err)
		}
	}
}

// Today's and tomorrow's partitions must both exist up front, so a write
// never fails just because the day rolled over mid-batch.
func TestOpenCreatesTodayAndTomorrowPartitions(t *testing.T) {
	_, day := newStore(t)

	for _, d := range []time.Time{day, day.Add(24 * time.Hour)} {
		if !partitionExists(t, suffix(d)) {
			t.Errorf("quotes_%s is not a partition of quotes", suffix(d))
		}
	}
}

func TestEnsurePartitionIsIdempotent(t *testing.T) {
	s, day := newStore(t)

	if err := ensurePartition(context.Background(), s.pool, day); err != nil {
		t.Fatalf("ensurePartition (second call for today) = %v, want nil", err)
	}
}

func TestRollOnceRecreatesTomorrowsPartition(t *testing.T) {
	s, day := newStore(t)
	ctx := context.Background()
	tomorrow := suffix(day.Add(24 * time.Hour))

	if _, err := s.pool.Exec(ctx, "DROP TABLE quotes_"+tomorrow); err != nil {
		t.Fatalf("dropping tomorrow's partition: %v", err)
	}
	if partitionExists(t, tomorrow) {
		t.Fatalf("quotes_%s still exists after being dropped", tomorrow)
	}

	if err := s.rollOnce(ctx); err != nil {
		t.Fatalf("rollOnce() = %v, want nil", err)
	}
	if !partitionExists(t, tomorrow) {
		t.Errorf("rollOnce did not recreate quotes_%s", tomorrow)
	}
}

// --- WriteBatch ---

func TestWriteBatchRoundTripsEveryField(t *testing.T) {
	s, day := newStore(t)
	ctx := context.Background()

	// Postgres stores microseconds; truncating here keeps the comparison
	// about the write path rather than about timestamp precision.
	observedAt := day.Add(3 * time.Hour).Truncate(time.Microsecond)
	want := quotetest.Bid("binance", "BTC-USD", 100.5,
		quotetest.Size(1.25), quotetest.At(observedAt))

	if err := s.WriteBatch(ctx, []quote.Quote{want}); err != nil {
		t.Fatalf("WriteBatch() = %v, want nil", err)
	}

	var got quote.Quote
	err := s.pool.QueryRow(ctx, `
		SELECT venue, market, selection, price, size, observed_at FROM quotes`).
		Scan(&got.Venue, &got.Market, &got.Selection, &got.Price, &got.Size, &got.ObservedAt)
	if err != nil {
		t.Fatalf("reading the row back: %v", err)
	}

	if got.Venue != want.Venue || got.Market != want.Market || got.Selection != want.Selection {
		t.Errorf("identity = %s/%s/%s, want %s/%s/%s",
			got.Venue, got.Market, got.Selection, want.Venue, want.Market, want.Selection)
	}
	if got.Price != want.Price || got.Size != want.Size {
		t.Errorf("price/size = %v/%v, want %v/%v", got.Price, got.Size, want.Price, want.Size)
	}
	if !got.ObservedAt.Equal(want.ObservedAt) {
		t.Errorf("observed_at = %v, want %v", got.ObservedAt, want.ObservedAt)
	}
}

func TestWriteBatchInsertsEveryQuote(t *testing.T) {
	s, day := newStore(t)
	at := quotetest.At(day.Add(time.Hour))

	batch := []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100.5, at),
		quotetest.Ask("binance", "BTC-USD", 101.5, at),
		quotetest.Bid("kraken", "ETH-USD", 50.0, at),
	}
	if err := s.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("WriteBatch() = %v, want nil", err)
	}

	if got := countQuotes(t, s, "SELECT count(*) FROM quotes"); got != len(batch) {
		t.Errorf("stored %d rows, want %d", got, len(batch))
	}
}

// Rows must land in the child partition for their observation day, which is
// what makes dropping a day's data a DROP TABLE rather than a DELETE scan.
func TestWriteBatchRoutesToTheDailyPartition(t *testing.T) {
	s, day := newStore(t)

	if err := s.WriteBatch(context.Background(), []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100, quotetest.At(day.Add(time.Hour))),
	}); err != nil {
		t.Fatalf("WriteBatch() = %v, want nil", err)
	}

	if got := countQuotes(t, s, "SELECT count(*) FROM quotes_"+suffix(day)); got != 1 {
		t.Errorf("quotes_%s holds %d rows, want 1", suffix(day), got)
	}
}

// The unique index plus ON CONFLICT DO NOTHING is what makes a replayed or
// overlapping batch harmless -- without it a restart would double-count.
func TestWriteBatchIsIdempotentForIdenticalRows(t *testing.T) {
	s, day := newStore(t)
	ctx := context.Background()

	batch := []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100, quotetest.At(day.Add(time.Hour))),
		quotetest.Ask("binance", "BTC-USD", 101, quotetest.At(day.Add(time.Hour))),
	}

	for i := range 3 {
		if err := s.WriteBatch(ctx, batch); err != nil {
			t.Fatalf("WriteBatch call %d = %v, want nil", i+1, err)
		}
	}

	if got := countQuotes(t, s, "SELECT count(*) FROM quotes"); got != len(batch) {
		t.Errorf("stored %d rows after writing the same batch 3x, want %d", got, len(batch))
	}
}

// Same key, different price: the conflict target is the identity, so the
// first write wins rather than the row being updated.
func TestWriteBatchKeepsTheFirstRowOnConflict(t *testing.T) {
	s, day := newStore(t)
	ctx := context.Background()
	at := quotetest.At(day.Add(time.Hour))

	if err := s.WriteBatch(ctx, []quote.Quote{quotetest.Bid("binance", "BTC-USD", 100, at)}); err != nil {
		t.Fatalf("first WriteBatch() = %v, want nil", err)
	}
	if err := s.WriteBatch(ctx, []quote.Quote{quotetest.Bid("binance", "BTC-USD", 999, at)}); err != nil {
		t.Fatalf("second WriteBatch() = %v, want nil", err)
	}

	var price float64
	if err := s.pool.QueryRow(ctx, "SELECT price FROM quotes").Scan(&price); err != nil {
		t.Fatalf("reading price: %v", err)
	}
	if price != 100 {
		t.Errorf("price = %v, want the original 100 (DO NOTHING, not DO UPDATE)", price)
	}
}

func TestWriteBatchEmptyBatchIsNoOp(t *testing.T) {
	s, _ := newStore(t)

	if err := s.WriteBatch(context.Background(), nil); err != nil {
		t.Fatalf("WriteBatch(nil) = %v, want nil", err)
	}
	if got := countQuotes(t, s, "SELECT count(*) FROM quotes"); got != 0 {
		t.Errorf("stored %d rows from an empty batch, want 0", got)
	}
}

// A quote dated outside every partition has nowhere to go. Failing loudly
// beats silently dropping it, which is what a default partition would do.
func TestWriteBatchFailsWithoutAMatchingPartition(t *testing.T) {
	s, day := newStore(t)

	err := s.WriteBatch(context.Background(), []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100, quotetest.At(day.AddDate(0, 0, 7))),
	})
	if err == nil {
		t.Fatal("WriteBatch() = nil error for a row with no matching partition, want one")
	}
}

func TestWriteBatchRespectsContextCancellation(t *testing.T) {
	s, day := newStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.WriteBatch(ctx, []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100, quotetest.At(day.Add(time.Hour))),
	})
	if err == nil {
		t.Fatal("WriteBatch() = nil error with a cancelled context, want one")
	}
}

// Several pipeline workers share one Store and one pool.
func TestWriteBatchConcurrentIsSafe(t *testing.T) {
	s, day := newStore(t)
	ctx := context.Background()

	const workers = 10

	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			at := quotetest.At(day.Add(time.Duration(i) * time.Minute))
			batch := []quote.Quote{
				quotetest.Ask("binance", "BTC-USD", 100*float64(i), at),
				quotetest.Bid("binance", "BTC-USD", 90*float64(i), at),
			}
			if err := s.WriteBatch(ctx, batch); err != nil {
				t.Errorf("WriteBatch() = %v, want nil", err)
			}
		})
	}
	wg.Wait()

	if got := countQuotes(t, s, "SELECT count(*) FROM quotes"); got != workers*2 {
		t.Errorf("stored %d rows, want %d", got, workers*2)
	}
}

// --- Close ---

func TestWriteBatchAfterCloseFails(t *testing.T) {
	requireDB(t)
	dropQuotes(t)

	s, err := Open(testDSN)
	if err != nil {
		t.Fatalf("Open() = %v, want nil", err)
	}
	t.Cleanup(func() { dropQuotes(t) })

	if err := s.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	err = s.WriteBatch(context.Background(), []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100),
	})
	if err == nil {
		t.Fatal("WriteBatch() after Close = nil error, want one")
	}
}

// Close stops the partition roller and drains it, so it must be safe for the
// shutdown path to call more than once.
func TestCloseIsIdempotent(t *testing.T) {
	requireDB(t)
	dropQuotes(t)

	s, err := Open(testDSN)
	if err != nil {
		t.Fatalf("Open() = %v, want nil", err)
	}
	t.Cleanup(func() { dropQuotes(t) })

	if err := s.Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close() = %v, want nil", err)
	}
}
