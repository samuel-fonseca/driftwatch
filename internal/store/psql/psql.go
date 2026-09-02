package psql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samuel-fonseca/driftwatch/internal/backoff"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

const (
	createTableQuery = "CREATE TABLE IF NOT EXISTS quotes (" +
		"venue       text        NOT NULL," +
		"market      text        NOT NULL," +
		"selection   text        NOT NULL," +
		"price       double precision NOT NULL," +
		"size        double precision," +
		"observed_at timestamptz NOT NULL" +
		") PARTITION BY RANGE (observed_at);" +
		"CREATE UNIQUE INDEX IF NOT EXISTS quotes_unique_idx ON quotes (venue, market, selection, observed_at);" +
		"CREATE INDEX IF NOT EXISTS quotes_market_observed_at_idx ON quotes (market, observed_at DESC);"
	createDailyPartitionQuery = `CREATE TABLE IF NOT EXISTS quotes_%[1]s
		PARTITION OF quotes
		FOR VALUES FROM ('%[2]s') TO ('%[3]s');`
)

type Store struct {
	pool   *pgxpool.Pool
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// open a new connection to PSQL server
func Open(connStr string) (*Store, error) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if err := ensureBaseTablesExist(ctx, pool); err != nil {
		return nil, fmt.Errorf("failed to ensure base tables exist: %w", err)
	}

	now := time.Now().UTC()
	if err := ensurePartition(ctx, pool, now); err != nil {
		return nil, fmt.Errorf("failed to create today's partition: %w", err)
	}
	if err := ensurePartition(ctx, pool, now.Add(24*time.Hour)); err != nil {
		return nil, fmt.Errorf("failed to create tomorrow's partition: %w", err)
	}

	schedCtx, cancel := context.WithCancel(context.Background())
	s := &Store{
		pool:   pool,
		cancel: cancel,
	}

	s.wg.Add(1)
	go s.rollPartitions(schedCtx)

	return s, nil
}

func ensureBaseTablesExist(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, createTableQuery)
	if err != nil {
		return err
	}
	return nil
}

// ensurePartition creates the daily partition covering the UTC calendar day
// containing t, if it doesn't already exist.
func ensurePartition(ctx context.Context, pool *pgxpool.Pool, t time.Time) error {
	day := t.UTC().Truncate(24 * time.Hour)
	suffix := day.Format("20060102")
	from := day.Format("2006-01-02")
	to := day.Add(24 * time.Hour).Format("2006-01-02")

	query := fmt.Sprintf(createDailyPartitionQuery, suffix, from, to)
	_, err := pool.Exec(ctx, query)
	return err
}

// rollPartitions runs until ctx is cancelled, waking up at every UTC
// midnight to make sure tomorrow's partition exists ahead of time. Combined
// with the two partitions Open creates up front, this keeps today's and
// tomorrow's partitions always present so WriteBatch never fails because a
// day rolled over.
func (s *Store) rollPartitions(ctx context.Context) {
	defer s.wg.Done()

	for {
		timer := time.NewTimer(time.Until(nextMidnightUTC(time.Now().UTC())))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := s.rollOnce(context.Background()); err != nil {
				log.Printf("psql: failed to roll partition: %v", err)
			}
		}
	}
}

// rollOnce ensures tomorrow's partition exists, relative to the current
// time.
func (s *Store) rollOnce(ctx context.Context) error {
	return ensurePartition(ctx, s.pool, time.Now().UTC().Add(24*time.Hour))
}

// nextMidnightUTC returns the next UTC midnight strictly after now.
func nextMidnightUTC(now time.Time) time.Time {
	y, m, d := now.Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, time.UTC)
}

func (s *Store) WriteBatch(ctx context.Context, batch []quote.Quote) error {
	return retryOnDeadlock(ctx, func(ctx context.Context) error {
		return s.writeBatchOnce(ctx, batch)
	})
}

const (
	deadlockCode       = "40P01"
	maxDeadlockRetries = 5
	deadlockBackoff    = 10 * time.Millisecond
)

func retryOnDeadlock(ctx context.Context, fn func(context.Context) error) error {
	wait := deadlockBackoff
	for range maxDeadlockRetries {
		err := fn(ctx)
		if err == nil {
			return nil
		}

		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != deadlockCode {
			return err // not a deadlock -- don't retry
		}

		if sleepErr := backoff.Sleep(ctx, backoff.Jitter(wait)); sleepErr != nil {
			return sleepErr
		}
		wait *= 2
	}
	return fmt.Errorf("exhausted %d retries after repeated deadlocks", maxDeadlockRetries)
}

func (s *Store) writeBatchOnce(ctx context.Context, batch []quote.Quote) error {
	if len(batch) == 0 {
		return nil
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `CREATE TEMP TABLE quotes_staging (LIKE quotes) ON COMMIT DROP`)
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	rows := make([][]any, len(batch))
	for i, q := range batch {
		rows[i] = []any{q.Venue, q.Market, q.Selection, q.Price, q.Size, q.ObservedAt}
	}
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"quotes_staging"},
		[]string{"venue", "market", "selection", "price", "size", "observed_at"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("failed to copy to temp table: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO quotes (venue, market, selection, price, size, observed_at)
		SELECT venue, market, selection, price, size, observed_at FROM quotes_staging
		ON CONFLICT (venue, market, selection, observed_at) DO NOTHING`)
	if err != nil {
		return fmt.Errorf("failed to insert from temp table: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *Store) Close() error {
	s.cancel()
	s.wg.Wait()
	s.pool.Close()
	return nil
}
