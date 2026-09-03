package ndjson

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/quotetest"
	"github.com/samuel-fonseca/driftwatch/internal/store"
)

var _ store.Store = (*Store)(nil)

// --- test helpers ---

// open creates a store under a temp path and returns both, closing the store
// with the test unless it was closed already (Close is idempotent).
func open(t *testing.T) (*Store, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "quotes.ndjson")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%s) = %v, want nil", path, err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

// readLines reads every line of the file, for asserting after a Close.
func readLines(t *testing.T, path string) []string {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s for verification: %v", path, err)
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning %s: %v", path, err)
	}
	return lines
}

// --- Open ---

func TestOpenRejectsAnUnusablePath(t *testing.T) {
	// A directory that does not exist cannot hold the file, and failing at
	// Open is the only chance to say so before quotes start arriving.
	path := filepath.Join(t.TempDir(), "no-such-dir", "quotes.ndjson")

	if s, err := Open(path); err == nil {
		s.Close()
		t.Error("Open() = nil error for an unwritable path, want an error")
	}
}

// --- WriteBatch ---

// Data is buffered until Close, so the assertions about file contents only
// hold afterwards. Both halves are checked here rather than in two tests,
// because the buffering is only interesting as the first half of this story.
func TestWriteBatchIsBufferedUntilCloseThenFlushed(t *testing.T) {
	s, path := open(t)

	batch := []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100, quotetest.Size(1)),
		quotetest.Ask("binance", "BTC-USD", 101, quotetest.Size(2)),
	}
	if err := s.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("WriteBatch() = %v, want nil", err)
	}

	if got, err := os.ReadFile(path); err != nil {
		t.Fatalf("ReadFile: %v", err)
	} else if len(got) > 0 {
		t.Errorf("file holds %d bytes before Close, want it still buffered", len(got))
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	lines := readLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("file holds %d lines, want 2", len(lines))
	}

	var got quote.Quote
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("line 0 is not valid JSON: %v (%q)", err, lines[0])
	}
	if got.Venue != "binance" || got.Market != "BTC-USD" || got.Selection != "bid" || got.Price != 100 {
		t.Errorf("line 0 decoded to %+v, want the bid that was written", got)
	}
}

// One line per quote, in the order written -- the "ND" in NDJSON.
func TestWriteBatchWritesOneLinePerQuoteInOrder(t *testing.T) {
	s, path := open(t)

	want := []string{"bid", "ask", "bid"}
	batch := []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100),
		quotetest.Ask("binance", "BTC-USD", 101),
		quotetest.Bid("kraken", "ETH-USD", 50),
	}
	if err := s.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("WriteBatch() = %v, want nil", err)
	}
	s.Close()

	lines := readLines(t, path)
	if len(lines) != len(want) {
		t.Fatalf("file holds %d lines, want %d", len(lines), len(want))
	}
	for i, line := range lines {
		var q quote.Quote
		if err := json.Unmarshal([]byte(line), &q); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", i, err)
		}
		if q.Selection != want[i] {
			t.Errorf("line %d selection = %q, want %q", i, q.Selection, want[i])
		}
	}
}

// A restart must extend the day's file, not truncate it.
func TestWriteBatchAppendsAcrossOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quotes.ndjson")

	for _, venue := range []string{"binance", "bitfinex"} {
		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open(%s) = %v, want nil", venue, err)
		}
		if err := s.WriteBatch(context.Background(), []quote.Quote{
			quotetest.Bid(venue, "BTC-USD", 100),
		}); err != nil {
			t.Fatalf("WriteBatch(%s) = %v, want nil", venue, err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close(%s) = %v, want nil", venue, err)
		}
	}

	if lines := readLines(t, path); len(lines) != 2 {
		t.Fatalf("file holds %d lines across two Opens, want 2 (append, not truncate)", len(lines))
	}
}

// The pipeline calls WriteBatch with whatever survived dedupe, which is
// routinely nothing.
func TestWriteBatchEmptyBatchIsNoOp(t *testing.T) {
	s, path := open(t)

	if err := s.WriteBatch(context.Background(), nil); err != nil {
		t.Fatalf("WriteBatch(nil) = %v, want nil", err)
	}
	s.Close()

	if lines := readLines(t, path); len(lines) != 0 {
		t.Fatalf("file holds %d lines from an empty batch, want 0", len(lines))
	}
}

// Several pipeline workers share one store, so the writes are serialised
// under a mutex. Meaningful under -race: an unsynchronised bufio.Writer
// interleaves partial lines and corrupts the file.
func TestConcurrentWriteBatchIsSafe(t *testing.T) {
	s, path := open(t)

	const (
		writers        = 10
		quotesPerWrite = 4
	)

	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			batch := []quote.Quote{
				quotetest.Ask("binance", "BTC-USD", 100*float64(i)),
				quotetest.Bid("binance", "BTC-USD", 90*float64(i)),
				quotetest.Ask("binance", "DOGE-USD", 100*float64(i)),
				quotetest.Bid("binance", "DOGE-USD", 90*float64(i)),
			}
			if err := s.WriteBatch(context.Background(), batch); err != nil {
				t.Errorf("WriteBatch() = %v, want nil", err)
			}
		})
	}
	wg.Wait()
	s.Close()

	lines := readLines(t, path)
	if want := writers * quotesPerWrite; len(lines) != want {
		t.Fatalf("file holds %d lines, want %d", len(lines), want)
	}
	// Every line must be independently decodable: a torn write shows up
	// here rather than as a silently corrupt data file.
	for i, line := range lines {
		var q quote.Quote
		if err := json.Unmarshal([]byte(line), &q); err != nil {
			t.Fatalf("line %d is not valid JSON: %v (%q)", i, err, line)
		}
	}
}

// --- Close ---

// A late write used to land in the flushed bufio.Writer over a closed file,
// return nil, and vanish at exit. Reporting the write as failed is the only
// way a caller can tell its data did not survive.
func TestWriteBatchAfterCloseIsRejected(t *testing.T) {
	s, path := open(t)

	if err := s.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	err := s.WriteBatch(context.Background(), []quote.Quote{
		quotetest.Bid("binance", "BTC-USD", 100),
	})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("WriteBatch() after Close = %v, want %v", err, ErrClosed)
	}
	if lines := readLines(t, path); len(lines) != 0 {
		t.Errorf("file holds %d lines, want 0 -- the rejected write must not appear", len(lines))
	}
}

// Shutdown paths can close twice: main defers Close and the pipeline test
// closes explicitly. The second must be a no-op rather than a double-close
// error on the underlying file.
func TestCloseIsIdempotent(t *testing.T) {
	s, _ := open(t)

	if err := s.Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close() = %v, want nil", err)
	}
}

// Close must not tear the file out from under a write already in progress.
// Meaningful under -race; the assertion is that everything either lands or
// is refused, with nothing silently lost in between.
func TestCloseRacesWithInFlightWriteBatch(t *testing.T) {
	s, path := open(t)

	const attempts = 200

	var (
		mu               sync.Mutex
		accepted         int
		rejected         int
		unexpectedErrors []error
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range attempts {
			err := s.WriteBatch(context.Background(), []quote.Quote{
				quotetest.Bid("binance", "BTC-USD", float64(i)),
			})

			mu.Lock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrClosed):
				rejected++
			default:
				unexpectedErrors = append(unexpectedErrors, err)
			}
			mu.Unlock()
		}
	}()

	s.Close() // races the loop above
	<-done

	mu.Lock()
	defer mu.Unlock()

	if len(unexpectedErrors) > 0 {
		t.Fatalf("WriteBatch returned %d unexpected errors, first: %v",
			len(unexpectedErrors), unexpectedErrors[0])
	}
	if accepted+rejected != attempts {
		t.Fatalf("accounted for %d of %d writes", accepted+rejected, attempts)
	}
	// Every accepted write was flushed; no accepted write went missing.
	if lines := readLines(t, path); len(lines) != accepted {
		t.Errorf("file holds %d lines, want %d -- accepted writes must survive Close", len(lines), accepted)
	}
}
