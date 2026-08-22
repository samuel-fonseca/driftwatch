package ndjson

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

// readLines reads every line of a file into a slice, for asserting
// against after a WriteBatch + Close.
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

func TestWriteBatchThenCloseProducesValidNDJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quotes.ndjson")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	batch := []quote.Quote{
		{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1, ObservedAt: time.Now()},
		{Venue: "binance", Market: "BTC-USD", Selection: "ask", Price: 101, Size: 2, ObservedAt: time.Now()},
	}

	if err := s.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	lines := readLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}

	var got quote.Quote
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("line 0 is not valid JSON: %v (line was: %s)", err, lines[0])
	}
	if got.Venue != "binance" || got.Market != "BTC-USD" || got.Selection != "bid" || got.Price != 100 {
		t.Errorf("line 0 decoded to %+v, unexpected fields", got)
	}
}

func TestWriteBatchAppendsAcrossMultipleOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quotes.ndjson")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open (1st): %v", err)
	}
	s1.WriteBatch(context.Background(), []quote.Quote{{Venue: "binance", Market: "BTC-USD", Selection: "bid"}})
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open (2nd): %v", err)
	}
	s2.WriteBatch(context.Background(), []quote.Quote{{Venue: "bitfinex", Market: "ETH-USD", Selection: "ask"}})
	s2.Close()

	lines := readLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("got %d lines across two Opens, want 2 (append, not truncate)", len(lines))
	}
}

func TestWriteBatchEmptyBatchIsNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quotes.ndjson")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.WriteBatch(context.Background(), nil); err != nil {
		t.Fatalf("WriteBatch(nil) should be a no-op, got error: %v", err)
	}
	s.Close()

	lines := readLines(t, path)
	if len(lines) != 0 {
		t.Fatalf("got %d lines from an empty batch, want 0", len(lines))
	}
}

func TestDataIsBufferedUntilClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quotes.ndjson")
	quotes := []quote.Quote{{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: 100, Size: 1, ObservedAt: time.Now()}}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.WriteBatch(context.Background(), quotes); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	emptyBytes, emptyErr := os.ReadFile(path)
	if emptyErr != nil {
		t.Fatalf("ReadFile: %v", emptyErr)
	}
	if len(emptyBytes) > 0 {
		t.Errorf("expected no data, got %d bytes", len(emptyBytes))
	}

	s.Close()

	nonEmptyBytes, nonEmptyErr := os.ReadFile(path)
	if nonEmptyErr != nil {
		t.Fatalf("ReadFile: %v", nonEmptyErr)
	}
	if len(nonEmptyBytes) == 0 {
		t.Errorf("expected data, got none")
	}
}

func TestConcurrentWriteBatchIsSafe(t *testing.T) {
	var wg sync.WaitGroup
	path := filepath.Join(t.TempDir(), "quotes.ndjson")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := range 10 {
		wg.Add(1)
		batch := []quote.Quote{
			{Venue: "binance", Market: "BTC-USD", Selection: "ask", Price: (100.0 * float64(i)), Size: 24.0, ObservedAt: time.Now(), ReceivedAt: time.Now()},
			{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: (90.0 * float64(i)), Size: 51.0, ObservedAt: time.Now(), ReceivedAt: time.Now()},
			{Venue: "binance", Market: "DOGE-USD", Selection: "ask", Price: (100.0 * float64(i)), Size: 23.0, ObservedAt: time.Now(), ReceivedAt: time.Now()},
			{Venue: "binance", Market: "DOGE-USD", Selection: "bid", Price: (90.0 * float64(i)), Size: 24.0, ObservedAt: time.Now(), ReceivedAt: time.Now()},
		}

		go func(s *Store, batch []quote.Quote) {
			defer wg.Done()
			if err := s.WriteBatch(context.Background(), batch); err != nil {
				t.Errorf("WriteBatch: %v", err)
			}
		}(s, batch)
	}

	wg.Wait()
	s.Close()

	lines := readLines(t, path)
	if len(lines) != 40 {
		t.Errorf("expected 40 lines, got %d", len(lines))
	}
}

func TestCloseRacesWithInFlightWriteBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quotes.ndjson")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			s.WriteBatch(context.Background(), []quote.Quote{
				{Venue: "binance", Market: "BTC-USD", Selection: "bid", Price: float64(i)},
			})
		}
	}()

	time.Sleep(time.Millisecond) // let the writer goroutine get going
	s.Close()                    // races with the in-flight WriteBatch loop above
	<-done
}
