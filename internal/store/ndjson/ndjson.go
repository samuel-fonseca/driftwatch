package ndjson

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

const bufferSize = 256 * 1024

type Store struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
}

func Open(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	return &Store{
		file:   f,
		writer: bufio.NewWriterSize(f, bufferSize),
	}, nil
}

// WriteBatch writes a batch of quotes to the JSON file.
func (s *Store) WriteBatch(ctx context.Context, batch []quote.Quote) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, q := range batch {
		data, err := json.Marshal(q)
		if err != nil {
			return fmt.Errorf("marshalling quote: %w", err)
		}

		if _, err := s.writer.Write(data); err != nil {
			return fmt.Errorf("writing quote: %w", err)
		}

		if _, err := s.writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("writing newline: %w", err)
		}
	}
	return nil
}

// Flush the writer and close the file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	flushErr := s.writer.Flush()
	closeErr := s.file.Close()
	err := errors.Join(flushErr, closeErr)
	if err != nil {
		return fmt.Errorf("closing store: %w", err)
	}
	return nil
}
