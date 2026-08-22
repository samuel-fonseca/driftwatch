package store

import (
	"context"

	"github.com/samuel-fonseca/driftwatch/internal/quote"
)

type Store interface {
	WriteBatch(ctx context.Context, batch []quote.Quote) error
	Close() error
}
