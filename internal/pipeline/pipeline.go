package pipeline

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/buffer"
	"github.com/samuel-fonseca/driftwatch/internal/dedupe"
	"github.com/samuel-fonseca/driftwatch/internal/divergence"
	"github.com/samuel-fonseca/driftwatch/internal/hub"
	"github.com/samuel-fonseca/driftwatch/internal/quote"
	"github.com/samuel-fonseca/driftwatch/internal/source"
	"github.com/samuel-fonseca/driftwatch/internal/store"
)

type Config struct {
	Sources []source.Source
	Store   store.Store
	Hub     *hub.Hub

	BufferCapacity   int
	DedupeCapacity   int
	EdgeThresholdBps float64
	StaleThreshold   time.Duration
	NumWorkers       int
	BatchSize        int
	RawChannelSize   int
}

type Pipeline struct {
	cfg   Config
	buf   *buffer.Buffer
	dedup *dedupe.Detector
	div   *divergence.Detector
}

func New(cfg Config) *Pipeline {
	if cfg.BufferCapacity == 0 {
		cfg.BufferCapacity = 16384
	}
	if cfg.DedupeCapacity == 0 {
		cfg.DedupeCapacity = 16384
	}
	if cfg.EdgeThresholdBps == 0 {
		cfg.EdgeThresholdBps = 5
	}
	if cfg.StaleThreshold == 0 {
		cfg.StaleThreshold = 2 * time.Second
	}
	if cfg.NumWorkers == 0 {
		cfg.NumWorkers = 4
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 256
	}
	if cfg.RawChannelSize == 0 {
		cfg.RawChannelSize = 4096
	}

	return &Pipeline{
		cfg:   cfg,
		buf:   buffer.New(cfg.BufferCapacity),
		dedup: dedupe.New(cfg.DedupeCapacity),
		div:   divergence.New(cfg.EdgeThresholdBps, cfg.StaleThreshold),
	}
}

type Stats struct {
	Buffer buffer.Stats `json:"buffer"`
	Hub    hub.Stats    `json:"hub"`
}

func (p *Pipeline) Stats() Stats {
	return Stats{
		Buffer: p.buf.Stats(),
		Hub:    p.cfg.Hub.Stats(),
	}
}

func (p *Pipeline) Run(ctx context.Context) error {
	raw := make(chan quote.Quote, p.cfg.RawChannelSize)

	var wg sync.WaitGroup

	for _, src := range p.cfg.Sources {
		wg.Add(1)
		go func(src source.Source) {
			defer wg.Done()
			err := src.Run(ctx, raw)
			if err != nil {
				log.Printf("failed to run source %s: %v", src.Name(), err)
			}
		}(src)
	}

	wg.Add(1)
	wg.Go(func() {
		defer wg.Done()
		for {
			select {
			case q := <-raw:
				p.buf.Push(q)
			case <-ctx.Done():
				return
			}
		}
	})

	for range p.cfg.NumWorkers {
		wg.Add(1)
		wg.Go(func() {
			defer wg.Done()
			p.worker(ctx)
		})
	}

	return nil
}

func (p *Pipeline) worker(ctx context.Context) {
	for {
		batch, err := p.buf.TakeBatch(ctx, p.cfg.BatchSize)
		if err != nil {
			return
		}

		survivors := p.dedup.FilterChanged(batch)
		if len(survivors) == 0 {
			continue
		}

		if err := p.cfg.Store.WriteBatch(ctx, survivors); err != nil {
			log.Printf("store write failed: %v", err)
		}

		for _, q := range survivors {
			sig := p.div.Observe(q)
			if sig == nil {
				continue
			}
			data, err := json.Marshal(sig)
			if err != nil {
				log.Printf("json marshal failed: %v", err)
				continue
			}
			p.cfg.Hub.Publish(hub.Event{Name: "signal", Data: data})
		}
	}
}
