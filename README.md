# Driftwatch

Driftwatch is a multi-venue crypto quote ingest engine. It polls multiple exchanges for live bid/ask quotes, normalizes them onto a common market key, and watches for moments where the same asset is priced differently across venues.

## Overview

The pipeline, end to end:

1. **Ingest** — poll Binance and Bitfinex for live bid/ask quotes.
2. **Normalize** — map each venue's own symbol format onto a canonical `BASE-QUOTE` market (e.g. Binance's `BTCUSDT` and Bitfinex's `tBTCUSD` both become `BTC-USD`), so the same asset can be compared across venues.
3. **Coalesce** — collapse bursts of updates for the same instrument into a single latest-value slot, bounded by the number of distinct live markets rather than the raw event rate.
4. **Dedupe** — drop quotes whose price and size haven't actually changed since the last time that instrument was seen.
5. **Persist** — write surviving quotes to disk as newline-delimited JSON.
6. **Detect divergence** — track the best bid and best ask per market across venues, and emit a signal when one venue's bid crosses another venue's ask.
7. **Stream** — fan signals out to subscribers over Server-Sent Events, without letting a slow subscriber degrade ingest.

Ingest is currently REST-polling based; a WebSocket-based ingest path would be a natural next step and should only improve on the throughput numbers below. The project has zero third-party dependencies — everything is built on the Go standard library.

## Features

- **Multi-venue ingest** through a single `Source` interface — Binance and Bitfinex REST pollers, each with jittered exponential backoff on failure and a reused, keep-alive HTTP client.
- **Symbol normalization** to canonical `BASE-QUOTE` markets — longest-match-first suffix stripping and stablecoin collapsing (USDT/USDC/etc. → USD) so venues actually line up.
- **Coalescing buffer** — bounded by distinct keys, not event rate; a `Push` for a key already pending replaces the value in place and never blocks; at capacity, the *oldest* pending entry is evicted to make room for a new key.
- **Change-detection dedupe** — a fingerprint (hash of price + size) per instrument, LRU-capped, that turns a poll-rate-bound write volume into a change-rate-bound one.
- **Cross-venue divergence detection** — recomputes best bid/ask per market on every surviving quote, with suppression for same-venue legs, sub-threshold edges, and stale legs.
- **NDJSON persistence** — batched writes through a buffered writer.
- **SSE fan-out hub** — small bounded channel per subscriber, non-blocking publish, and a drop-budget that disconnects a subscriber that's fallen permanently behind, so no single slow client can degrade ingest for everyone else.
- **Live profiling** — a `/stats` JSON endpoint and `net/http/pprof` wired in from the start.

## Where things are

```
cmd/driftwatch/main.go     entrypoint: wiring, HTTP routes (/stream, /stats, /debug/pprof/*)

internal/quote/            the Quote type — Key(), MarketKey(), Fingerprint()
internal/normalize/        venue symbol → canonical market
internal/source/           the Source interface
  binance/                 Binance REST poller
  bitfinex/                Bitfinex REST poller (+ BenchmarkDecode)
internal/buffer/           the coalescing buffer
internal/dedupe/           change-detection filter
internal/divergence/       cross-venue divergence detector
internal/store/            the Store interface
  ndjson/                  NDJSON append-only writer
internal/hub/              SSE fan-out hub
internal/pipeline/         wires the above into one running pipeline
```

## Running it

```
go run ./cmd/driftwatch
```

Listens on `:8080`:

- `GET /stream` — Server-Sent Events stream of divergence signals
- `GET /stats` — JSON snapshot of pipeline counters
- `/debug/pprof/*` — standard Go profiling endpoints

```
curl -N localhost:8080/stream
```

streams signals live as they're detected.

## Testing

```
go test -race ./...
```

Coverage includes:

- table-driven symbol normalization tests, including the Bitfinex funding-row edge case and unsplittable symbols
- decoder tests against inline JSON fixtures, including zero-priced rows being dropped
- buffer tests for coalescing, position retention under a hot key, oldest-first eviction, batch limits, blocking-then-waking, and a concurrent accounting test
- dedupe tests for fingerprint-based change detection and LRU eviction
- a divergence test asserting a signal fires correctly across three venues
- hub tests for fan-out to multiple subscribers and a wedged subscriber never blocking publish
- NDJSON store tests, including a close racing an in-flight write

There's also one benchmark in the repo, covering the Bitfinex JSON decode path:

```
go test -bench=BenchmarkDecode -benchmem -run=^$ ./internal/source/[bitfinex|binance]
```

Optionally add `-memprofile` to get memory usage stats:

```
go test -bench=BenchmarkDecode -benchmem -run=^$ ./internal/source/[bitfinex|binance] -memprofile=data/decode-[bitfinex|binance].mem.out
```

## Current stats

**Throughput.** Observed live: ~5,586 quotes/sec, measured via two `/stats` snapshots one second apart (589,414 → 595,000 pushed), on the current REST-polling ingest path.

**Coalescing buffer.** Across a live run of 600,000+ processed quotes, verified against real, concurrent, adversarial network data (not just the synthetic `-race` test):

- The accounting invariant `Coalesced + Evicted + Taken + Depth == Pushed` held exactly.
- `MaxDepth` peaked at 687 against a configured capacity of 16,384 — nowhere near backpressure at this load.
- `Evicted`: 0 — no data loss from the coalescing buffer across the entire run.

**Change detection.** ~66% of inbound quotes were discarded as unchanged (397,319 quotes taken from the buffer → ~133,000 lines written to disk).

**SSE hub.** Verified functionally correct with 50 live concurrent subscribers — `Dropped`: 0, `Evicted`: 0. Not yet load-tested with a large number of concurrent subscribers.

**Resource usage.**

- CPU: 3.36% utilization sampled over a 30-second live pprof capture at this load — not CPU-bound at this scale.
- Heap: ~9.4MB in-use after ~600k quotes processed — stable, no evidence of unbounded growth. (One caveat: the divergence detector's per-symbol latest-quote map is unbounded by design; in practice it's naturally capped by the number of distinct symbols across venues, not by tick volume.)

## Known limitations

- Prices are carried as `float64` — a deliberate simplification, not financial-grade precision.
- Collapsing stablecoins (USDT/USDC/etc.) to USD is what makes cross-venue comparison possible, but a stablecoin depeg can look exactly like a real arbitrage signal.
- The divergence detector's per-symbol map is unbounded by design, capped only by the number of distinct symbols across venues rather than by tick volume.
- The SSE hub hasn't been load-tested with a large number of concurrent subscribers yet.
- Dedupe currently discards ~66% of inbound quotes as unchanged.
- There's no message broker in front of ingest — the coalescing buffer's own backpressure handling (drop the oldest stale entry, never the newest) is the load-bearing design choice here, by intent.
- Ingest is REST-polling based today; a WebSocket ingest path and additional storage backends are natural next steps.
