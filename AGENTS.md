# AGENTS.md

Conventions and invariants for agents working in this repo. Setup, endpoints
and the contributor workflow are in [README.md](README.md) — this file covers
only what you cannot infer from the code.

## Layout

| Package | Role |
| --- | --- |
| `internal/quote` | `Quote`, plus `Key()` (identity) and `Fingerprint()` (value) |
| `internal/normalize` | Asset canonicalisation, `Market()`, `QuoteClass()` |
| `internal/symbols` | `Registry`: venue-native symbol → `Instrument`, refreshed on a ticker |
| `internal/source` | The `Source` interface every venue satisfies |
| `internal/source/poller` | Shared REST poll loop, `HTTP`, `Tick`, `ParseFunc`, `Tuning` |
| `internal/source/{binance,bitfinex,kraken}` | Per-venue tick parsing and symbol loading, wrapped in a `poller` |
| `internal/buffer` | Coalescing FIFO keyed by `Quote.Key()` |
| `internal/dedupe` | LRU fingerprint cache; drops quotes whose price and size are unchanged |
| `internal/divergence` | Cross-venue crossed-book detector, with per-reason suppression counters |
| `internal/hub` | SSE fan-out with per-subscriber drop budget |
| `internal/store{,/ndjson,/psql}` | `Store` interface and its two implementations |
| `internal/metrics` | Prometheus collector over each stage's `Stats()` |
| `internal/pipeline` | Wires the stages together and owns the goroutines |

Two `main` packages: `./cmd/driftwatch` is the service; `./cmd` is a standalone
SSE load generator.

Data flow: **sources → `raw` chan → buffer → workers → dedupe → store →
divergence → hub**.

## Conventions

- Standard library only in production code beyond pgx, prometheus and
  godotenv. Tests use the stdlib `testing` package — there is no assertion
  library, and `testify` is an indirect dependency of testcontainers, not a
  sanctioned import.
- Wrap errors with `%w` and a phrase naming the operation
  (`fmt.Errorf("parsing tickers: %w", err)`).
- Prefer an unexported seam over an exported knob when something needs to be
  testable. `hub.subscribeWithBuffer` and `Hub.heartbeat` exist for tests but
  change no public API.
- Config structs default themselves through a `WithDefaults`/`ApplyDefaults`
  method built on `cmp.Or`, so the methods must stay idempotent.
- Comments explain *why*, especially where a venue quirk or a past bug drove
  the code. Do not strip them when refactoring; they are the record of what
  went wrong last time.

## Testing

`go test -race ./...` is the bar, and CI runs exactly that. The suite must also
survive `-count=3` — repeated runs are how the no-sleep rule below is enforced.

- **Write the test first for behavior changes.** New behavior or a bug fix
  starts with a test that fails for the right reason, then the code that makes
  it pass. Refactors are the exception: keep the existing tests green and
  unchanged — a refactor whose tests need rewriting is a behavior change.
- **Table-driven by default.** Prefer one table with named subtests over
  several near-identical functions.
- **Assert the reason, not just the outcome.** The divergence suppression
  tests compare `Stats()` deltas so that a quote suppressed for the *wrong*
  reason fails. A bare `sig == nil` check passes either way and hides
  misclassification.
- **Never `time.Sleep` to synchronise.** Poll a predicate (`waitFor`,
  `waitForSubscribers`, `waitForRequests`) or wait on a real signal. Sleeps
  are acceptable only to prove a negative — that something has *not* happened
  yet.
- **Use the test-helper packages** rather than re-rolling fixtures:
  - `internal/quotetest` — `Bid`/`Ask`/`Sel` with `Size`/`At`/`Ago` options.
  - `internal/source/sourcetest` — `Server`/`StatusServer` HTTP fakes.
  - `internal/hub/hubtest` — an SSE client that parses real frames.
  None are imported by production code.
- **Test through the public API.** Do not walk `container/list` internals or
  declare methods on production types from a test file; drain the buffer with
  `TakeBatch` and read counters through `Stats()`.
- `internal/store/psql` needs Docker (testcontainers). Without it each test
  skips with a reason — do not make it silently report zero tests.

## Invariants that break silently

Each of these has cost real debugging time. Changing any one needs a test that
would fail if it regressed.

- **`pipeline.New` registers a collector into a registry, and registries reject
  duplicates.** Building two pipelines in one process panics unless each gets
  its own `Config.Registry`. Tests must pass `prometheus.NewPedanticRegistry()`.
- **`pipeline.Run` must block until every goroutine returns.** `main` closes the
  store as soon as it returns; an early return means writing to a closed store.
- **Venue liveness comes from `MarkAlive`, not from the stored quote.** Dedupe
  removes unchanged repeats before `Observe` sees them, so a venue holding a
  steady price has a frozen `ObservedAt` while being perfectly alive. Judging
  staleness from the stored quote suppresses good signals.
- **`Market()` must never collapse stablecoins.** `BTC-USDT` and `BTC-USD` are
  different books; merging them makes venues overwrite each other. `QuoteClass`
  is the only sanctioned place they may be treated as equivalent.
- **Symbol tables key on the venue-native ticker symbol.** Kraken's `XXBTZUSD`,
  not its `altname` or `wsname`; Bitfinex keys carry the `t` prefix. Key on the
  wrong field and the table matches nothing, with no error anywhere.
- **A symbol loader that returns an empty table must error.** A silently empty
  table means every lookup misses and the venue publishes nothing forever
  without a single log line or backoff.
- **Parsers skip unreadable rows, never guess.** A row with one bad field is
  dropped whole; a half-read book publishes a side the venue never sent.
- **Tuning defaults are load-bearing.** `backoff.Next` doubles, and doubling
  zero stays zero, so a zero `InitialBackoff` retries a failing venue in a hot
  loop. `poller.New` re-applies `Tuning.WithDefaults()` for exactly this reason.
- **Buffer coalescing keeps the entry's queue position.** Moving it would let a
  hot key starve everything behind it.
- **`ndjson` rejects writes after `Close`.** They previously landed in the
  flushed `bufio.Writer` over a closed file, returned `nil`, and were lost.
