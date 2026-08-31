# Driftwatch

Driftwatch is a multi-venue crypto quote ingest engine written in Go. It polls
several exchanges for live bid/ask quotes, normalizes each venue's symbols onto
a common market key, and emits a signal whenever the same asset is priced
differently across venues.

The pipeline is roughly: **ingest → normalize → coalesce → dedupe → persist →
detect divergence → stream**. Signals are fanned out over Server-Sent Events,
with backpressure handled so a slow subscriber can't degrade ingest for anyone
else. Everything outside of storage and metrics is built on the Go standard
library.

## Getting started

Requires Go 1.26+ and Docker.

```sh
git clone https://github.com/samuel-fonseca/driftwatch.git
cd driftwatch
cp .env.example .env
docker compose up -d
```

The service listens on `:8080` (configurable via `APP_PORT`):

| Endpoint          | Description                              |
| ----------------- | ---------------------------------------- |
| `/stream`         | SSE stream of divergence signals         |
| `/stats`          | JSON snapshot of pipeline counters       |
| `/metrics`        | Prometheus metrics                       |
| `/debug/pprof/*`  | Standard Go profiling endpoints          |

Watch signals as they're detected:

```sh
curl -N localhost:8080/stream
```

To run it directly instead of in Docker, set `DATABASE_URL` and:

```sh
go run ./cmd/driftwatch
```

## Development

```sh
go test -race ./...          # full suite
go vet ./...
go test -bench=. -benchmem -run=^$ ./...
```

CI runs build, vet, and the race-enabled test suite on every push and pull
request to `main`.

## Contributing

Issues and pull requests are welcome.

1. Fork the repo and branch off `main`.
2. Keep changes focused, and add tests alongside them — most packages here are
   table-driven and concurrency-sensitive, so `go test -race ./...` must pass.
3. Run `go vet ./...` and `gofmt` before opening a PR.
4. Open a pull request describing what changed and why.

New venues are the easiest place to start: implement the `Source` interface in
`internal/source/`, add the venue's symbol rules to `internal/normalize/`, and
wire it up in `cmd/driftwatch/main.go`.

## Known limitations

- Prices are carried as `float64` — a deliberate simplification, not
  financial-grade precision.
- Stablecoins (USDT/USDC/etc.) collapse to USD so venues line up, which means a
  depeg can look like a real arbitrage signal.
- Ingest is REST-polling based today; a WebSocket path is the natural next step.

## License

MIT — see [LICENSE](LICENSE).
