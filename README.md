<div align="center">
  <h1>Solana Realtime Indexer</h1>
  <p><strong>Real-time pump.fun and PumpSwap trade indexer in Rust</strong></p>
  <p>Streams over Yellowstone gRPC, decodes with
     <a href="https://github.com/sevenlabs-hq/carbon">Carbon</a>, and stores in Postgres.</p>

  <p>
    <img alt="ci" src="https://github.com/shaurya35/solana-realtime-indexer/actions/workflows/ci.yml/badge.svg">
    <img alt="license" src="https://img.shields.io/badge/license-MIT-blue">
    <img alt="release" src="https://img.shields.io/github/v/release/shaurya35/solana-realtime-indexer">
  </p>

  <p>
    <a href="#demo">Demo</a> ·
    <a href="#how-it-works">How it works</a> ·
    <a href="#try-it">Try it</a> ·
    <a href="#query-it">Query it</a> ·
    <a href="BENCHMARKS.md">Benchmarks</a> ·
    <a href="DESIGN.md">Design</a>
  </p>
</div>

---

## Demo

**[Watch the demo](https://youtu.be/URRNNI0bn_Q)** (3 minutes)

Walks through starting the indexer with a single command, decoding live trades from mainnet, and automatically detecting and backfilling a gap in the stream.

## How it works

![The path a trade takes, including repair](docs/images/pipeline-overview.png)

Trades arrive over gRPC, are decoded, batched in memory, and committed to Postgres. If the stream disconnects, missing slots are logged, refetched from RPC, and reconciled against the database.

![One decode path, three ways in](docs/images/three-way-architecture.png)

Live streams, local file replays, and slot-range backfills all enter through the same core function: `run_pipeline`. The pipeline treats every data source uniformly.

Because live ingestion, gap recovery, and test suites run through this identical execution path, the decoding logic cannot drift.

Regardless of input source, every transaction goes through four steps:

![What happens to a single trade](docs/images/single-transaction-architecture.png)

**Decode.** Raw wire bytes are parsed into typed structs using dedicated per-program decoders.

**Interpret.** pump.fun trades use a bonding curve where the mint is present in the instruction. PumpSwap pools use base and quote vaults without specifying which holds SOL, so vault orientation is resolved asynchronously off the hot path.

**Hand off.** The worker pushes parsed rows into a bounded channel and immediately returns to stream ingestion—it never blocks on database I/O.

**Write.** A dedicated writer task batches queued rows and commits them alongside the stream checkpoint inside a single transaction. The checkpoint marker can never advance beyond what has been written to disk.

Gap detection, recovery, offline verification (`verify`), the query API, and Prometheus metrics operate as decoupled auxiliary systems around this core loop.

## Where the code is

```
# src/pipeline.rs
Unified pipeline entrypoint (run_pipeline) handling all input sources.

# src/datasources/
Input adapters: live Yellowstone gRPC, recorded JSONL files, or RPC slot ranges.

# src/processors/
Program decoders: pumpfun.rs and pumpswap.rs.

# src/pools.rs
Pool orientation cache; determines which vault holds SOL for PumpSwap pools.

# src/writer.rs
Batch writer; persists trade rows and progress checkpoints atomically.

# src/gaps.rs
Stream continuity monitor; flags dropped slots and sequence breaks.

# src/recover.rs
Targeted RPC backfiller for missing slot ranges.

# src/verify.rs
Independent verification against local recordings or on-chain state.

# src/repair.rs
Retroactive trade parser for records stored before pool orientation was resolved.

# src/api.rs
Read-only HTTP query API.
```

## Try it

```bash
git clone https://github.com/shaurya35/solana-realtime-indexer
cd solana-realtime-indexer
docker compose up --build
```

Then query the local API:

```bash
curl -s localhost:3000/health | jq
curl -s 'localhost:3000/trades/recent?limit=5' | jq
```

No external account or API key is required. Postgres boots cleanly, loads seed data from `fixtures/golden-500.jsonl`, and serves it via the API.

This also launches Prometheus on `localhost:9090` and Grafana on `localhost:3001` with a preconfigured dashboard (`docker/grafana/dashboards/indexer.json`). Dashboards remain idle until an active runner (such as `live` below) begins emitting metrics.

The fixture contains 500 real mainnet transactions captured as raw wire bytes, enabling local development and testing without an RPC endpoint. Note that PumpSwap pool orientation lookups will query Solana's public RPC, which requires an internet connection but no credentials.

To run tests without Docker or network dependencies:

```bash
cp .env.example .env
cargo test
```

Two integration tests require a live database and will print `SKIPPED` if Postgres is unreachable. To run the full suite locally:

```bash
docker compose up -d postgres
TEST_DATABASE_URL=postgres://indexer:indexer@localhost:5433/indexer \
  cargo test -- --test-threads=1
```

Tests run single-threaded here because database tests share a schema and clean it between runs. CI provisions its own isolated database container, running all eleven tests on every push.

## Run it against live mainnet

Live streaming requires valid endpoints. Configure your Yellowstone gRPC endpoint and Postgres connection in `.env` before running.

If you are using the free public endpoint at `solana-yellowstone-grpc.publicnode.com:443`, generate a free access token at [allnodes.com/publicnode](https://www.allnodes.com/publicnode) and set it as `YELLOWSTONE_X_TOKEN` in `.env`.

```bash
# Decode live mainnet trades
cargo run -- live

# Record raw stream traffic to disk
cargo run -- capture --minutes 5

# Replay a recorded stream file
cargo run -- replay --path fixtures/golden-500.jsonl

# Verify a recording against database state
cargo run -- verify --path fixtures/golden-500.jsonl

# Verify an on-chain slot range against the database
cargo run -- verify-range --from 437993119 --to 437993182

# Fetch and ingest missing slots identified by gap detection
cargo run -- recover

# Re-parse stored payloads with updated pool orientations
cargo run -- repair

# Backfill an arbitrary slot range via RPC
cargo run -- backfill --from 437993119 --to 437993182

# Run the read-only query API
cargo run -- api

# Replay fixture data at a fixed rate to measure throughput
cargo run --release -- bench --rate 4800 --repeat 3 --output results/bench.json
```

The `live` command also serves Prometheus metrics at `localhost:9100/metrics`.

## Query it

```
GET /health                       Slot checkpoint, total row count, unresolved gaps
GET /trades/recent?limit=20       Latest ingested trades
GET /trades/token/{mint}?limit=50 Historical trades for a given token mint
GET /volume/token/{mint}          Trade count and cumulative SOL volume for a token
```

Amounts are returned as strings. Raw values are stored as 64-bit unsigned integers; serializing them as native JSON numbers risks silent precision loss in JavaScript clients beyond `Number.MAX_SAFE_INTEGER` (2^53 - 1).

## Coverage

Events handled by the indexer, benchmarked against a 500-transaction mainnet sample (`fixtures/golden-500.jsonl`):

| Program | Event | Handled | Seen in sample |
|---|---|---|---|
| pump.fun | `CpiEvent::TradeEvent` | yes | 58 |
| PumpSwap | `CpiEvent::BuyEvent` | yes | 445 combined |
| PumpSwap | `CpiEvent::SellEvent` | yes | (buy and sell) |
| PumpSwap | `CpiEvent::CreatePoolEvent` | yes | 0 |
| PumpSwap | Deposit, Withdraw, other events | decoded, ignored | not counted |
| pump.fun | `Buy`, `Sell`, `Create` instructions | ignored on purpose | not counted |

503 events total across 500 transactions and 92 distinct liquidity pools.

Outer instructions are intentionally ignored: they only represent requested execution parameters, whereas inner CPI log events confirm what actually settled on-chain. See [DESIGN.md](DESIGN.md) for details.

Ten trades across ten distinct pools were cross-checked against raw transaction balance changes, matching lamport for lamport. Pool orientation was verified across all 445 PumpSwap events in the dataset.

## Twelve hours unattended

![The dashboard over the run](docs/images/grafana-stats.png)

Production telemetry from a 12-hour soak test on mainnet (August 15–16, 2026):

```
12,439,266 events processed (12,415,571 trades)
Zero panics, zero dropped updates, zero dead-letter queue entries
Resident memory stable between 29 MB and 36 MB
```

During this run, an upstream gRPC disconnection dropped 938 slots. Both gap detectors flagged the exact same range. Running `recover` backfilled the missing blocks, and `verify-range` confirmed complete parity with the chain: 105,481 events expected, 105,481 found, and zero duplicates.

Batch write latency (time from channel receipt to database commit) averaged 170 ms, with a p99 between 0.6 s and 1.4 s. Detailed methodology and latency traces are documented in [DESIGN.md](DESIGN.md), with raw run outputs in `docs/evidence/`.

## Throughput

Synthetic replay of committed fixtures through the production pipeline under sustained load:

| Target tx/s | Result |
|---:|:---|
| 4,800 | Stable; 0 dropped events, zero schedule lag |
| 9,600 | Backpressure engaged; 3.9 s schedule lag, 0 dropped events |

Under saturated input rates, the pipeline does not drop data. It applies bounded channel backpressure upstream to pace ingestion. Across 18 benchmark trials, the engine decoded all 1,696,116 expected events without uncommitted writes or dead-letter spills.

*Note: Benchmarks were performed on a single local development machine running local Postgres. Actual production throughput will vary based on hardware, network latency, and disk I/O. See [BENCHMARKS.md](BENCHMARKS.md) for test harness details, parameters, and full metrics tables.*

## Status

Tagged at `v0.1.0`. All eleven tests pass in CI on every push.

Working:

- [x] Real-time pump.fun and PumpSwap trade decoding over Yellowstone gRPC
- [x] Deep CPI trade extraction (supporting aggregator routers and bot bundles)
- [x] Dynamic pool orientation via token vault lookups (no hardcoded base/quote assumptions)
- [x] Atomic batch writes pairing trade rows with stream checkpoint markers
- [x] Graceful shutdown with in-flight batch flushing
- [x] Unified gap detection and recovery via standard decoding pipelines
- [x] Independent reconciliation tooling (`verify`) for missing or duplicate records
- [x] Retroactive trade resolution (`repair`) for late pool discoveries
- [x] Dead-letter queue capturing unparsable or failed batches with error payloads
- [x] Read-only HTTP query API
- [x] Prometheus metrics exporter and packaged Grafana dashboard
- [x] Zero-configuration local development setup via Docker
- [x] Automated CI validation: formatting, linting, and tests

Planned:

- [ ] Support for token-to-token liquidity pools (abstracting quote assets beyond SOL)
- [ ] Checkpoint-aware boot sequences to resume streaming without manual offsets
- [x] Integrated benchmarking suite with published throughput baselines ([results](BENCHMARKS.md))
- [ ] Chain reorg and fork reconciliation handling

Known architectural trade-offs are detailed in [DESIGN.md](DESIGN.md).

## Notes

- **Precision:** All asset amounts are stored as raw integers (lamports for SOL, atomic base units for SPL tokens) to eliminate floating-point rounding errors.
- **Fixtures:** Raw gRPC wire recordings consume roughly ~340 MB per 2-minute capture and are gitignored. The repository includes `fixtures/golden-500.jsonl` as a deterministic test dataset.
- **Architecture:** Read [DESIGN.md](DESIGN.md) for rationale behind thread boundaries, memory guarantees, and current design constraints.

## Writing & Media

- **Article:** [Real-Time Indexing on Solana in Rust: streaming, decoding, and proving completeness](https://medium.com/@shauryajha35/indexing-on-solana-in-rust-streaming-decoding-and-proving-completeness-982812209b2b) — Architectural walkthrough, performance profiling, and edge cases encountered.
- **Walkthrough:** [Demo Video](https://youtu.be/URRNNI0bn_Q) (3 minutes) — Live startup, stream monitoring, and gap repair.

## Contributing

Contributions, bug reports, and PRs are welcome. The [Planned](#status) section is a great place to start.

CI enforces formatting, linting, and tests on all branches and pull requests. Please run the standard verification suite before submitting:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test -- --test-threads=1
```

*Ensure `TEST_DATABASE_URL` is set before running tests (see [Try it](#try-it)); otherwise, database integration tests will skip.*

If you are modifying decoding schemas or pool resolution routines, review [DESIGN.md](DESIGN.md) first to ensure proposed changes do not break downstream gap recovery guarantees.

## License

[MIT](LICENSE)
