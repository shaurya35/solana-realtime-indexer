![ci](https://github.com/shaurya35/solana-realtime-indexer/actions/workflows/ci.yml/badge.svg)

# solana-realtime-indexer

Indexes pump.fun and PumpSwap trades from Solana mainnet, in real time.

Written in Rust. Streams over Yellowstone gRPC, decodes with
[Carbon](https://github.com/sevenlabs-hq/carbon), stores in Postgres.

Work in progress, built in public.

## How it works

![The path a trade takes, including repair](docs/images/pipeline-overview.png)

Trades arrive over gRPC, get decoded, batched and written. When the stream breaks, the
missing slots are recorded, refetched from the chain, and checked against what is already
stored.

![One decode path, three ways in](docs/images/three-way-architecture.png)

Live traffic, a saved file, and a range of slots all enter through the same function.
`run_pipeline` takes a datasource and does not care which one it got.

So the decoder that handles mainnet is the same one the tests run, and the same one that
fills a gap. There is no second path to drift.

Everything after that box is the same four steps, whichever way the data came in.

![What happens to a single trade](docs/images/single-transaction-architecture.png)

**Decode.** Raw wire bytes to a typed struct, one decoder per program.

**Interpret.** pump.fun is a bonding curve, so the mint is in the instruction. PumpSwap is
a pool with a base slot and a quote slot, and nothing in the instruction says which one
holds the token. That lookup happens in the background, off the hot path.

**Hand off.** The processor drops the row into a channel and returns. It never waits for
the database.

**Write.** A separate task batches rows and commits them with the progress marker in one
transaction, so the marker can never claim more than was written.

Gap detection, recovery, `verify`, the query API and metrics are left off. They answer
different questions and hang off the side of this.

## Try it

```bash
git clone https://github.com/shaurya35/solana-realtime-indexer
cd solana-realtime-indexer
docker compose up --build
```

Then:

```bash
curl -s localhost:3000/health | jq
curl -s 'localhost:3000/trades/recent?limit=5' | jq
```

No account and no API key. Postgres comes up empty, gets loaded from
`fixtures/golden-500.jsonl`, and the API serves it.

The same command brings up Prometheus on `localhost:9090` and Grafana on
`localhost:3001`, with the dashboard already loaded from
`docker/grafana/dashboards/indexer.json`. It stays empty until something is running to
scrape, which means `live` below rather than the fixture.

That fixture is 500 real mainnet transactions saved as raw wire bytes. It is what lets this
and the test suite run without an endpoint.

The demo does use Solana's public RPC to work out which side of a PumpSwap pool is SOL.
That needs internet, but no account.

Tests, with no Docker and no network:

```bash
cp .env.example .env
cargo test
```

Three of the ten need a database and print `SKIPPED` without one. To run all of them:

```bash
docker compose up -d postgres
TEST_DATABASE_URL=postgres://indexer:indexer@localhost:5433/indexer \
  cargo test -- --test-threads=1
```

Single threaded because those three share a schema and empty it between runs. CI brings up
its own Postgres, so all ten run on every push.

## Run it against live mainnet

This is the part that needs an endpoint. Set a Yellowstone gRPC URL and a Postgres URL
in `.env`.

The free endpoint at `solana-yellowstone-grpc.publicnode.com:443` needs a personal token,
which you generate for free at [allnodes.com/publicnode](https://www.allnodes.com/publicnode).
It goes in `.env` as `YELLOWSTONE_X_TOKEN`. Without it the connection is refused.

```bash
# decode trades as they happen
cargo run -- live

# record traffic to a file
cargo run -- capture --minutes 5

# replay a recording
cargo run -- replay --path fixtures/golden-500.jsonl

# check a recording against the database
cargo run -- verify --path fixtures/golden-500.jsonl

# check a slot range against the chain
cargo run -- verify-range --from 437993119 --to 437993182

# refetch whatever the gaps say is missing
cargo run -- recover

# rebuild trades from stored payloads
cargo run -- repair

# refetch a slot range by hand
cargo run -- backfill --from 437993119 --to 437993182

# serve it on localhost:3000
cargo run -- api
```

`live` also serves Prometheus metrics on `localhost:9100/metrics`.

## Query it

```
GET /health                       last completed slot, row counts, unresolved gaps
GET /trades/recent?limit=20       newest trades
GET /trades/token/{mint}?limit=50 trades for one token
GET /volume/token/{mint}          trade count and total SOL for one token
```

Amounts are returned as strings. They are stored as raw integers and can exceed what a JSON
number holds exactly, so sending them as numbers would let a JavaScript client round them
without saying so.

## Coverage

What the indexer handles, and what showed up in a 500-transaction mainnet sample
(`fixtures/golden-500.jsonl`).

| Program | Event | Handled | Seen in sample |
|---|---|---|---|
| pump.fun | `CpiEvent::TradeEvent` | yes | 58 |
| PumpSwap | `CpiEvent::BuyEvent` | yes | 445 combined |
| PumpSwap | `CpiEvent::SellEvent` | yes | (buy and sell) |
| PumpSwap | `CpiEvent::CreatePoolEvent` | yes | 0 |
| PumpSwap | Deposit, Withdraw, other events | decoded, ignored | not counted |
| pump.fun | `Buy`, `Sell`, `Create` instructions | ignored by design | not counted |

503 events total, from 500 transactions, across 92 distinct pools.

The instructions are ignored on purpose. They record what a user asked for. The CPI events
record what executed. See [DESIGN.md](DESIGN.md).

Ten trades across ten different pools were checked by hand against the token balance changes
recorded in each transaction. Token amounts matched exactly. Orientation was checked across
all 445 PumpSwap events.

## Twelve hours unattended

![The dashboard over the run](docs/images/grafana-dashboard.png)

One unattended run against mainnet, 15 to 16 August 2026.

```
12,439,266 events           0 panics
12,415,571 trades           0 updates dropped
        0 dead letters      memory flat, 29 to 36 MB
```

The stream dropped once, for 938 slots. Both gap detectors recorded the same range
independently. `recover` refetched it and `verify-range` checked the result against the
chain: 105,481 events expected, 105,481 found, nothing missing and nothing extra.

Latency from a row reaching the writer to its batch committing averaged 170 ms, with a p99
between 0.6 and 1.4 seconds. Method and full numbers in [DESIGN.md](DESIGN.md), raw output
in `docs/evidence/`.

## Status

Working:

- [x] Live pump.fun and PumpSwap decoding over Yellowstone gRPC
- [x] Trades found at any depth, including behind routers and bots
- [x] Pool direction checked against wrapped SOL rather than assumed
- [x] Batched writes, with the progress marker committed in the same transaction
- [x] Graceful shutdown that flushes the last batch
- [x] Gap detection and recovery through the same decode path as live traffic
- [x] `verify`, an independent check for missing or duplicated rows
- [x] `repair`, rebuilding trades once a pool becomes known
- [x] `dead_letters`, so a failed batch is kept with its error instead of discarded
- [x] Read-only query API
- [x] Prometheus metrics and a Grafana dashboard
- [x] One command boot, no account and no API key
- [x] CI on every push: format, lint, tests

Planned:

- [ ] Token to token pools, which need a quote asset in the schema and not just SOL
- [ ] Reading the checkpoint at startup, so a restart resumes instead of starting over
- [ ] A bench command and published throughput numbers
- [ ] Reorg handling

Known limits are listed in [DESIGN.md](DESIGN.md).

## Notes

Amounts are stored as raw integers. Lamports for SOL, raw units for tokens. Never floats.

Recordings are large, around 340 MB for two minutes, so they are gitignored.
`fixtures/golden-500.jsonl` is a small committed slice used by the tests.

[DESIGN.md](DESIGN.md) covers the reasoning behind each decision, the measurements, and
what this does not do yet.

## Writing

[Real-Time Indexing on Solana in Rust: streaming, decoding, and proving completeness](https://medium.com/@shauryajha35/indexing-on-solana-in-rust-streaming-decoding-and-proving-completeness-982812209b2b)
— how this was built, and what went wrong on the way.

## License

MIT
