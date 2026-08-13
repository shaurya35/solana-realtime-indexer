![ci](https://github.com/shaurya35/solana-realtime-indexer/actions/workflows/ci.yml/badge.svg)

# solana-realtime-indexer

Indexes pump.fun and PumpSwap trades from Solana mainnet, in real time.

Written in Rust. Streams over Yellowstone gRPC, decodes with
[Carbon](https://github.com/sevenlabs-hq/carbon), stores in Postgres.

Work in progress, built in public.

## How it works

![One decode path, three ways in](docs/images/three-way-architecture.png)

Live traffic, a saved file, and a range of slots all enter through the same function.
`run_pipeline` takes a datasource and does not care which one it got.

That is the decision the rest of the project rests on. The decoder that handles mainnet is
the decoder the tests run, and the decoder that fills a gap. There is no second path that
could quietly drift from the first.

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

That fixture is 500 real mainnet transactions saved as raw wire bytes. It is the largest
file in the repo, and it is what makes both this and the test suite run without an endpoint.

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
cargo run -- live                                      # decode trades as they happen
cargo run -- capture --minutes 5                       # record traffic to a file
cargo run -- replay --path fixtures/golden-500.jsonl   # replay a recording
cargo run -- verify --path fixtures/golden-500.jsonl   # check a recording against the database
cargo run -- verify-range --from 437993119 --to 437993182  # check a slot range against the chain
cargo run -- recover                                   # refetch whatever the gaps say is missing
cargo run -- repair                                    # rebuild trades from stored payloads
cargo run -- backfill --from 437993119 --to 437993182  # refetch a slot range by hand
cargo run -- api                                       # serve it on localhost:3000
```

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

## What makes indexing this hard

Three problems that trip up most naive implementations.

**1. The trade is usually buried.**

Most volume goes through routers and bots, which call pump.fun from inside their own
instruction. If you only read the top level of a transaction, you see the router and miss
the trade completely.

**2. The instruction is not the trade.**

A `Buy` instruction says what the user asked for. The `TradeEvent` the program emits says
what actually executed. The price moves between those two moments. Index the instruction
and your numbers are wrong.

**3. One transaction can hold several trades.**

Here is a real one from the test fixture:

```
5Pi1ga4SX3wN78mLQVXshdSUHdaLgexZzZsAZWngZ3GKLc4rfoZK96svu78V5WV1jswpio4wMgjx29b3pcWsm6JZ

  path [5, 1, 6]   sell  119,569,853,093 E2ueKQ…pump   for 8,473,779 lamports
  path [5, 4, 6]   buy    44,622,120,223 FSwrbj…pump   for 8,264,547 lamports
```

A bot rotating out of one token and into another, atomically. Two tokens, one transaction.

Carbon reports instruction index 5 for both, because the index is just the first element of
the path. Most indexers key rows on `(signature, instruction_index)`. Under that key these
two collide, one overwrites the other with no error, and an entire token's trade is gone.

This one keys on `signature + absolute_path + event_ordinal`, where `absolute_path` is the
full route through the transaction tree, so both survive.

Three of the 500 transactions in the fixture do this, and the test
`carbon_index_alone_would_collide` asserts it. Details in [DESIGN.md](DESIGN.md).

## Other things it gets right

**Pool direction is not assumed.** PumpSwap pools store two tokens, and which one is SOL is
not fixed. Read them positionally and you report the wrong number, with no error, because
both values are just integers.

This is not a rare case. Measured on 445 real events across 92 pools:

```
116  base is the token   (normal)
115  base is wrapped SOL (inverted)
214  undetermined (native SOL, nothing to compare against)
```

Half the pools where it could be established store SOL in the base slot. This checks both
mints against the wrapped SOL address, and flips the buy/sell direction too.

**Every discard is counted.** An RPC call on the hot path was costing 66% of the stream,
invisibly, because the datasource discards without a counter when it can't keep up. Pool
lookups now happen in the background, every discard path this code owns is counted, and
the refusals coming from upstream are counted too.

Counted, not eliminated. Carbon's queue drops transactions when the pipeline falls behind,
and a 30 minute mainnet soak recorded 524,666 refused updates against 167,617 decoded. The
number is visible rather than absent, which is the difference between a known limit and a
silent one. Closing that gap is upstream work, tracked in
[Carbon issue #580](https://github.com/sevenlabs-hq/carbon/issues/580).

```
5.9 events/sec   ->   118 events/sec
zero updates dropped over a 190 second run
92% cache hit rate, 457 RPC calls covering 1,486 misses
```

**Writes are batched.** Saving one row at a time to a hosted Postgres meant two network
round trips per trade. Measured on the 500 transaction fixture: 344 ms per event, with the
process idle for 99% of it.

```
344 ms per event   ->   8.7 ms per event
3.1 events/sec     ->   181 events/sec on live mainnet
```

Rows are buffered and written in groups of 100, or every 500 ms, whichever comes first. The
progress marker is committed inside the same transaction as its batch, so it can never
claim more than was actually written.

**It can replay itself.** `capture` saves raw bytes off the wire. `replay` feeds them back
through the same decode path live traffic uses. That is how the tests prove the same input
always produces the same output, with no network involved.

**An event that could not be interpreted can be interpreted later.** A PumpSwap trade in a
pool nobody has seen yet cannot be oriented, so no trade row is written. The raw event is
stored anyway. Once the pool is known, `repair` rebuilds those trades from the stored
payload, with no network and no re-fetch.

```
8,315 events with no trade row
7,771 rebuilt
  465 token to token pools, no SOL leg, not representable in this schema
   31 pool never resolved
   48 amount larger than i64
```

Running it twice writes nothing the second time. Rows are keyed the same way as the
originals, so a repeat is a no-op rather than a duplicate. Numbers and method in
[docs/evidence/repair.txt](docs/evidence/repair.txt).

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

## Verified against the chain

Ten trades across ten different pools were checked against the token balance changes
recorded in each transaction. Token amounts matched exactly in every case.

Orientation was checked across all 445 events, which is where the 50/50 split above comes
from.

## Status

Done:

- Live pump.fun and PumpSwap decoding
- Pool to token resolution, direction handled
- Pool cache loaded from Postgres at startup, so a restart is not blind
- Stable event IDs
- Capture, replay, deterministic tests
- Bounded queues with a stated overflow policy, counters every 10 seconds
- Postgres schema and migrations
- Batched writes, with the progress marker committed in the same transaction as its batch
- Graceful shutdown that flushes the last batch before exiting
- `verify`, an independent check for missing or duplicated rows, exits non-zero on either
- `dead_letters`, so a failed batch is kept with its error instead of discarded
- CI on every push: format, lint, tests
- Gap detection, two independent ways: the disconnect signal, and a jump in slot numbers
- Recovery: gaps are refetched from the chain through the same decode path as live traffic
- A recovered range can be checked against the chain, not just against a saved file
- Read-only query API over the indexed data
- Docker compose, one command boot with no account and no API key

- `repair`, rebuilding trades from stored payloads once the pool becomes known

Next:

- Amounts as `NUMERIC` rather than `BIGINT`, so a `u64` that exceeds `i64` is not dropped
- Token to token pools, which need a quote asset in the schema and not just SOL
- A metrics endpoint, rather than counters printed every ten seconds

## Notes

Amounts are stored as raw integers. Lamports for SOL, raw units for tokens. Never floats.

Recordings are large, around 340 MB for two minutes, so they are gitignored.
`fixtures/golden-500.jsonl` is a small committed slice used by the tests.

[DESIGN.md](DESIGN.md) covers the reasoning behind each decision, the measurements, and
what this does not do yet.

## License

MIT
