![ci](https://github.com/shaurya35/solana-realtime-indexer/actions/workflows/ci.yml/badge.svg)

# solana-realtime-indexer

Indexes pump.fun and PumpSwap trades from Solana mainnet, in real time.

Written in Rust. Streams over Yellowstone gRPC, decodes with
[Carbon](https://github.com/sevenlabs-hq/carbon), stores in Postgres.

Work in progress, built in public.

## Try it

```bash
git clone https://github.com/shaurya35/solana-realtime-indexer
cd solana-realtime-indexer
cp .env.example .env
cargo test
```

Tests run off a committed fixture. No endpoint, no API key, no network.

That fixture is `fixtures/golden-500.jsonl`, 500 real mainnet transactions saved as raw wire
bytes. It is the largest file in the repo and it is the reason the tests need nothing.

## Run it

Needs a Yellowstone gRPC endpoint and a Postgres URL in `.env`.

The free endpoint at `solana-yellowstone-grpc.publicnode.com:443` needs a personal token,
which you generate for free at [allnodes.com/publicnode](https://www.allnodes.com/publicnode).
It goes in `.env` as `YELLOWSTONE_X_TOKEN`. Without it the connection is refused.

```bash
cargo run -- live                                      # decode trades as they happen
cargo run -- capture --minutes 5                       # record traffic to a file
cargo run -- replay --path fixtures/golden-500.jsonl   # replay a recording
cargo run -- verify --path fixtures/golden-500.jsonl   # check nothing is missing or extra
```

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
3ud3k16PF71eMbWpUirgaMHaxnxLiZQ87VUKcaYK7VGom8PaqKRXsh2TJ6zSAuJyPi4C6BPb2QFWVLdfDe87M3jV

  path [3, 5]   sold     1,365,845,649
  path [6, 5]   received 1,462,977,130
```

A bot buying in one pool and selling in another, atomically. Most indexers key rows on
`(signature, instruction_index)`. Both of these rows have the same instruction index, so
one of them silently disappears.

This one keys on `signature + absolute_path + event_ordinal`, where `absolute_path` is the
full route through the transaction tree. Details in [DESIGN.md](DESIGN.md).

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

**Nothing is dropped silently.** An RPC call on the hot path was costing 66% of the stream,
invisibly, because the datasource discards without a counter when it can't keep up. Pool
lookups now happen in the background and every discard path is counted.

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

Next:

- Backfill: go and fetch what the gaps say is missing
- Query API and metrics
- Docker compose, one command boot

## Notes

Amounts are stored as raw integers. Lamports for SOL, raw units for tokens. Never floats.

Recordings are large, around 340 MB for two minutes, so they are gitignored.
`fixtures/golden-500.jsonl` is a small committed slice used by the tests.

[DESIGN.md](DESIGN.md) covers the reasoning behind each decision, the measurements, and
what this does not do yet.

## License

MIT
