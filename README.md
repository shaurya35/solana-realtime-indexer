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

Tests run off a committed fixture. No endpoint, no API key, no network. Three tests, under
a second.

## Run it

Needs a mainnet Yellowstone gRPC endpoint in `.env`.

```bash
cargo run -- live                                      # decode trades as they happen
cargo run -- capture --minutes 5                       # record traffic to a file
cargo run -- replay --path fixtures/golden-500.jsonl   # replay a recording
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

**Pool direction is not assumed.** PumpSwap pools store two tokens, and which one is SOL
is not fixed. Read them positionally and half your pools report swapped amounts that still
look plausible. This checks both against the wrapped SOL address, and flips the buy/sell
direction too when the pool is inverted.

**Nothing is dropped silently.** An RPC call on the hot path was costing 66% of the stream,
invisibly, because the datasource discards without a counter when it can't keep up. Pool
lookups now happen in the background and every discard path is counted.

```
5.9 events/sec   ->   118 events/sec
zero updates dropped over a 190 second run
92% cache hit rate, 457 RPC calls covering 1,486 misses
```

**It can replay itself.** `capture` saves raw bytes off the wire. `replay` feeds them back
through the same decode path live traffic uses. That is how the tests prove the same input
always produces the same output, with no network involved.

## Status

Done:

- Live pump.fun and PumpSwap decoding
- Pool to token resolution, direction handled
- Stable event IDs
- Capture, replay, deterministic tests
- Bounded queues with a stated overflow policy, counters every 10 seconds
- Postgres schema and migrations

Next:

- Postgres writes, batched
- Crash and restart test proving nothing is lost or duplicated
- Query API and metrics

## Notes

Amounts are stored as raw integers. Lamports for SOL, raw units for tokens. Never floats.

Recordings are large, around 340 MB for two minutes, so they are gitignored.
`fixtures/golden-500.jsonl` is a small committed slice used by the tests.

[DESIGN.md](DESIGN.md) covers the reasoning behind each decision, the measurements, and
what this does not do yet.

## License

MIT
