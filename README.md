# solana-realtime-indexer

Real-time Solana indexer for pump.fun and PumpSwap trades, written in Rust.

Streams mainnet over Yellowstone gRPC, decodes trades with
[Carbon](https://github.com/sevenlabs-hq/carbon) 1.0, and stores them in Postgres.

Work in progress. Building in public.

## Quick start

```bash
git clone https://github.com/shaurya35/solana-realtime-indexer
cd solana-realtime-indexer
cp .env.example .env
cargo test
```

The tests run off a committed fixture, so you don't need a gRPC endpoint to try it. Three
tests, a quarter of a second, no network.

To run it live you need a mainnet Yellowstone endpoint in `.env`:

```bash
# decode mainnet trades
cargo run -- live

# save real traffic to a file
cargo run -- capture --minutes 5

# replay a saved file
cargo run -- replay --path fixtures/golden-500.jsonl
```

## What's interesting here

**It decodes fills, not intents.** The `Buy` instruction is what the user asked for. The
`TradeEvent` is what actually happened. Prices move between the two, so if you index
instructions your numbers are wrong. This matches the CPI events.

**It finds trades hidden inside other transactions.** A lot of volume goes through routers,
which call pump.fun as an inner instruction. Read only the top level and you miss the trade
entirely.

**Every event gets an ID that actually works.** Most indexers key on
`(signature, instruction_index)`. Here's a real transaction from the test fixture:

```
3ud3k16PF71eMbWpUirgaMHaxnxLiZQ87VUKcaYK7VGom8PaqKRXsh2TJ6zSAuJyPi4C6BPb2QFWVLdfDe87M3jV
  path [3, 5]  sold     1,365,845,649
  path [6, 5]  received 1,462,977,130
```

One transaction, two trades. A bot buying in one pool and selling in another. With an
instruction index as the key those collide and you lose one, so the key here is
`signature + absolute_path + event_ordinal`.

**It doesn't assume which side of a pool is SOL.** PumpSwap pools store two mints, and the
order isn't guaranteed. Read the amounts positionally and every inverted pool reports
swapped values that still look plausible. This checks both mints against the wrapped SOL
address. The buy/sell direction flips too, not just the amounts.

**Nothing gets dropped silently.** An RPC lookup on the hot path was costing 66% of the
stream, invisibly, because the datasource discards without a counter when the downstream is
slow. Pool metadata now resolves in a background task and every discard path is counted.

```
5.9 events/sec  ->  118 events/sec sustained
zero updates discarded across a 190 second run
92% pool cache hit rate, 457 RPC calls serving 1,486 misses
```

**You can replay it.** `capture` saves the raw bytes off the wire, `replay` feeds them back
through the same decode path. That's how the tests prove the same input always gives the
same output, without touching the network.

## Status

Working:

- Live pump.fun and PumpSwap decoding
- Pool to token mint resolution, with orientation handled
- Stable event IDs
- Capture and replay, with deterministic tests
- Bounded queues with a stated overflow policy, and counters every 10 seconds
- Postgres schema and migrations

Next up:

- Postgres sink with batched writes
- Crash and restart test proving no events are lost or duplicated
- Query API and metrics

## Notes

Amounts are stored as raw integers (lamports, raw token units), never floats.

Captures are big, roughly 340 MB for two minutes of mainnet, so they're gitignored.
`fixtures/golden-500.jsonl` is a small committed slice for tests.

See [DESIGN.md](DESIGN.md) for the reasoning behind each decision, the measurements, and an
honest list of what this doesn't do yet.

## License

MIT
