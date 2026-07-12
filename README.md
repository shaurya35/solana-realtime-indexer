# solana-realtime-indexer

A Rust indexer for Pump.fun and PumpSwap trades on Solana, built with
[Carbon](https://github.com/sevenlabs-hq/carbon) and Yellowstone gRPC.

The goal is to decode real trades correctly — including ones that go through
routers via CPI — not just look at outer instructions.

## What it does

Right now it works from transaction fixtures under `fixtures/`. It loads a
parsed Pump.fun buy (routed through FLASHX), walks the inner instructions,
and finds the Pump.fun program calls so the instruction data can be decoded.

Next steps are live ingestion over Yellowstone gRPC and Carbon's Pump.fun /
PumpSwap decoders.

## Why CPI matters

A direct Pump.fun buy shows up as an outer instruction. A routed buy (for
example through FLASHX) calls Pump.fun as an inner instruction. If you only
read the top level, you miss the trade.

The Pump.fun buy instruction is the intent. The self-CPI event is the executed
result. See `notes/pumpfun-transaction-walkthrough.md` for a full walk of one
fixture.

## Project layout

```
solana-realtime-indexer/
  src/main.rs       fixture parser / entrypoint
  fixtures/         real Pump.fun transaction JSON
  notes/            transaction walkthroughs
  Cargo.toml        dependencies (Carbon, Yellowstone, etc.)
```

## Setup

Copy the env example and point it at a mainnet RPC:

```bash
cp .env.example .env
```

## Run

```bash
cargo run
```

This reads `fixtures/pumpfun-buy-via-flashx-01-parsed.json` and prints the
Pump.fun inner instructions it finds.
