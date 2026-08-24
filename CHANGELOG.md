# Changelog

## v0.1.0 (24 August 2026)

First release.
[Release page](https://github.com/shaurya35/solana-realtime-indexer/releases/tag/v0.1.0)

A real time indexer for pump.fun and PumpSwap trades on Solana mainnet. It streams
transactions over Yellowstone gRPC, decodes them with Carbon, and writes them to Postgres.

### Features

- Live decoding of pump.fun and PumpSwap trades
- Finds trades at any CPI depth, including those made through routers and bots
- Resolves which side of a PumpSwap pool holds SOL instead of assuming it
- Batched writes, with the progress checkpoint committed in the same transaction
- Detects gaps in the stream and refetches the missing slots from the chain
- `verify` and `verify-range` to check stored rows against a recording or the chain
- `repair` to rebuild trades once a pool becomes known
- Failed batches are stored with their error instead of being dropped
- Read only query API, Prometheus metrics, and a Grafana dashboard

### Soak test

One unattended run against mainnet on 15 and 16 August 2026 processed 12,439,266 events
and 12,415,571 trades with no panics, no dropped updates, and memory flat at 29 to 36 MB.
The stream dropped once for 938 slots. Recovery refetched the range and verification found
all 105,481 events, with nothing missing and nothing extra.

### Known limits

- Token to token pools are not supported yet. Only pools with a SOL side are indexed.
- A restart does not resume from the stored checkpoint yet.
- Reorgs are not handled.

See DESIGN.md for the reasoning behind each decision and the full list of limits.
