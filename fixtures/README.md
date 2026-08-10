# Fixtures

Real mainnet data, committed so the tests and the demo need no endpoint and no
API key.

## `golden-500.jsonl`

500 transactions captured from Yellowstone on 21 July 2026, one per line, as
raw wire bytes in base64 with the slot and signature in plain text alongside.

Raw bytes on purpose. A fixture holding already-decoded events would let
`replay` hand those events straight back without the decoder ever running,
which would test the file reader and nothing else.

Used by `replay`, `verify`, the deterministic test, and the Docker demo. It
holds 58 pump.fun fills and 445 PumpSwap fills across 92 pools.

## `pumpfun-buy-via-flashx-01.json`

The raw JSON-RPC response for signature `3pXo1Y73…`, a pump.fun buy routed
through FLASHX. Kept as the source the parsed fixture below was derived from.

Read [flashx-walkthrough.md](flashx-walkthrough.md) for what makes this
transaction the interesting one.

## `pumpfun-buy-via-flashx-01-parsed.json`

The same transaction, parsed. Used by `decodes_successful_pumpfun_trade`, which
asserts the decoder pulls out `3940708338` tokens for `97777` lamports from an
instruction buried two levels deep behind a router.

## `pumpfun-failed-01.json`

A failed pump.fun transaction. Used by `rejects_failed_transaction`, which
checks nothing is stored for a transaction that did not execute.

## `capture-*.jsonl`

Not committed. `capture` writes these and they run to hundreds of megabytes.
Gitignored.
