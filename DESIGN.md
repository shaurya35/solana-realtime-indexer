# DESIGN

Design decisions for solana-realtime-indexer, and the reasoning behind each.

## Event identity

### Why `absolute_path` + `event_ordinal`?

absolute_path is the full route through the tree, so no two instructions in a transaction share one.
event_ordinal covers the case where a single instruction emits multiple events.

### Why does `(signature, instruction_index)` collide?

<!-- You have real evidence: signature 3ud3k16… produced fills at
     absolute_path [3,5] and [6,5] in one transaction. Use it.
     What exactly would a naive indexer lose here? -->

## Storage

### Why are amounts BIGINT and never float?

<!-- What goes wrong with floats and money. Why decimals are a
     read-side concern. What u64 lamports means for the column type. -->

### Why are `events` and `trades` separate tables?

<!-- What each one represents. What becomes possible when decode
     logic changes and you have both. -->

### What does the checkpoint row mean, and when does it move?

<!-- Not built yet — answer from first principles.
     Why is SELECT MAX(slot) FROM events not equivalent? -->

## Divergence from Carbon's generated tables

<!-- Carbon generates one table per instruction type, keyed on
     (__signature, __instruction_index, __stack_height).
     One paragraph: what is that key good for, what is it not
     good for, and why is yours a different product rather
     than a better version of the same one? -->

## Backpressure

### The problem

Carbon calls `process()` one event at a time and waits for each to finish. `ensure_pool()`
made an RPC call inside it, roughly 300ms. While blocked, the stream kept arriving and
filled Carbon's internal channel (default capacity 1,000). When full, the Yellowstone
datasource calls `try_send`, which fails immediately, logs at a level nobody watches, and
drops the update.

Measured 18 Jul, 25-second live runs:

| Version | Trades seen |
|---|---|
| No RPC lookup | 442 |
| RPC in the buy arm only | 276 |
| RPC in both arms | 147 |

Roughly two thirds of the firehose, gone with no exception, no counter and no trace.

### Why a bigger queue was not the answer

A buffer absorbs a burst: a short spike that then subsides, giving the consumer time to
catch up. It does nothing for a consumer that is permanently slower than the producer.
At pump.fun volume the stream never subsides, so any fixed buffer fills eventually —
10,000 delays the first drop by a few seconds and changes nothing after that. An
unbounded channel does not drop at all; it grows until the process is killed for memory,
which is a worse failure than dropping because it takes the whole indexer down.

The cause was never the queue size. It was a 300ms network call sitting inside a loop
that handles one event at a time.

### What we changed

Pool metadata moved to a shared in-memory cache (`Arc<RwLock<HashMap<Pubkey, PoolInfo>>>`).
On a miss the processor sends the pool address to a background task over a bounded channel
and returns immediately; the event is emitted with the mint marked UNKNOWN. The background
task performs the RPC and fills the cache. `CreatePoolEvent` populates the cache for free,
with decimals, when a pool is born on the stream.

The pipeline channel is set explicitly to 10,000 via `channel_buffer_size`.

`process()` now contains no `.await` at all.

### The overflow policy, stated

Two queues, two different policies, deliberately:

| Queue | Policy | Reason |
|---|---|---|
| Pipeline (events) | never overflows | `process()` no longer awaits anything, so it drains far faster than the stream fills it. The queue exists to absorb bursts, not to hide a slow consumer. |
| Pool lookups | drop and count (`try_send` → `LOOKUPS_DROPPED`) | A dropped lookup request costs nothing: that pool trades again within seconds and the miss re-fires. A dropped event is gone from the chain's history as far as this indexer is concerned. |

Drop-and-count is neither good nor bad in itself. It is correct for anything that repeats
and wrong for anything that does not — which is why the same policy is right for lookup
requests and unacceptable for events.

### Results

190-second live run, 28 Jul. Raw output in `docs/gates/week1/drop-fix-stats.txt`.

| | Before (18 Jul) | After (28 Jul) |
|---|---|---|
| Rate | 5.9/s | ~118/s, peak 201/s |
| Updates dropped | invisible | 0, every 10s tick |
| Pool cache hit rate | n/a | 92.1% at 190s |
| RPC calls | one per uncached trade | 457 total, serving 1,486 misses |

The rate comparison is indicative rather than controlled: the before number was taken on a
different day, so market volume differed. The drop count and cache behaviour are not
subject to that caveat.

**Cost of the trade:** 1,495 of 19,428 PumpSwap fills (7.7%) printed without a resolved
mint. That is the right trade because a missing mint is recoverable and a missing event is
not: the pool address is stored on every event, so any unresolved mint can be filled in
later from the `pools` table or a batch RPC pass, whereas an update the datasource dropped
was never seen and cannot be reconstructed from anything we hold.

### What this does NOT fix

Carbon's Yellowstone datasource still uses `try_send` and still drops silently when its
downstream is slow. We removed the reason our downstream was slow; we did not change the
behaviour. Any future blocking call on the hot path will reproduce the same invisible loss.

Making the loss visible upstream — a dropped-update counter on the datasource — is a
proposed contribution to Carbon rather than something fixed here.