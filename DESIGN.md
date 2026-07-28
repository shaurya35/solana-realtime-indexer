# Design

Why this indexer is built the way it is.

Each section states a decision, the reasoning behind it, and where relevant the evidence
from real mainnet data that supports it.

## Contents

- [Event identity](#event-identity)
- [What gets indexed](#what-gets-indexed)
- [Pool orientation](#pool-orientation)
- [Backpressure](#backpressure)
- [Storage](#storage)
- [Divergence from Carbon's generated tables](#divergence-from-carbons-generated-tables)
- [Testing strategy](#testing-strategy)
- [Known limitations](#known-limitations)

## Event identity

Every decoded event is keyed on three fields:

```
signature + absolute_path + event_ordinal
```

`absolute_path` is the event's position in the transaction's CPI tree, expressed as a list
of indices. The length of the list is the depth. A path of `[3, 0, 0]` means outer
instruction 3, then its first inner instruction, then that instruction's first inner
instruction.

`event_ordinal` distinguishes multiple events emitted by a single instruction. For the
decoders used here one instruction produces one event, so this field is always 0. It is
part of the key because a future decoder may not have that property, and adding a column
to a primary key after the fact is expensive.

### Why not `(signature, instruction_index)`

This is the key most indexers use, and it loses data on real transactions.

Carbon exposes an `index` field on instruction metadata. That field is the first element of
`absolute_path`, which is the outer instruction number. Every instruction nested anywhere
inside a given outer instruction therefore shares the same `index`.

Here is a transaction from the committed test fixture:

```
signature 3ud3k16PF71eMbWpUirgaMHaxnxLiZQ87VUKcaYK7VGom8PaqKRXsh2TJ6zSAuJyPi4C6BPb2QFWVLdfDe87M3jV

  absolute_path [3, 5]   sold      1,365,845,649
  absolute_path [6, 5]   received  1,462,977,130
```

One transaction, two separate fills. This is a bot buying in one pool and selling in
another, atomically, which is common on Solana. With an instruction index in the key these
two rows collide and one is silently discarded. Every arbitrage transaction would lose half
its volume, and nothing in the system would report an error.

`absolute_path` distinguishes them because it describes the full route through the tree
rather than the entry point.

## What gets indexed

The indexer matches CPI events, not instruction arguments.

A `Buy` instruction records what the user requested. The `TradeEvent` emitted by the
program records what actually executed. Prices move between those two moments, so the
amounts differ. An indexer that reports the instruction reports intent, and intent is not
what happened.

Concretely, the pipeline matches:

| Program | Matched |
|---|---|
| pump.fun | `CpiEvent::TradeEvent` |
| PumpSwap | `CpiEvent::BuyEvent`, `CpiEvent::SellEvent`, `CpiEvent::CreatePoolEvent` |

Failed transactions are excluded at two levels. The subscription filter sets
`failed: false`, so the server does not send them on the live path. A second check on
`transaction_metadata.meta.status` covers the replay path, where saved files may contain
failed transactions.

The status check belongs at the transaction level rather than the event level. Solana
transactions are atomic, so an event can be emitted and then rolled back when a later
instruction fails. Only the transaction knows the final outcome.

## Pool orientation

A PumpSwap pool holds two tokens, recorded as `base_mint` and `quote_mint`. The order is
not guaranteed. Some pools are created with the traded token as base and SOL as quote.
Others are the reverse.

Trade events report `base_amount` and `quote_amount` without saying which is which. Reading
them positionally produces wrong values for every inverted pool, and the values look
plausible because both are just integers.

The indexer resolves this by comparing both mints against the wrapped SOL address
(`So11111111111111111111111111111111111111112`) and assigning amounts accordingly.

The trade direction inverts as well. A `BuyEvent` means the user acquired the base token.
On a pool where base is SOL, acquiring base means the user was selling. The reported side
therefore comes from the resolved orientation, not from which event variant fired.

Pools with SOL on neither side are reported with raw amounts and no orientation, because
"SOL amount" has no meaning for a token to token pool.

## Backpressure

### The problem

Carbon invokes the processor one event at a time and waits for each to complete. The pool
metadata lookup originally made an RPC call inside that path, taking roughly 300ms. While
blocked, incoming updates accumulated in Carbon's internal channel, which holds 1,000 items
by default. Once full, the Yellowstone datasource calls `try_send`, which fails immediately
rather than waiting, logs at a level that is off by default, and discards the update.

Measured over 25 second live runs:

| Configuration | Trades observed |
|---|---|
| No RPC lookup | 442 |
| RPC in the buy path only | 276 |
| RPC in both paths | 147 |

Approximately two thirds of the stream was being discarded with no exception, no counter,
and no visible log line.

### Why a larger queue does not solve this

A buffer absorbs a burst, meaning a short spike that then subsides and gives the consumer
time to recover. It does nothing for a consumer that is permanently slower than the
producer. At pump.fun volume the stream does not subside, so any fixed buffer fills. Raising
the capacity to 10,000 delays the first discard by a few seconds and changes nothing
afterwards.

An unbounded channel does not discard at all. It grows until the process is terminated for
memory use, which is a worse outcome than discarding because it takes the entire indexer
offline rather than losing a subset of updates.

The cause was never the queue size. It was a network call inside a loop that handles one
event at a time.

### The change

Pool metadata moved to a shared in-memory cache. On a cache miss the processor sends the
pool address to a background task over a bounded channel and returns immediately. The event
is emitted with the mint marked unresolved. The background task performs the RPC call and
populates the cache, so subsequent trades on that pool resolve from memory.

`CreatePoolEvent` populates the cache directly when a pool is created on the stream,
including token decimals, at no network cost.

A local set of already requested pools prevents duplicate lookups. A busy pool may produce
many cache misses in the window before its first lookup returns, and without deduplication
each would queue a separate request.

The processor now performs no awaits at all.

### Overflow policy

Two queues, two deliberately different policies.

| Queue | Capacity | Policy | Reasoning |
|---|---|---|---|
| Pipeline events | 10,000 | Does not overflow | The processor no longer blocks, so it drains faster than the stream fills. The buffer absorbs bursts rather than compensating for a slow consumer. |
| Pool lookups | 1,000 | Discard and count | A discarded lookup request costs nothing. The pool trades again within seconds and the miss re-fires. |

Discard and count is neither correct nor incorrect on its own. It is appropriate for
anything that repeats and unacceptable for anything that does not, which is why the same
policy is right for lookup requests and wrong for events.

Both capacities are set explicitly in `config.rs` rather than left as library defaults, so
the values are a decision rather than an accident.

### Results

A 190 second live run. Raw output is in `docs/gates/week1/drop-fix-stats.txt`.

| Metric | Before | After |
|---|---|---|
| Events decoded per second | 5.9 | 118 sustained, 201 peak |
| Updates discarded | Unknown, not instrumented | 0 across every 10 second interval |
| Pool cache hit rate | Not applicable | 92.1% at 190 seconds |
| RPC calls | One per uncached trade | 457 total, serving 1,486 cache misses |

The throughput comparison is indicative rather than controlled. The two measurements were
taken on different days, so market volume differed. The discard count and cache behaviour
are not subject to that caveat.

The cost of this design is that 1,495 of 19,428 PumpSwap fills, or 7.7%, were emitted
without a resolved mint. This is an acceptable trade because a missing mint is recoverable
and a missing event is not. The pool address is recorded on every event, so an unresolved
mint can be filled in later from the `pools` table or a batch RPC pass. An update that the
datasource discarded was never observed and cannot be reconstructed from anything the
system holds.

## Storage

Four tables. Migrations are in `migrations/`.

### Amounts are BIGINT, never floating point

All amounts are stored as raw integers: lamports for SOL, raw units for tokens.

A `u64` of lamports can exceed the range that a 64 bit float represents exactly, at which
point arithmetic begins rounding silently. Financial totals computed from rounded inputs
are wrong in ways that are difficult to detect because no operation fails.

Token decimals are stored separately in the `pools` table and applied at display time.
Applying them at write time would mean storing a value that cannot be recovered exactly.

### `events` and `trades` are separate tables

`events` is an append only record of what was decoded, including the full payload as JSONB.
`trades` is the interpreted subset, flattened into queryable columns.

The separation exists so that improved decode logic does not require re-reading mainnet.
If interpretation changes, `trades` can be rebuilt from `events`. If both were one table,
a decode fix would mean refetching historical data.

`events` carries a `parser_version` column. Any change to decode logic increments it, which
makes it possible to distinguish rows produced by old logic from rows produced by current
logic, and to backfill selectively.

### The checkpoint

`ingestion_checkpoints` holds exactly one row, enforced by a check constraint. It records
the highest slot for which processing is known to be complete.

The checkpoint advances only when a batch commits, in the same database transaction as the
events in that batch. This means it can never report progress that was not durably written.

`SELECT MAX(slot) FROM events` is not equivalent and cannot be substituted. Events are
written in batches. If the process fails partway through a batch, some rows from that batch
are present and others are not, and the missing rows are not necessarily the highest ones.
`MAX(slot)` returns the highest slot that arrived, which may be well above the highest slot
that completed. Resuming from it skips the gaps below it permanently and silently.

Resuming from the checkpoint instead means reprocessing part of a batch. The primary key
combined with `ON CONFLICT DO NOTHING` makes those repeated inserts no-ops, so overlap is
free.

## Divergence from Carbon's generated tables

Carbon can generate Postgres tables for a decoder. Those tables are keyed on:

```
__signature, __instruction_index, __stack_height, __slot
```

with one table per instruction type.

That shape is a faithful log of instruction invocations, and it is useful for that purpose.
It is not suitable as the storage layer here for two reasons.

The key includes `__instruction_index`, which as described above is the outer instruction
number and is shared across an entire CPI subtree. Adding `__stack_height` reduces
collisions but does not eliminate them, because two instructions at the same depth under the
same outer instruction share both values.

More fundamentally, one table per instruction type answers a different question. It records
which instructions were called. This system needs to record which trades occurred, across
two protocols, in a shape that supports queries by token and by time.

These are different products rather than better and worse versions of the same product.
Carbon's generated tables are a reasonable raw log. A normalized event store is a different
artifact with different requirements.

## Testing strategy

The test suite runs with no network access and no gRPC credentials.

`capture` records raw Yellowstone wire bytes to a JSON Lines file, one transaction per line,
base64 encoded, with slot and signature stored in plain text alongside so a transaction can
be located without decoding the file.

The raw bytes are stored deliberately. A fixture containing already decoded events would
mean that replaying it returns those same events without the decoder ever running. Such a
test verifies the file reader and nothing else.

`replay` implements Carbon's `Datasource` trait and feeds a captured file into the pipeline
through the identical decode path used by live traffic. Because both data sources satisfy
the same trait, the pipeline cannot distinguish them, so results proven under replay hold
for live.

The primary test replays a committed fixture twice and asserts that both runs produce an
identical set of event identifiers. `fixtures/golden-500.jsonl` contains 500 real mainnet
transactions, including 58 pump.fun fills and 214 PumpSwap fills.

The same capture and replay pair also provides fixture mode for reviewers without a paid
endpoint, input for crash recovery testing, and input for benchmarking.

## Known limitations

**Carbon's silent discard is unchanged.** The Yellowstone datasource still calls `try_send`
and still discards without a counter when its downstream is slow. This system removed the
reason its downstream was slow. It did not change the underlying behaviour, and any future
blocking call in the processor would reproduce the same invisible loss. Making the loss
visible upstream, through a discarded update counter on the datasource, is a proposed
contribution to Carbon rather than something addressed here.

**Token decimals are incomplete.** Pools discovered through `CreatePoolEvent` carry correct
decimals. Pools discovered through RPC do not, because the pool account records mints but
not their decimal precision, which requires a second lookup against each mint account. This
affects display formatting only, since amounts are stored raw.

**Reorg handling is not implemented.** The indexer subscribes at the default commitment
level and does not track fork choice or handle slot rollback. Describing this system as
reorg safe would require a passing fork replacement test, which does not exist.

**Coverage is partial.** Instruction and event variants outside those listed above are
decoded and ignored rather than stored. Unknown variants are logged and never cause a panic.
