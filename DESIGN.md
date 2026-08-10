# Design

Why this indexer is built the way it is.

Each section gives a decision, the reason, and where possible the evidence from real
mainnet data.

## How an event is identified

Every event is keyed on three things:

```
signature + absolute_path + event_ordinal
```

`absolute_path` is where the event sits in the transaction, written as a list of positions.
The length of the list is how deep it is. `[3, 0, 0]` means outer instruction 3, then its
first inner instruction, then that one's first inner instruction.

`event_ordinal` separates multiple events from a single instruction. With the current
decoders one instruction produces one event, so it is always 0. It is in the key because a
future decoder might not behave that way, and widening a primary key later is expensive.

### Why not the instruction index

Most indexers use `(signature, instruction_index)`. That loses data.

Carbon's `index` field is the first element of `absolute_path`, which is the outer
instruction number. Every instruction nested inside a given outer instruction shares it.

From the test fixture:

```
signature 3ud3k16PF71eMbWpUirgaMHaxnxLiZQ87VUKcaYK7VGom8PaqKRXsh2TJ6zSAuJyPi4C6BPb2QFWVLdfDe87M3jV

  absolute_path [3, 5]   sold      1,365,845,649
  absolute_path [6, 5]   received  1,462,977,130
```

One transaction, two fills. A bot buying in one pool and selling in another. Both have the
same instruction index, so one row overwrites the other and no error is raised. Every
arbitrage transaction would lose half its volume.

The full path distinguishes them because it describes the whole route, not just the entry
point.

## What gets indexed

CPI events, not instruction arguments.

A `Buy` instruction records the request. The `TradeEvent` records what executed. Prices
move between them, so the amounts differ. Indexing the instruction means reporting intent
instead of outcome.

| Program | Matched |
|---|---|
| pump.fun | `CpiEvent::TradeEvent` |
| PumpSwap | `BuyEvent`, `SellEvent`, `CreatePoolEvent` |

Everything else is decoded and ignored rather than stored. Unknown variants are logged and
never cause a panic. See the coverage table in the README for what that looks like on a
real sample.

Failed transactions are excluded twice. The subscription sets `failed: false` so the server
never sends them live. A check on the transaction status covers replay, where saved files
can contain failures.

That check belongs on the transaction, not the event. Solana transactions are atomic, so an
event can be emitted and then rolled back when a later instruction fails. Only the
transaction knows the outcome.

## Pool direction

A PumpSwap pool holds two tokens, stored as `base_mint` and `quote_mint`. Which one is SOL
is not fixed. Some pools are created one way, some the other.

Trade events report `base_amount` and `quote_amount` without saying which is which. Reading
them by position gives wrong values on every inverted pool, and the values look reasonable
because they are just numbers.

### How common this is

Measured against a captured mainnet sample of 500 transactions producing 445 PumpSwap
events across 92 distinct pools. Orientation was determined independently by comparing each
reported amount against the actual token balance changes recorded in the transaction.

| | Count |
|---|---|
| `base` is the traded token (normal) | 116 |
| `base` is wrapped SOL (inverted) | 115 |
| Could not determine | 214 |

Of the 231 events where orientation could be established, **half store SOL in the base
slot.** This is not an edge case. Reading amounts positionally would misreport SOL volume
on roughly half of all PumpSwap trades, with no error raised anywhere, because both values
are integers of similar magnitude.

The 214 undetermined cases used native SOL rather than wrapped SOL, so there is no token
balance change to compare against. Determining those requires reading lamport balance
deltas, which is not yet implemented.

### The rule

Compare both mints against wrapped SOL (`So11111111111111111111111111111111111111112`) and
assign from there.

The direction inverts too. A `BuyEvent` means the user acquired the base token. If base is
SOL, acquiring base means the user was selling. So the reported side comes from the
resolved orientation, not from which event variant fired.

Pools with SOL on neither side are reported with raw amounts and no direction, since "SOL
amount" is meaningless for a token to token pool.

### Amounts are gross, not net

The `quote_amount` on a trade event is the total the user committed, including protocol and
creator fees. It is not what arrives in the pool.

Worked example, signature `2ivLAN1ZToVmdwTZExEmJPLBxUj6rLHnwMTeWyvFa768WYarD2ebTdUZrHDD3XPBGEGF5mZoLpnjWRJ1v4mC7e2t`:

```
event reports:  quote_amount = 995,000,000

on-chain wSOL movement:
    982,912,591   to the pool
      4,569,630   protocol fee
      4,569,630   creator fee
      2,948,149   returned to the user
    ───────────
    995,000,000
```

A block explorer shows the four transfers separately and never displays 995,000,000 as a
single line. Anyone reconciling this data against an explorer needs to know that.

### Verification status

Token amounts were checked against on-chain balance changes for 10 trades across 10
distinct pools and matched exactly in every case.

The orientation rule itself is confirmed correct by the measurement above. The code path
that applies it has not yet been exercised end to end, because the pool cache was empty
during the verification run and every event was emitted unresolved. Closing that gap
requires a sustained live run.

## Backpressure

### The problem

Carbon calls the processor one event at a time and waits for each to finish. The pool
lookup used to make an RPC call inside that path, taking around 300ms. While blocked,
incoming updates piled into Carbon's internal channel, which holds 1,000 by default. Once
full, the datasource calls `try_send`, which fails instantly rather than waiting, logs at a
level that is off by default, and throws the update away.

Measured over 25 second runs:

| Setup | Trades seen |
|---|---|
| No RPC lookup | 442 |
| RPC on buys only | 276 |
| RPC on both | 147 |

Two thirds of the stream, gone. No error, no counter, no visible log.

### Why a bigger queue does not help

A buffer absorbs a burst: a short spike that then settles, giving the consumer time to
catch up. It does nothing for a consumer that is permanently slower than the producer. At
pump.fun volume the stream never settles, so any buffer fills. Ten thousand delays the
first loss by seconds and changes nothing after.

An unbounded channel does not drop at all. It grows until the process is killed for memory,
which is worse, because losing some updates beats losing the whole indexer.

The queue size was never the cause. A network call inside a one-at-a-time loop was.

### What changed

Pool data moved to a shared in-memory cache. On a miss, the processor sends the pool
address to a background task and returns straight away. The event is emitted with the token
marked unresolved. The background task does the RPC and fills the cache, so later trades on
that pool resolve from memory.

`CreatePoolEvent` fills the cache directly when a pool is created on the stream, including
decimals, at no network cost.

A local set of already-requested pools stops duplicate lookups. A busy pool can miss many
times in the window before its first lookup returns, and each miss would otherwise queue
another request.

The processor now performs no waits at all.

### Two queues, two policies

| Queue | Size | Policy | Why |
|---|---|---|---|
| Events | 10,000 | Never overflows | The processor no longer blocks, so it drains faster than the stream fills. The buffer handles bursts, not a slow consumer. |
| Pool lookups | 1,000 | Drop and count | A dropped lookup costs nothing. The pool trades again within seconds and the miss repeats. |

Dropping is not right or wrong on its own. It is fine for anything that repeats and
unacceptable for anything that does not. That is the whole difference between these two
queues.

Both sizes are set in `config.rs` rather than left as defaults, so they are a choice rather
than an accident.

### Results

A 190 second live run. Raw output in `docs/gates/week1/drop-fix-stats.txt`.

| | Before | After |
|---|---|---|
| Events per second | 5.9 | 118 sustained, 201 peak |
| Updates dropped | Unknown, not measured | 0, every 10 second interval |
| Cache hit rate | n/a | 92% |
| RPC calls | One per uncached trade | 457, covering 1,486 misses |

The throughput comparison is indicative, not controlled. The two runs happened on different
days, so market volume differed. The drop count and cache numbers are not affected by that.

The cost: 1,495 of 19,428 PumpSwap fills, or 7.7%, were emitted without a resolved token.
That is an acceptable trade because a missing token can be recovered and a missing event
cannot. Every event stores the pool address, so the token can be filled in later. An update
the datasource threw away was never seen at all.

## Storage

Six tables. Migrations in `migrations/`.

### Amounts are integers, never floats

Lamports for SOL, raw units for tokens.

A `u64` of lamports can exceed what a 64 bit float represents exactly. Past that point
arithmetic rounds silently, and totals built from rounded inputs are wrong in ways nothing
reports.

Decimals live in the `pools` table and are applied when displaying. Applying them on write
would store a number that cannot be recovered.

### `events` and `trades` are separate

`events` is an append-only record of what was decoded, keeping the full payload as JSON.
`trades` is the interpreted part, flattened into columns you can query.

They are separate so that improving the decode logic does not mean re-reading mainnet. If
interpretation changes, `trades` can be rebuilt from `events`. As one table, a decode fix
would mean refetching history.

`events` has a `parser_version` column. Changing decode logic bumps it, which makes it
possible to tell old rows from current ones and to backfill selectively.

### Writes are batched

The first version wrote one row at a time. Timing a replay of 503 events gave 173 seconds of
wall clock and 2 seconds of CPU. The process was idle for 99% of it, waiting on a database in
another region. That works out to 344 ms per event, two round trips each.

Mainnet delivers well over 100 events per second. At 344 ms per event the pipeline falls
behind, Carbon's channel fills, and updates are discarded.

Rows now collect in a buffer and are written together, either when 100 have gathered or when
500 ms have passed, whichever comes first. The size trigger is what makes it fast. The time
trigger is what stops rows sitting in memory through a quiet period. Both are needed.

The same replay now takes 4 seconds, 8.7 ms per event. On live traffic, throughput went from
3.1 events per second to 181.

The writer is its own task behind a channel. When that channel is full the sender waits
instead of discarding, which is the opposite of what Carbon's datasource does. A writer
falling behind shows up as backpressure, not as missing rows.

A flush is given 10 seconds. A normal one takes about 170 ms, so this only trips when
something is wrong. Without it, a hung database holds the writer open indefinitely and
shutdown waits behind it. That was not theoretical: one insert took 59.9 seconds during a
network outage, and Ctrl+C did nothing until the network came back.

### The checkpoint

`ingestion_checkpoints` holds exactly one row, enforced by a constraint. It records the
highest slot known to be fully processed.

It advances only when a batch commits, inside the same database transaction as that batch's
events. So it can never claim progress that was not written.

`SELECT MAX(slot) FROM events` is not the same thing. Events are written in batches. If the
process dies partway through one, some rows are there and others are not, and the missing
ones are not necessarily the highest. `MAX(slot)` returns the highest slot that arrived,
which can be well above the highest that completed. Resuming from it skips the gaps below
it, permanently and silently.

Resuming from the checkpoint means redoing part of a batch. The primary key plus
`ON CONFLICT DO NOTHING` makes those repeats no-ops, so the overlap is free.

### Dead letters

A batch that fails to write is kept in `dead_letters` with the error that killed it, instead
of being logged and dropped.

A failed transaction rolls back, so nothing partial lands. With nowhere to put those rows, up
to 100 trades disappear on one connection timeout, and the row count keeps climbing so
nothing looks wrong.

This does not cover every case. When the database itself is unreachable, writing the dead
letters fails too. A 30 minute run recorded 4 batch failures: three connection pool timeouts,
where the database was up and the rows were kept, and one network error, where they were not.

## Why not Carbon's generated tables

Carbon can generate Postgres tables for a decoder, keyed on
`__signature, __instruction_index, __stack_height, __slot`, with one table per instruction
type.

That is a good log of which instructions were called. It does not work as the storage layer
here for two reasons.

The key includes the instruction index, which as above is the outer instruction number and
is shared across a whole subtree. Adding stack height helps but does not fix it, since two
instructions at the same depth under the same outer instruction still match on both.

More importantly, one table per instruction type answers a different question. It records
instruction calls. This project records trades, across two protocols, in a shape that
supports querying by token and by time.

Different products, not better and worse versions of the same one.

## Gaps in the stream

The connection to mainnet will drop. When it comes back, the slots that passed during the
outage are simply absent, and nothing records that they are missing.

Two detectors write to `stream_gaps`, because they catch different failures.

The first uses Carbon's disconnection signal. The datasource notices when 30 seconds pass
with no messages, remembers the last slot it saw, and reports the range once the stream
resumes. That signal is easy to miss: the client takes a channel for it, and passing `None`,
which every example does, means the work is done and then thrown away.

The second watches slot numbers on every transaction. Slots are not consecutive even when
nothing is wrong, since a validator that misses its turn leaves an empty slot behind. The
threshold came from measuring rather than guessing. Across 167,000 recorded events, jumps of
1 to 10 slots happened thousands of times and jumps of 205 or more happened once each, with
nothing in between. 50 sits in that gap and is about 20 seconds of silence.

It exists because the first detector only fires when the connection visibly breaks. A stall
shorter than 30 seconds, or a stream that stays open and quietly skips ahead, produces no
disconnection at all.

Both processors share one slot counter, an atomic updated with `fetch_max`. One shared number
rather than one each, because each processor sees only part of the traffic and two counters
would disagree. `fetch_max` means the number only moves forward, so events arriving slightly
out of order cannot rewind it, and whichever processor reaches a gap first claims it while the
second sees nothing.

The counter starts empty on every run. Comparing against a slot from a previous run would
report each restart as a gap the size of the downtime, which is a different problem and one
the checkpoint already handles.

Gaps carry a status of open, recovering or closed rather than a done flag, so a recovery that
was attempted and failed does not look the same as one never tried.

A 25 second outage produced two rows with identical boundaries, 437993119 to 437993182, 63
slots, one from each detector. Two independent methods agreeing exactly is the useful part.
The duplicate is left alone: those rows record observations rather than work items, and
refetching a range twice costs nothing because the writes are idempotent.

## Filling them back in

A gap row says what is missing. Getting it back means asking an RPC node for those blocks.

`getBlocks` first, then `getBlock` for each. `getBlocks` returns only the slots in a range
that actually produced a block, so the crawl never asks for one that does not exist.
`getSignaturesForAddress` was the alternative and was rejected: it is discovery for a single
program and can miss things at the edges of a range, where a gap's boundaries always are.

Recovered transactions go through the same processors as live traffic. A separate decode
path for recovered data would be a second set of bugs, and the two would drift apart in ways
nothing would report.

Carbon ships an RPC block crawler that does this. It is not used here. Its task processor
sends with `try_send` and, on a full channel, breaks out of the loop and abandons the rest of
the block. For a backfill, whose entire purpose is completeness, that is the wrong trade: it
would fill one gap while quietly creating another. This one blocks instead, because the RPC
is happy to wait.

Blocks are fetched with a growing delay on failure, four attempts at 200ms, 400ms and 800ms.
A single failure is usually a blip. A run of them is the provider asking you to slow down,
and retrying without a delay makes that worse. Carbon's reconnect loop has no such delay and
issued 29,837 subscribe attempts during a 90 second outage, which is what prompted adding one
here.

Ranges are fetched a few slots either side of the recorded boundaries. A partial block is
most likely at the edges, and re-inserting costs nothing because of the primary key.

Two detectors can report the same hole. Both rows stay, since two independent methods
agreeing is worth recording, but recovery skips a range it has already covered in the same
run rather than fetching it twice.

A gap moves open, then recovering, then closed. It is marked recovering before the work
starts, so a crash leaves it visibly stuck rather than looking untried. Only a close records
when and how, which means the timestamp is evidence of a real recovery and the status is
just a label.

## Knowing nothing was lost

`verify` answers one question. Does the database hold exactly what a file decodes to.

Row counts cannot answer it. A count rising while data is quietly missing looks the same as
one rising correctly, because there is nothing to compare it against.

The expected list has to come from somewhere the write path cannot influence. A tally kept by
the writer would only prove the writer agrees with itself. If it skips a row and does not
count it, the tally and the table still match and the check passes.

So `verify` decodes the file a second time with no database attached. No writer is created,
so that pass cannot be shaped by anything on the storage side. It then reads back only the
signatures the file contains, since the same database also holds live data that has nothing
to do with the fixture.

The comparison runs both ways. Expected and not found means data was lost. Found and not
expected means rows exist the file cannot account for. Checking one direction catches loss
and is blind to duplication.

It compares sets, not counts. 503 against 503 still passes when one row is duplicated and a
different one is missing.

It exits non-zero, so a crash test or a CI job can fail on it without anyone reading the
output.

What it proves is that nothing was lost between decoding and storage. It does not prove the
decoding is correct, which is a separate check against the chain. And it works on fixtures
only, since live data has no file to compare against.

## Testing

The test suite runs with no network and no credentials.

`capture` records raw wire bytes to a JSON Lines file, one transaction per line, base64
encoded, with slot and signature stored in plain text so a transaction can be found without
decoding the file.

Raw bytes are stored deliberately. A fixture holding already-decoded events would mean
replay hands back those events without the decoder ever running, which tests the file
reader and nothing else.

`replay` implements Carbon's `Datasource` trait and feeds a saved file through the same
decode path live traffic uses. Both sources satisfy the same trait, so the pipeline cannot
tell them apart, and anything proven under replay holds for live.

The main test replays a fixture twice and asserts both runs produce the same set of event
IDs. `fixtures/golden-500.jsonl` holds 500 real mainnet transactions, including 58 pump.fun
fills and 214 PumpSwap fills.

The same pair also gives reviewers a way to run the project without a paid endpoint, and
will later provide input for crash testing and benchmarking.

## What this does not do

**Carbon's silent drop is unchanged.** The datasource still calls `try_send` and still
discards without a counter when the downstream is slow. This project removed the reason its
downstream was slow. It did not change that behaviour, and any future blocking call in the
processor would bring the same invisible loss back. Adding a dropped-update counter
upstream is a proposed contribution to Carbon, not something fixed here.

**Decimals are incomplete.** Pools found through `CreatePoolEvent` have correct decimals.
Pools found through RPC do not, because the pool account stores mints but not their decimal
precision, which needs a second lookup per mint. This affects display only, since amounts
are stored raw.

**Reorgs are not handled.** The indexer subscribes at the default commitment and does not
track fork choice or handle rollback. Calling this reorg safe would need a passing fork
replacement test, and there isn't one.

**Coverage is partial.** Instruction and event types beyond those listed are decoded and
ignored rather than stored. Unknown types are logged and never panic.

**The slot watermark has not caught a gap on its own.** Every outage tested so far was long
enough that Carbon's disconnection signal fired too. The case only the watermark can catch,
a stream that stays open and skips ahead, has not been reproduced.

**`verify` cannot check live data.** It compares against a file, and live traffic has no
file. Verifying a live range needs the expected list rebuilt from the chain.
