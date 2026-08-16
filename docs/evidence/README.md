# Evidence

Raw output behind the claims in the README and DESIGN.md. None of it is generated or edited.
Each file is what a run actually printed.

Grouped by what it proves.

```
correctness/   the numbers are right
throughput/    it keeps up
durability/    it does not lose things
gaps/          it notices when the stream breaks
12h-run/       one unattended run, 15-16 August 2026
```

Images embedded in the README and DESIGN.md live in `docs/images/`. Screenshots that only
back a specific run stay with that run.

---

## `correctness/`

**`recon-raw.log`** — ten live trades checked against the token balance changes recorded in
each transaction. Token amounts matched exactly in every case.

This is also where the pool orientation numbers come from: across 445 events, 116 pools
stored the token in the base slot, 115 stored wrapped SOL there, and 214 could not be
established because the trade used native SOL.

**`repair.txt`** — two runs of `repair` against a backlog of 8,315 events that had been
stored but never interpreted. 7,771 recovered, every remaining row accounted for by a named
reason, and a second run that wrote nothing. Also the measurement behind the token-to-token
limit: 465 events across 150 pools the current schema cannot represent.

## `throughput/`

**`drop-fix-stats.txt`** — counters before and after moving pool lookups off the hot path.
An RPC call inside the processor was costing 66% of the stream, invisibly, because the
datasource discards without a counter when it cannot keep up.

**`30min-run.txt`** — final stats line from a 30 minute unattended run against mainnet.
167,085 events, zero panics, 4 database errors. The number below it is how many transactions
the datasource refused because the pipeline could not keep up. See `12h-run/` for what that
figure looks like once the database is local.

## `durability/`

**`dead-letters.txt`** — the first run in which a failed batch was actually parked. The
insert had been targeting a misspelled table since the feature was written, so the README
claimed a safety property the code did not have. Includes the method for forcing every flush
to fail against a healthy database, and two limits the run exposed.

## `gaps/`

**`gaptest.log.gz`, `gaptest2.log.gz`** — full logs from two runs where the network was
pulled for about a minute, to test gap detection.

They are also the evidence behind
[Carbon issue #580](https://github.com/sevenlabs-hq/carbon/issues/580): the Yellowstone
datasource retries `subscribe` with no delay, which came to roughly 3,000 attempts a second
while the machine had no network.

```
$ grep -c "Failed to subscribe" gaptest2.log
29976
```

The three lines that actually described the outage were at 1311, 1312 and 31296. Everything
between them was the same error.

## `12h-run/`

One unattended run against mainnet, 21:39 on 15 August to 09:39 on 16 August 2026, stopped
by its own watchdog at the twelve hour mark.

**`run-summary.txt`** — start, end, and why it stopped.

**`watchdog.log`** — 720 samples, one a minute. Memory, free disk, elapsed time, commit lag
and decoded count. Memory held between 29.5 and 36.2 MB for the whole run with no trend. The
720 samples with no gap larger than 90 seconds also show the machine never slept, so the run
is twelve real hours.

**`final-metrics.txt`** — the complete metrics endpoint output, taken seven minutes before
the end. 12,180,605 events decoded, zero refused, zero dead letters.

**`counts.txt`** — row counts after recovery. 12,439,266 events, 12,415,571 trades, no dead
letters.

**`gaps.txt`** — the stream dropped once, for 938 slots. Both detectors recorded the same
range, to the slot, independently. Captured before the deduplication migration removed the
second row.

**`gap-verify.txt`** — `recover` refetched the range and `verify-range` checked the result
against the chain. 105,481 events expected, 105,481 found, nothing missing and nothing extra.

**`dashboard.png`, `terminal.png`, `final-state.png`** — the Grafana panels across the run,
the script starting and stopping, and the final counters.
