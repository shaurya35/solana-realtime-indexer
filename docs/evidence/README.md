# Evidence

Raw output kept behind the claims in the README and DESIGN.md. None of it is
generated or edited. Each file is what a run actually printed.

## `recon-raw.log`

Ten live trades checked against the token balance changes recorded in each
transaction. Token amounts matched exactly in every case.

This is also where the pool orientation numbers come from: across 445 events,
116 pools stored the token in the base slot, 115 stored wrapped SOL there, and
214 could not be established because the trade used native SOL.

## `drop-fix-stats.txt`

Counters before and after moving pool lookups off the hot path. An RPC call
inside the processor was costing 66% of the stream, invisibly, because the
datasource discards without a counter when it cannot keep up.

## `30min-run.txt`

Final stats line from a 30 minute unattended run against mainnet. 167,085
events, zero panics, 4 database errors. The number below it is how many
transactions the datasource refused because the pipeline could not keep up.

## `repair.txt`

Two runs of `repair` against a backlog of 8,315 events that had been stored but
never interpreted. 7,771 recovered, every remaining row accounted for by a
named reason, and a second run that wrote nothing. Also the measurement behind
the token-to-token limit: 465 events across 150 pools that the current schema
cannot represent.

## `dead-letters.txt`

The first run in which a failed batch was actually parked. The insert had been
targeting a misspelled table since the feature was written, so the README
claimed a safety property the code did not have. Includes the method for
forcing every flush to fail against a healthy database, and two limits the run
exposed.

## `gaptest.log.gz`, `gaptest2.log.gz`

Full logs from two runs where the network was pulled for about a minute, to
test gap detection.

They are also the evidence behind
[Carbon issue #580](https://github.com/sevenlabs-hq/carbon/issues/580): the
Yellowstone datasource retries `subscribe` with no delay, which came to roughly
3,000 attempts a second while the machine had no network.

```
$ grep -c "Failed to subscribe" gaptest2.log
29976
```

The three lines that actually described the outage were at 1311, 1312 and
31296. Everything between them was the same error.
