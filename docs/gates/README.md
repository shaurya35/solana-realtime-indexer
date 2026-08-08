# Gates

Raw output kept as evidence for claims made in the README and DESIGN.md.
Nothing here is generated. Each file is the actual output of a run.

## week1

**`recon-raw.log`**
Ten live trades checked against the token balance changes recorded in each
transaction. This is where the pool orientation numbers come from: 116 normal,
115 inverted, 214 undetermined across 445 events.

**`drop-fix-stats.txt`**
Before and after counters for moving pool lookups off the hot path. The 66%
drop rate and the throughput change come from here.

**`30min-run.txt`**
Final stats line from a 30 minute unattended run on mainnet. 167,085 events,
zero panics, 4 database errors. The second number is the count of transactions
the datasource refused because the pipeline could not keep up.
