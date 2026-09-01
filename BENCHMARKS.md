# Replay benchmark results

I added a `bench` command to replay the committed fixture through the normal
indexer pipeline at a controlled rate. The goal was to find the point where the
local pipeline could no longer keep up.

## Result

The indexer stayed clean through 4,800 transactions per second. At a requested
9,600 transactions per second, it processed a median 8,502.81 transactions per
second and built up 3.88 seconds of schedule lag.

No events were lost at the overloaded rate. The pipeline slowed down and
applied backpressure instead.

- Highest tested clean rate: **4,800 transactions/second**
- First tested rate that fell behind: **9,600 transactions/second**
- Observed boundary: **between 4,800 and 9,600 transactions/second**

## Test setup

- Machine: MacBook Pro, Apple M5 Pro, 15-core CPU, 24 GB memory
- Operating system: macOS 26.5.2
- Rust: 1.96.1
- Database: PostgreSQL 16 in Docker
- Build: release mode
- Fixture: `fixtures/golden-500.jsonl`
- Fixture contents: 500 transactions and 503 decoded events per pass
- Trials: three per rate
- Writer batch size: 100 rows
- Writer flush interval: 500 ms
- Pipeline queue: 10,000 entries
- Writer queue: 10,000 entries

I used 120 transactions per second as the normal load, then tested 5x, 10x,
and doubled the rate until the pipeline fell behind.

A rate counted as falling behind when median achieved throughput dropped below
95% of the requested rate or median schedule lag exceeded one second. Missing
events, uncommitted rows, database errors, or dead letters would also invalidate
a run.

## Measurements

Values in the table are medians from three trials. Writer p99 is the upper edge
of a histogram bucket, not an exact percentile value.

| Requested tx/s | Median achieved tx/s | Median schedule lag | Writer mean | Writer p99 upper bound | Result |
|---:|---:|---:|---:|---:|:---|
| 120 | 120.02 | 3.00 ms | 272.89 ms | 2,000 ms | Clean |
| 600 | 600.01 | 5.29 ms | 102.12 ms | 500 ms | Clean |
| 1,200 | 1,199.98 | 4.62 ms | 60.49 ms | 500 ms | Clean |
| 2,400 | 2,399.95 | 8.72 ms | 35.76 ms | 100 ms | Clean |
| 4,800 | 4,799.85 | 84.50 ms | 25.96 ms | 100 ms | Clean |
| 9,600 | 8,502.81 | 3,879.29 ms | 1,164.31 ms | 2,000 ms | Fell behind |

Across the 18 valid trials, the benchmark sent 1,686,000 transactions and
decoded all 1,696,116 expected events. It recorded zero missing events, zero
uncommitted rows, zero database write errors, and zero dead letters.

## Evidence

- [Machine-readable summary](benchmarks/summary.json)
- [Raw trial results](benchmarks/results/2026-09-01/)

The preliminary smoke run is not included. It started before the pool seed had
finished and loaded 91 of 92 pools, so it was not a valid benchmark trial.

## Limits

These numbers describe one local machine, one fixture, and a local PostgreSQL
container. They are useful for comparing this indexer's behaviour under load,
but they are not a claim about every deployment. The exact breaking point was
not measured; the tested boundary is between 4,800 and 9,600 transactions per
second.
