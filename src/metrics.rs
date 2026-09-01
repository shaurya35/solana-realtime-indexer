use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub static EVENTS_DECODED: AtomicU64 = AtomicU64::new(0);
pub static SKIPPED_FAILED: AtomicU64 = AtomicU64::new(0);
pub static POOL_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
pub static POOL_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
pub static POOL_LOOKUPS: AtomicU64 = AtomicU64::new(0);
pub static POOL_LOOKUP_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static LOOKUPS_DROPPED: AtomicU64 = AtomicU64::new(0);
pub static REPLAY_SKIPPED: AtomicU64 = AtomicU64::new(0);
pub static DB_WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static DEAD_LETTERS: AtomicU64 = AtomicU64::new(0);
pub static GAP_DETECTED: AtomicU64 = AtomicU64::new(0);
pub static HEAD_SLOT: AtomicU64 = AtomicU64::new(0);
pub static COMMITTED_SLOT: AtomicU64 = AtomicU64::new(0);
pub static TRADES_WRITTEN: AtomicU64 = AtomicU64::new(0);
pub static UNORIENTED: AtomicU64 = AtomicU64::new(0);
pub static ORIENTED_NORMAL: AtomicU64 = AtomicU64::new(0);
pub static ORIENTED_INVERTED: AtomicU64 = AtomicU64::new(0);

const BUCKETS: usize = 10;

const BUCKET_EDGES_US: [u64; BUCKETS - 1] = [
    500, 1_000, 5_000, 25_000, 100_000, 500_000, 2_000_000, 10_000_000, 60_000_000,
];

#[derive(Debug, Clone, serde::Serialize)]
pub struct HistogramSummary {
    pub count: u64,
    pub mean_ms: Option<f64>,
    pub p50_upper_ms: Option<f64>,
    pub p95_upper_ms: Option<f64>,
    pub p99_upper_ms: Option<f64>,
}

pub struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    sum_us: AtomicU64,
}

impl Histogram {
    const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; BUCKETS],
            sum_us: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, elapsed: Duration) {
        let us = elapsed.as_micros() as u64;

        let slot = BUCKET_EDGES_US
            .iter()
            .position(|&edge| us <= edge)
            .unwrap_or(BUCKETS - 1);

        self.buckets[slot].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
    }

    pub fn summary(&self) -> HistogramSummary {
        let buckets: [u64; BUCKETS] =
            std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed));

        let count = buckets.iter().sum();
        let sum_us = self.sum_us.load(Ordering::Relaxed);

        HistogramSummary {
            count,
            mean_ms: if count == 0 {
                None
            } else {
                Some(sum_us as f64 / count as f64 / 1_000.0)
            },
            p50_upper_ms: percentile_upper_ms(&buckets, count, 0.50),
            p95_upper_ms: percentile_upper_ms(&buckets, count, 0.95),
            p99_upper_ms: percentile_upper_ms(&buckets, count, 0.99),
        }
    }
}

fn percentile_upper_ms(buckets: &[u64; BUCKETS], count: u64, percentile: f64) -> Option<f64> {
    if count == 0 {
        return None;
    }

    let target = (count as f64 * percentile).ceil() as u64;
    let mut running = 0u64;

    for (index, value) in buckets.iter().enumerate() {
        running += value;

        if running >= target {
            return BUCKET_EDGES_US
                .get(index)
                .map(|edge| *edge as f64 / 1_000.0);
        }
    }

    None
}

pub static DECODE_TIME: Histogram = Histogram::new();
pub static FLUSH_TIME: Histogram = Histogram::new();
pub static RECEIVE_TO_COMMITTED: Histogram = Histogram::new();

pub fn set_head_slot(slot: u64) {
    HEAD_SLOT.fetch_max(slot, Ordering::Relaxed);
}

pub fn set_committed_slot(slot: u64) {
    COMMITTED_SLOT.fetch_max(slot, Ordering::Relaxed);
}

pub fn commit_lag_slots() -> u64 {
    let committed = COMMITTED_SLOT.load(Ordering::Relaxed);

    if committed == 0 {
        return 0;
    }

    HEAD_SLOT.load(Ordering::Relaxed).saturating_sub(committed)
}

pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn get(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

pub fn spawn_reporter() {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        let mut last_decoded = 0u64;

        loop {
            tick.tick().await;

            let decoded = get(&EVENTS_DECODED);
            let rate = (decoded - last_decoded) as f64 / 10.0;
            last_decoded = decoded;

            println!(
                "[stats] decoded={decoded} ({rate:.1}/s) cache_hits={} cache_misses={} lookups={} lookup_errors={} lookups_dropped={} skipped_failed={} replay_skipped={} db_errors={} dead_letters={}, gaps={} lag={} slots trades={} unoriented={}",
                get(&POOL_CACHE_HITS),
                get(&POOL_CACHE_MISSES),
                get(&POOL_LOOKUPS),
                get(&POOL_LOOKUP_ERRORS),
                get(&LOOKUPS_DROPPED),
                get(&SKIPPED_FAILED),
                get(&REPLAY_SKIPPED),
                get(&DB_WRITE_ERRORS),
                get(&DEAD_LETTERS),
                get(&GAP_DETECTED),
                commit_lag_slots(),
                get(&TRADES_WRITTEN),
                get(&UNORIENTED),
            );
        }
    });
}

pub fn render() -> String {
    let mut out = String::new();

    let counters = [
        (
            "indexer_events_decoded_total",
            "events decoded from the stream",
            &EVENTS_DECODED,
        ),
        (
            "indexer_skipped_failed_total",
            "transactions skipped because they failed on chain",
            &SKIPPED_FAILED,
        ),
        (
            "indexer_pool_cache_hits_total",
            "pool lookups answered from memory",
            &POOL_CACHE_HITS,
        ),
        (
            "indexer_pool_cache_misses_total",
            "pool lookups not in memory",
            &POOL_CACHE_MISSES,
        ),
        (
            "indexer_pool_lookups_total",
            "pool lookups sent to RPC",
            &POOL_LOOKUPS,
        ),
        (
            "indexer_pool_lookup_errors_total",
            "pool lookups RPC could not answer",
            &POOL_LOOKUP_ERRORS,
        ),
        (
            "indexer_lookups_dropped_total",
            "pool lookups discarded because the queue was full",
            &LOOKUPS_DROPPED,
        ),
        (
            "indexer_replay_skipped_total",
            "fixture lines replay could not use",
            &REPLAY_SKIPPED,
        ),
        (
            "indexer_db_write_errors_total",
            "batches the database refused",
            &DB_WRITE_ERRORS,
        ),
        (
            "indexer_dead_letters_total",
            "rows parked after a failed batch",
            &DEAD_LETTERS,
        ),
        (
            "indexer_gaps_detected_total",
            "stream gaps recorded",
            &GAP_DETECTED,
        ),
        (
            "indexer_trades_written_total",
            "events that produced a trade row",
            &TRADES_WRITTEN,
        ),
        (
            "indexer_unoriented_total",
            "events stored with no trade row, pool could not be oriented",
            &UNORIENTED,
        ),
        (
            "indexer_oriented_normal_total",
            "trades from pools where the quote side is wrapped SOL",
            &ORIENTED_NORMAL,
        ),
        (
            "indexer_oriented_inverted_total",
            "trades from pools where the base side is wrapped SOL",
            &ORIENTED_INVERTED,
        ),
    ];

    for (name, help, counter) in counters {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}\n",
            get(counter)
        ));
    }

    out.push_str(&format!(
        "# HELP indexer_commit_lag_slots slots between the newest slot seen and the newest committed\n\
         # TYPE indexer_commit_lag_slots gauge\n\
         indexer_commit_lag_slots {}\n",
        commit_lag_slots()
    ));

    histogram(
        &mut out,
        "indexer_decode_seconds",
        "time from an instruction arriving to it being counted as decoded",
        &DECODE_TIME,
    );
    histogram(
        &mut out,
        "indexer_flush_seconds",
        "time to commit one batch",
        &FLUSH_TIME,
    );
    histogram(
        &mut out,
        "indexer_receive_to_committed_seconds",
        "time from a row reaching the writer to its batch committing",
        &RECEIVE_TO_COMMITTED,
    );

    out
}

fn histogram(out: &mut String, name: &str, help: &str, hist: &Histogram) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));

    let mut running = 0u64;

    for (slot, edge) in BUCKET_EDGES_US.iter().enumerate() {
        running += hist.buckets[slot].load(Ordering::Relaxed);
        let seconds = *edge as f64 / 1_000_000.0;
        out.push_str(&format!("{name}_bucket{{le=\"{seconds}\"}} {running}\n"));
    }

    running += hist.buckets[BUCKETS - 1].load(Ordering::Relaxed);

    out.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {running}\n"));
    out.push_str(&format!(
        "{name}_sum {}\n",
        hist.sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
    ));
    out.push_str(&format!("{name}_count {running}\n"));
}

pub fn spawn_exporter(port: u16) {
    tokio::spawn(async move {
        let app = axum::Router::new().route("/metrics", axum::routing::get(|| async { render() }));

        match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
            Ok(listener) => {
                println!("metrics on http://localhost:{port}/metrics");
                if let Err(err) = axum::serve(listener, app).await {
                    eprintln!("metrics: server stopped: {err}");
                }
            }
            Err(err) => eprintln!("metrics: could not bind port {port}: {err}"),
        }
    });
}

#[cfg(test)]
mod histogram_tests {
    use super::*;

    #[test]
    fn summary_reports_percentile_bucket_bounds() {
        let histogram = Histogram::new();

        histogram.observe(Duration::from_millis(1));
        histogram.observe(Duration::from_millis(20));
        histogram.observe(Duration::from_millis(80));
        histogram.observe(Duration::from_millis(600));

        let summary = histogram.summary();

        assert_eq!(summary.count, 4);
        assert_eq!(summary.p50_upper_ms, Some(25.0));
        assert_eq!(summary.p95_upper_ms, Some(2_000.0));
        assert_eq!(summary.p99_upper_ms, Some(2_000.0));
    }
}
