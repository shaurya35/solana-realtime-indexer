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
                "[stats] decoded={decoded} ({rate:.1}/s) cache_hits={} cache_misses={} lookups={} lookup_errors={} lookups_dropped={} skipped_failed={} replay_skipped={} db_errors={} dead_letters={}, gaps={}",
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
            );
        }
    });
}
