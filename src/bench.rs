use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use carbon_core::datasource::{Datasource, DatasourceId, Update, UpdateType};
use carbon_core::error::CarbonResult;
use chrono::Utc;
use serde::Serialize;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_signature::Signature;
use tokio::sync::mpsc;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;

use crate::config::{SOLANA_RPC_URL, database_url};
use crate::datasources::replay::ReplayDatasource;
use crate::db;
use crate::metrics::{
    DB_WRITE_ERRORS, DEAD_LETTERS, EVENTS_DECODED, HistogramSummary, LOOKUPS_DROPPED,
    POOL_CACHE_MISSES, POOL_LOOKUP_ERRORS, POOL_LOOKUPS, RECEIVE_TO_COMMITTED, REPLAY_SKIPPED, get,
};
use crate::pipeline::run_pipeline;

#[derive(Default)]
struct BenchStats {
    sent: AtomicU64,
    elapsed_us: AtomicU64,
    max_schedule_lag_us: AtomicU64,
    max_channel_send_wait_us: AtomicU64,
}

impl BenchStats {
    fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    fn elapsed(&self) -> Duration {
        Duration::from_micros(self.elapsed_us.load(Ordering::Relaxed))
    }

    fn max_schedule_lag(&self) -> Duration {
        Duration::from_micros(self.max_schedule_lag_us.load(Ordering::Relaxed))
    }

    fn max_channel_send_wait(&self) -> Duration {
        Duration::from_micros(self.max_channel_send_wait_us.load(Ordering::Relaxed))
    }

    fn finish(&self, sent: u64, elapsed: Duration) {
        self.sent.store(sent, Ordering::Relaxed);
        self.elapsed_us
            .store(duration_us(elapsed), Ordering::Relaxed);
    }

    fn record_schedule_lag(&self, elapsed: Duration) {
        self.max_schedule_lag_us
            .fetch_max(duration_us(elapsed), Ordering::Relaxed);
    }

    fn record_channel_send_wait(&self, elapsed: Duration) {
        self.max_channel_send_wait_us
            .fetch_max(duration_us(elapsed), Ordering::Relaxed);
    }
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

struct PacedReplayDatasource {
    inner: ReplayDatasource,
    rate: u64,
    signature_salt: u64,
    stats: Arc<BenchStats>,
}

#[async_trait]
impl Datasource for PacedReplayDatasource {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: mpsc::Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()> {
        let (replay_sender, mut replay_receiver) = mpsc::channel(1);
        let producer_token = cancellation_token.clone();

        let producer = self.inner.consume(id, replay_sender, producer_token);

        let forwarder = async {
            let started = TokioInstant::now();
            let mut sent = 0u64;

            while let Some((mut update, datasource_id)) = replay_receiver.recv().await {
                if cancellation_token.is_cancelled() {
                    replay_receiver.close();
                    break;
                }

                let scheduled_at =
                    started + Duration::from_secs_f64(sent as f64 / self.rate as f64);

                tokio::time::sleep_until(scheduled_at).await;

                make_signature_unique(&mut update, self.signature_salt, sent);

                let send_started = Instant::now();

                if sender.send((update, datasource_id)).await.is_err() {
                    replay_receiver.close();
                    break;
                }

                self.stats.record_channel_send_wait(send_started.elapsed());

                self.stats.record_schedule_lag(
                    TokioInstant::now().saturating_duration_since(scheduled_at),
                );

                sent += 1;
            }

            self.stats.finish(sent, started.elapsed());
        };

        let (producer_result, ()) = tokio::join!(producer, forwarder);

        producer_result
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![UpdateType::Transaction]
    }
}

fn make_signature_unique(update: &mut Update, salt: u64, index: u64) {
    let Update::Transaction(transaction) = update else {
        return;
    };

    let mut bytes = transaction.signature.as_ref().to_vec();
    let mask = salt.wrapping_add(index).to_le_bytes();

    for (signature_byte, mask_byte) in bytes.iter_mut().take(mask.len()).zip(mask) {
        *signature_byte ^= mask_byte;
    }

    if let Ok(signature) = Signature::try_from(bytes.as_slice()) {
        transaction.signature = signature;
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkResult {
    schema_version: u32,
    generated_at_utc: String,

    fixture: String,
    requested_tps: u64,
    repeat: u32,

    input_transactions: u64,
    expected_events: u64,
    decoded_events: u64,
    missing_events: u64,
    unexpected_events: u64,
    uncommitted_rows: u64,

    pipeline_elapsed_seconds: f64,
    source_elapsed_seconds: f64,
    source_achieved_tps: f64,
    decoded_events_per_second: f64,

    max_schedule_lag_ms: f64,
    max_channel_send_wait_ms: f64,
    writer_to_commit: HistogramSummary,

    replay_skipped: u64,
    pool_cache_misses: u64,
    pool_lookups: u64,
    pool_lookup_errors: u64,
    lookups_dropped: u64,
    database_write_errors: u64,
    dead_letters: u64,

    falling_behind: bool,
}

pub async fn run_benchmark(
    path: String,
    rate: u64,
    repeat: u32,
    expected_events_per_pass: u64,
    output: String,
) -> Result<(), Box<dyn std::error::Error>> {
    if rate == 0 {
        return Err("--rate must be greater than zero".into());
    }

    if repeat == 0 {
        return Err("--repeat must be greater than zero for a benchmark".into());
    }

    let expected_events = expected_events_per_pass
        .checked_mul(u64::from(repeat))
        .ok_or("expected event count overflowed")?;

    let database_url = database_url().ok_or("DATABASE_URL not set")?;

    let database = db::connect(&database_url).await?;
    let stats = Arc::new(BenchStats::default());

    let datasource = PacedReplayDatasource {
        inner: ReplayDatasource {
            path: path.clone(),
            repeat,
        },
        rate,
        signature_salt: signature_salt()?,
        stats: stats.clone(),
    };

    let pipeline_started = Instant::now();

    run_pipeline(
        datasource,
        Some(RpcClient::new(SOLANA_RPC_URL.to_string())),
        None,
        Some(database),
        None,
    )
    .await?;

    let pipeline_elapsed = pipeline_started.elapsed();
    let source_elapsed = stats.elapsed();

    let decoded_events = get(&EVENTS_DECODED);
    let writer_to_commit = RECEIVE_TO_COMMITTED.summary();

    let source_achieved_tps = if source_elapsed.is_zero() {
        0.0
    } else {
        stats.sent() as f64 / source_elapsed.as_secs_f64()
    };

    let decoded_events_per_second = if pipeline_elapsed.is_zero() {
        0.0
    } else {
        decoded_events as f64 / pipeline_elapsed.as_secs_f64()
    };

    let missing_events = expected_events.saturating_sub(decoded_events);

    let unexpected_events = decoded_events.saturating_sub(expected_events);

    let uncommitted_rows = decoded_events.saturating_sub(writer_to_commit.count);

    let max_schedule_lag_ms = stats.max_schedule_lag().as_secs_f64() * 1_000.0;

    let max_channel_send_wait_ms = stats.max_channel_send_wait().as_secs_f64() * 1_000.0;

    let replay_skipped = get(&REPLAY_SKIPPED);
    let pool_cache_misses = get(&POOL_CACHE_MISSES);
    let pool_lookups = get(&POOL_LOOKUPS);
    let pool_lookup_errors = get(&POOL_LOOKUP_ERRORS);
    let lookups_dropped = get(&LOOKUPS_DROPPED);
    let database_write_errors = get(&DB_WRITE_ERRORS);
    let dead_letters = get(&DEAD_LETTERS);

    let falling_behind = source_achieved_tps < rate as f64 * 0.95
        || max_schedule_lag_ms > 1_000.0
        || missing_events > 0
        || unexpected_events > 0
        || uncommitted_rows > 0
        || replay_skipped > 0
        || pool_cache_misses > 0
        || pool_lookups > 0
        || pool_lookup_errors > 0
        || lookups_dropped > 0
        || database_write_errors > 0
        || dead_letters > 0;

    let result = BenchmarkResult {
        schema_version: 1,
        generated_at_utc: Utc::now().to_rfc3339(),

        fixture: path,
        requested_tps: rate,
        repeat,

        input_transactions: stats.sent(),
        expected_events,
        decoded_events,
        missing_events,
        unexpected_events,
        uncommitted_rows,

        pipeline_elapsed_seconds: pipeline_elapsed.as_secs_f64(),
        source_elapsed_seconds: source_elapsed.as_secs_f64(),
        source_achieved_tps,
        decoded_events_per_second,

        max_schedule_lag_ms,
        max_channel_send_wait_ms,
        writer_to_commit,

        replay_skipped,
        pool_cache_misses,
        pool_lookups,
        pool_lookup_errors,
        lookups_dropped,
        database_write_errors,
        dead_letters,

        falling_behind,
    };

    let output_path = std::path::Path::new(&output);

    if let Some(parent) = output_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(
        output_path,
        format!("{}\n", serde_json::to_string_pretty(&result)?),
    )?;

    let p99 = result
        .writer_to_commit
        .p99_upper_ms
        .map(|value| format!("{value:.1} ms"))
        .unwrap_or_else(|| "overflow bucket".to_string());

    println!(
        "benchmark result: requested={} tx/s \
         achieved={:.1} tx/s decoded={} expected={} \
         max_lag={:.1} ms writer_p99<={} \
         falling_behind={}",
        result.requested_tps,
        result.source_achieved_tps,
        result.decoded_events,
        result.expected_events,
        result.max_schedule_lag_ms,
        p99,
        result.falling_behind,
    );

    println!("benchmark JSON written to {output}");

    Ok(())
}

fn signature_salt() -> Result<u64, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

    Ok((nanos as u64) ^ ((nanos >> 64) as u64) ^ u64::from(std::process::id()))
}
