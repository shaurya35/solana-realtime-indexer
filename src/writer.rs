use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::db::{self, PendingWrite};
use crate::metrics::{
    DB_WRITE_ERRORS, DEAD_LETTERS, FLUSH_TIME, RECEIVE_TO_COMMITTED, inc, set_committed_slot,
};

const BATCH_SIZE: usize = 100;

const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct Writer {
    tx: mpsc::Sender<PendingWrite>,
}

impl Writer {
    pub async fn send(&self, row: PendingWrite) {
        if self.tx.send(row).await.is_err() {
            inc(&DB_WRITE_ERRORS);
        }
    }
}

pub fn spawn_writer(db: PgPool, capacity: usize) -> (Writer, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<PendingWrite>(capacity);

    let handle = tokio::spawn(async move {
        let mut batch: Vec<PendingWrite> = Vec::with_capacity(BATCH_SIZE);
        let mut tick = tokio::time::interval(FLUSH_INTERVAL);

        loop {
            tokio::select! {
                received = rx.recv() => {
                    match received {
                        Some(row) => {
                            batch.push(row);
                            if batch.len() >= BATCH_SIZE {
                                flush(&db, &mut batch).await;
                            }
                        }
                        None => {
                            flush(&db, &mut batch).await;
                            break;
                        }
                    }
                }
                _ = tick.tick() => {
                    flush(&db, &mut batch).await;
                }
            }
        }
    });

    (Writer { tx }, handle)
}

async fn flush(db: &PgPool, batch: &mut Vec<PendingWrite>) {
    let Some((slot, signature)) = batch
        .iter()
        .max_by_key(|r| r.event.slot)
        .map(|r| (r.event.slot, r.event.signature.clone()))
    else {
        return;
    };

    let started = Instant::now();

    let attempt =
        tokio::time::timeout(FLUSH_TIMEOUT, write_batch(db, batch, slot, &signature)).await;

    match attempt {
        Ok(Ok(())) => {
            FLUSH_TIME.observe(started.elapsed());

            for row in batch.iter() {
                RECEIVE_TO_COMMITTED.observe(row.queued_at.elapsed());
            }

            set_committed_slot(slot as u64);
        }

        Ok(Err(err)) => {
            inc(&DB_WRITE_ERRORS);
            eprintln!("db: batch of {} failed: {err}", batch.len());

            match db::write_dead_letters(db, batch, &err.to_string()).await {
                Ok(()) => {
                    for _ in batch.iter() {
                        inc(&DEAD_LETTERS);
                    }
                }
                Err(e) => eprintln!("db: could not park {} dead letters: {e}", batch.len()),
            }
        }

        Err(_) => {
            inc(&DB_WRITE_ERRORS);
            let err = format!("flush timed out after {FLUSH_TIMEOUT:?}");
            eprintln!("db: batch of {} {err}", batch.len());

            match db::write_dead_letters(db, batch, &err).await {
                Ok(()) => {
                    for _ in batch.iter() {
                        inc(&DEAD_LETTERS);
                    }
                }
                Err(e) => eprintln!("db: could not park {} dead letters: {e}", batch.len()),
            }
        }
    }

    batch.clear();
}

async fn write_batch(
    db: &PgPool,
    batch: &[PendingWrite],
    slot: i64,
    signature: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = db.begin().await?;

    db::write_events(&mut tx, batch).await?;
    db::write_trades(&mut tx, batch).await?;
    db::write_checkpoint(&mut tx, slot, signature).await?;

    tx.commit().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{EventRow, TradeRow};
    use rust_decimal::Decimal;

    fn one_bad_batch() -> Vec<PendingWrite> {
        vec![PendingWrite {
            queued_at: Instant::now(),
            event: EventRow {
                signature: "testsig".to_string(),
                absolute_path: vec![1, 2],
                event_ordinal: 0,
                slot: 1,
                block_time: None,
                program: "pumpswap",
                event_type: "BuyEvent",
                payload: serde_json::json!({}),
            },
            trade: Some(TradeRow {
                pool: None,
                token_mint: "mint".to_string(),
                side: "sideways",
                sol_amount: Decimal::from(1),
                token_amount: Decimal::from(1),
                trader: "trader".to_string(),
                fee: None,
            }),
        }]
    }

    #[tokio::test]
    async fn failed_batch_rolls_back_and_parks() {
        let Some(db) = crate::db::test_pool().await else {
            return;
        };

        let mut batch = one_bad_batch();
        flush(&db, &mut batch).await;

        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&db)
            .await
            .unwrap();

        let dead: i64 = sqlx::query_scalar("SELECT count(*) FROM dead_letters")
            .fetch_one(&db)
            .await
            .unwrap();

        assert_eq!(events, 0, "...");
        assert_eq!(dead, 1, "...");
        assert!(batch.is_empty(), "...");
    }
}
