use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::db::{self, PendingWrite};
use crate::metrics::{DB_WRITE_ERRORS, inc};

const BATCH_SIZE: usize = 100;

const FLUSH_INTERVAL: Duration = Duration::from_millis(500);

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

pub fn spawn_writer(db: PgPool, capacity: usize) -> Writer {
    let (tx, mut rx) = mpsc::channel::<PendingWrite>(capacity);

    tokio::spawn(async move {
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

    Writer { tx }
}

async fn flush(db: &PgPool, batch: &mut Vec<PendingWrite>) {
    if batch.is_empty() {
        return;
    }

    if let Err(err) = db::write_events(db, batch).await {
        inc(&DB_WRITE_ERRORS);
        eprintln!("db: event batch of {} failed: {err}", batch.len());
        batch.clear();
        return;
    }

    if let Err(err) = db::write_trades(db, batch).await {
        inc(&DB_WRITE_ERRORS);
        eprintln!("db: trade batch failed: {err}");
    }

    batch.clear();
}