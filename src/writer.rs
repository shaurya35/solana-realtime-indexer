use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::db::{self, PendingWrite};
use crate::metrics::{DB_WRITE_ERRORS, DEAD_LETTERS, inc};

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

    let mut tx = match db.begin().await {
        Ok(tx) => tx,
        Err(err) => {
            inc(&DB_WRITE_ERRORS);
            eprintln!("db: could not start transaction: {err}");
            batch.clear();
            return;
        }
    };

    let result = async {
        db::write_events(&mut *tx, batch).await?;
        db::write_trades(&mut *tx, batch).await?;
        db::write_checkpoint(&mut *tx, slot, &signature).await
    }
    .await;

    match result {
        Ok(()) => {
            if let Err(err) = tx.commit().await {
                inc(&DB_WRITE_ERRORS);
                eprintln!("db: commit failed, batch of {}: {err}", batch.len());
            }
        }
        Err(err) => {
            inc(&DB_WRITE_ERRORS);
            eprintln!("db: batch of {} failed, rolled back: {err}", batch.len());

            match db::write_dead_letters(db, batch, &err.to_string()).await {
                Ok(()) => {
                    for _ in batch.iter() {
                        inc(&DEAD_LETTERS);
                    }
                }
                Err(e) => println!("db: could not park {} dead letters: {e}", batch.len()),
            }
        }
    }

    batch.clear();
}
