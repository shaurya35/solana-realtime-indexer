use carbon_core::datasource::DatasourceDisconnection;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::db;
use crate::metrics::{DB_WRITE_ERRORS, GAP_DETECTED, inc};

pub fn spawn_gap_recorder(db: PgPool, capacity: usize) -> mpsc::Sender<DatasourceDisconnection> {
    let (tx, mut rx) = mpsc::channel::<DatasourceDisconnection>(capacity);

    tokio::spawn(async move {
        while let Some(gap) = rx.recv().await {
            println!(
                "gap detected: slots {} to {}, {} missed",
                gap.last_slot_before_disconnect, gap.first_slot_after_reconnect, gap.missed_slots
            );

            let written = db::write_gap(
                &db,
                gap.last_slot_before_disconnect as i64,
                gap.first_slot_after_reconnect as i64,
                gap.missed_slots as i64,
                gap.disconnect_time,
            )
            .await;

            match written {
                Ok(()) => inc(&GAP_DETECTED),
                Err(err) => {
                    inc(&DB_WRITE_ERRORS);
                    eprintln!("db: could not record gaps, {err}");
                }
            }
        }
    });

    tx
}
