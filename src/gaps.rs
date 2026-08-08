use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use tokio::sync::mpsc::Sender;

use carbon_core::datasource::DatasourceDisconnection;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::config::SLOT_GAP_TOLERANCE;
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
                &gap.source,
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

pub fn check_slot(
    watermark: &AtomicU64,
    gaps: Option<&Sender<DatasourceDisconnection>>,
    slot: u64,
) {
    let previous = watermark.fetch_max(slot, Ordering::Relaxed);

    if previous == 0 || slot <= previous + SLOT_GAP_TOLERANCE {
        return;
    }

    let Some(tx) = gaps else {
        return;
    };

    let gap = DatasourceDisconnection {
        source: "watermark".to_string(),
        disconnect_time: Utc::now(),
        last_slot_before_disconnect: previous,
        first_slot_after_reconnect: slot,
        missed_slots: slot - previous,
    };

    if tx.try_send(gap).is_err() {
        eprintln!("gaps: could not record watermark gap at slot {slot}");
    }
}
