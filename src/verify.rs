use std::collections::BTreeSet;

use sqlx::{PgPool, Row};

use crate::datasources::replay::ReplayDatasource;
use crate::identity::{EventId, EventLog};
use crate::pipeline::run_pipeline;

pub async fn run_verify(path: String, db: &PgPool) -> Result<bool, Box<dyn std::error::Error>> {

    let events = EventLog::default();
    run_pipeline(ReplayDatasource { path, repeat: 1 }, None, events.clone(), None).await?;

    let expected: BTreeSet<EventId> = events.lock().unwrap().iter().cloned().collect();

    let signatures: Vec<String> = expected.iter().map(|e| e.signature.clone()).collect();

    let rows = sqlx::query(
        "SELECT signature, absolute_path, event_ordinal
         FROM events
         WHERE signature = ANY($1)",
    )
    .bind(&signatures[..])
    .fetch_all(db)
    .await?;

    let actual: BTreeSet<EventId> = rows
        .into_iter()
        .map(|r| EventId {
            signature: r.get("signature"),
            absolute_path: r.get("absolute_path"),
            event_ordinal: r.get::<i32, _>("event_ordinal") as u32,
        })
        .collect();

    let missing: Vec<&EventId> = expected.difference(&actual).collect();
    let extra: Vec<&EventId> = actual.difference(&expected).collect();

    println!("expected {}", expected.len());
    println!("actual   {}", actual.len());
    println!("missing  {}", missing.len());
    println!("extra    {}", extra.len());

    for e in missing.iter().take(10) {
        println!("  missing {} path={:?} ord={}", e.signature, e.absolute_path, e.event_ordinal);
    }
    for e in extra.iter().take(10) {
        println!("  extra   {} path={:?} ord={}", e.signature, e.absolute_path, e.event_ordinal);
    }

    Ok(missing.is_empty() && extra.is_empty())
}