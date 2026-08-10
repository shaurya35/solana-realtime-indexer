use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

#[derive(Serialize)]
struct Trade {
    signature: String,
    slot: i64,
    program: String,
    token_mint: String,
    side: String,
    sol_amount: String,
    token_amount: String,
    trader: String,
}

#[derive(Serialize)]
struct Health {
    last_completed_slot: Option<i64>,
    events: i64,
    trades: i64,
    unresolved_gaps: i64,
}

#[derive(Serialize)]
struct Volume {
    token_mint: String,
    trades: i64,
    sol_volume: String,
}

#[derive(Deserialize)]
struct Limit {
    limit: Option<i64>,
}

fn rows(limit: &Limit) -> i64 {
    limit.limit.unwrap_or(50).clamp(1, 500)
}

fn failed(err: sqlx::error::Error) -> StatusCode {
    eprintln!("api: query failed: {err}");
    StatusCode::INTERNAL_SERVER_ERROR
}

async fn health(State(db): State<PgPool>) -> Result<Json<Health>, StatusCode> {
    let row = sqlx::query(
        "SELECT
           (SELECT last_completed_slot FROM ingestion_checkpoints WHERE id = 1) AS last_slot,
           (SELECT count(*) FROM events) AS events,
           (SELECT count(*) FROM trades) AS trades,
           (SELECT count(*) FROM stream_gaps WHERE status <> 'closed') AS unresolved_gaps",
    )
    .fetch_one(&db)
    .await
    .map_err(failed)?;

    Ok(Json(Health {
        last_completed_slot: row.get("last_slot"),
        events: row.get("events"),
        trades: row.get("trades"),
        unresolved_gaps: row.get("unresolved_gaps"),
    }))
}

fn trade_from(r: &sqlx::postgres::PgRow) -> Trade {
    Trade {
        signature: r.get("signature"),
        slot: r.get("slot"),
        program: r.get("program"),
        token_mint: r.get("token_mint"),
        side: r.get("side"),
        sol_amount: r.get::<i64, _>("sol_amount").to_string(),
        token_amount: r.get::<i64, _>("token_amount").to_string(),
        trader: r.get("trader"),
    }
}

async fn recent(
    State(db): State<PgPool>,
    Query(limit): Query<Limit>,
) -> Result<Json<Vec<Trade>>, StatusCode> {
    let rows_out = sqlx::query(
        "SELECT signature, slot, program, token_mint, side, sol_amount, token_amount, trader
         FROM trades ORDER BY slot DESC LIMIT $1",
    )
    .bind(rows(&limit))
    .fetch_all(&db)
    .await
    .map_err(failed)?;

    Ok(Json(rows_out.iter().map(trade_from).collect()))
}

async fn by_token(
    State(db): State<PgPool>,
    Path(mint): Path<String>,
    Query(limit): Query<Limit>,
) -> Result<Json<Vec<Trade>>, StatusCode> {
    let rows_out = sqlx::query(
        "SELECT signature, slot, program, token_mint, side, sol_amount, token_amount, trader
         FROM trades WHERE token_mint = $1 ORDER BY slot DESC LIMIT $2",
    )
    .bind(&mint)
    .bind(rows(&limit))
    .fetch_all(&db)
    .await
    .map_err(failed)?;

    Ok(Json(rows_out.iter().map(trade_from).collect()))
}

async fn volume(
    State(db): State<PgPool>,
    Path(mint): Path<String>,
) -> Result<Json<Volume>, StatusCode> {
    let row = sqlx::query(
        "SELECT count(*) AS trades, coalesce(sum(sol_amount), 0) AS sol_volume
         FROM trades WHERE token_mint = $1",
    )
    .bind(&mint)
    .fetch_one(&db)
    .await
    .map_err(failed)?;

    Ok(Json(Volume {
        token_mint: mint,
        trades: row.get("trades"),
        sol_volume: row.get::<i64, _>("sol_volume").to_string(),
    }))
}

pub async fn run_api(db: PgPool, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/trades/recent", get(recent))
        .route("/trades/token/{mint}", get(by_token))
        .route("/volume/token/{mint}", get(volume))
        .with_state(db);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;

    println!("api listening on http://localhost:{port}");

    axum::serve(listener, app).await?;

    Ok(())
}
