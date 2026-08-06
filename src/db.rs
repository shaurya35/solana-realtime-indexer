use crate::metrics::{DB_WRITE_ERRORS, inc};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(url)
        .await
}

pub struct EventRow {
    pub signature: String,
    pub absolute_path: Vec<u8>,
    pub event_ordinal: i32,
    pub slot: i64,
    pub block_time: Option<i64>,
    pub program: &'static str,   
    pub event_type: &'static str, 
    pub payload: Value,
}

pub struct TradeRow {
    pub pool: Option<String>,
    pub token_mint: String,
    pub side: &'static str, 
    pub sol_amount: i64,
    pub token_amount: i64,
    pub trader: String,
    pub fee: Option<i64>,
}

pub async fn write_event(pool: &PgPool, e: &EventRow) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO events
           (signature, absolute_path, event_ordinal, slot, block_time, program, event_type, payload)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT DO NOTHING",
    )
    .bind(&e.signature)
    .bind(&e.absolute_path)
    .bind(e.event_ordinal)
    .bind(e.slot)
    .bind(e.block_time)
    .bind(e.program)
    .bind(e.event_type)
    .bind(&e.payload)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn write_trade(
    pool: &PgPool,
    e: &EventRow,
    t: &TradeRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO trades
           (signature, absolute_path, event_ordinal, slot, block_time, program,
            pool, token_mint, side, sol_amount, token_amount, trader, fee)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         ON CONFLICT DO NOTHING",
    )
    .bind(&e.signature)
    .bind(&e.absolute_path)
    .bind(e.event_ordinal)
    .bind(e.slot)
    .bind(e.block_time)
    .bind(e.program)
    .bind(&t.pool)
    .bind(&t.token_mint)
    .bind(t.side)
    .bind(t.sol_amount)
    .bind(t.token_amount)
    .bind(&t.trader)
    .bind(t.fee)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn write(pool: &PgPool, e: &EventRow, t: Option<&TradeRow>) {
    if let Err(err) = write_event(pool, e).await {
        inc(&DB_WRITE_ERRORS);
        eprintln!("db: event insert failed sig={} err={err}", e.signature);
        return;
    }

    if let Some(t) = t {
        if let Err(err) = write_trade(pool, e, t).await {
            inc(&DB_WRITE_ERRORS);
            eprintln!("db: trade insert failed sig={} err={err}", e.signature);
        }
    }
}

pub async fn write_pool(
    db: &PgPool,
    pool: &str,
    base_mint: &str,
    quote_mint: &str,
    base_decimals: Option<i32>,
    quote_decimals: Option<i32>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO pools (pool, base_mint, quote_mint, base_decimals, quote_decimals)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (pool) DO UPDATE SET
           base_decimals  = COALESCE(pools.base_decimals, EXCLUDED.base_decimals),
           quote_decimals = COALESCE(pools.quote_decimals, EXCLUDED.quote_decimals)",
    )
    .bind(pool)
    .bind(base_mint)
    .bind(quote_mint)
    .bind(base_decimals)
    .bind(quote_decimals)
    .execute(db)
    .await?;

    Ok(())
}

pub struct StoredPool {
    pub pool: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_decimals: Option<i32>,
    pub quote_decimals: Option<i32>,
}

pub async fn load_pools(db: &PgPool) -> Result<Vec<StoredPool>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT pool, base_mint, quote_mint, base_decimals, quote_decimals FROM pools",
    )
    .fetch_all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| StoredPool {
            pool: r.get("pool"),
            base_mint: r.get("base_mint"),
            quote_mint: r.get("quote_mint"),
            base_decimals: r.get("base_decimals"),
            quote_decimals: r.get("quote_decimals"),
        })
        .collect())
}