use serde_json::Value;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new().max_connections(5).connect(url).await
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

pub struct PendingWrite {
    pub event: EventRow,
    pub trade: Option<TradeRow>,
}

pub async fn write_events(tx: &mut sqlx::PgConnection, rows: &[PendingWrite]) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut q: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO events
           (signature, absolute_path, event_ordinal, slot, block_time, program, event_type, payload) ",
    );

    q.push_values(rows, |mut b, r| {
        b.push_bind(&r.event.signature)
            .push_bind(&r.event.absolute_path)
            .push_bind(r.event.event_ordinal)
            .push_bind(r.event.slot)
            .push_bind(r.event.block_time)
            .push_bind(r.event.program)
            .push_bind(r.event.event_type)
            .push_bind(&r.event.payload);
    });

    q.push(" ON CONFLICT DO NOTHING");

    q.build().execute(tx).await?;

    Ok(())
}

pub async fn write_trades(tx: &mut sqlx::PgConnection, rows: &[PendingWrite]) -> Result<(), sqlx::Error> {
    let pairs: Vec<(&EventRow, &TradeRow)> = rows
        .iter()
        .filter_map(|r| r.trade.as_ref().map(|t| (&r.event, t)))
        .collect();

    if pairs.is_empty() {
        return Ok(());
    }

    let mut q: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO trades
           (signature, absolute_path, event_ordinal, slot, block_time, program,
            pool, token_mint, side, sol_amount, token_amount, trader, fee) ",
    );

    q.push_values(pairs, |mut b, (e, t)| {
        b.push_bind(&e.signature)
            .push_bind(&e.absolute_path)
            .push_bind(e.event_ordinal)
            .push_bind(e.slot)
            .push_bind(e.block_time)
            .push_bind(e.program)
            .push_bind(&t.pool)
            .push_bind(&t.token_mint)
            .push_bind(t.side)
            .push_bind(t.sol_amount)
            .push_bind(t.token_amount)
            .push_bind(&t.trader)
            .push_bind(t.fee);
    });

    q.push(" ON CONFLICT DO NOTHING");

    q.build().execute(tx).await?;

    Ok(())
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
    let rows =
        sqlx::query("SELECT pool, base_mint, quote_mint, base_decimals, quote_decimals FROM pools")
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

pub async fn write_checkpoint(
    tx: &mut sqlx::PgConnection,
    slot: i64,
    signature: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO ingestion_checkpoints (id, last_completed_slot, last_completed_signature, updated_at)
         VALUES (1, $1, $2, now())
         ON CONFLICT (id) DO UPDATE SET
           last_completed_slot      = EXCLUDED.last_completed_slot,
           last_completed_signature = EXCLUDED.last_completed_signature,
           updated_at               = now()",
    )
    .bind(slot)
    .bind(signature)
    .execute(tx)
    .await?;

    Ok(())
}