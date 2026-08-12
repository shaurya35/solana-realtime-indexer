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

pub async fn write_events(
    tx: &mut sqlx::PgConnection,
    rows: &[PendingWrite],
) -> Result<(), sqlx::Error> {
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

pub async fn write_trades(
    tx: &mut sqlx::PgConnection,
    rows: &[PendingWrite],
) -> Result<(), sqlx::Error> {
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

pub async fn write_dead_letters(
    db: &PgPool,
    rows: &[PendingWrite],
    error: &str,
) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut q: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO dead_letters
                (signature, absolute_path, event_ordinal, slot, payload, error) ",
    );

    q.push_values(rows, |mut b, r| {
        b.push_bind(&r.event.signature)
            .push_bind(&r.event.absolute_path)
            .push_bind(r.event.event_ordinal)
            .push_bind(r.event.slot)
            .push_bind(&r.event.payload)
            .push_bind(error);
    });

    q.build().execute(db).await?;

    Ok(())
}

pub async fn write_gap(
    db: &PgPool,
    start_slot: i64,
    end_slot: i64,
    missed_slots: i64,
    detected_at: chrono::DateTime<chrono::Utc>,
    detected_by: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO stream_gaps (start_slot, end_slot, missed_slots, detected_at, detected_by)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(start_slot)
    .bind(end_slot)
    .bind(missed_slots)
    .bind(detected_at)
    .bind(detected_by)
    .execute(db)
    .await?;

    Ok(())
}

pub struct OpenGap {
    pub gap_id: i64,
    pub start_slot: i64,
    pub end_slot: i64,
}

pub async fn open_gaps(db: &PgPool) -> Result<Vec<OpenGap>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT gap_id, start_slot, end_slot FROM stream_gaps
         WHERE status = 'open' ORDER BY gap_id",
    )
    .fetch_all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| OpenGap {
            gap_id: r.get("gap_id"),
            start_slot: r.get("start_slot"),
            end_slot: r.get("end_slot"),
        })
        .collect())
}

pub async fn mark_gap(db: &PgPool, gap_id: i64, status: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE stream_gaps
         SET status = $2,
             recovered_at = CASE WHEN $2 = 'closed' THEN now() ELSE recovered_at END,
             recovery_method = CASE WHEN $2 = 'closed' THEN 'rpc-block-fetch' ELSE recovery_method END
         WHERE gap_id = $1",
    )
    .bind(gap_id)
    .bind(status)
    .execute(db)
    .await?;

    Ok(())
}

pub struct UnrepairedEvent {
    pub signature: String,
    pub absolute_path: Vec<u8>,
    pub event_ordinal: i32,
    pub slot: i64,
    pub block_time: Option<i64>,
    pub event_type: String,
    pub payload: Value,
}

pub struct RepairedTrade {
    pub signature: String,
    pub absolute_path: Vec<u8>,
    pub event_ordinal: i32,
    pub slot: i64,
    pub block_time: Option<i64>,
    pub trade: TradeRow,
}

pub async fn load_unrepaired(db: &PgPool, limit: i64) -> Result<Vec<UnrepairedEvent>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT e.signature, e.absolute_path, e.event_ordinal, e.slot, e.block_time,
                e.event_type, e.payload
           FROM events e
           LEFT JOIN trades t
             ON  t.signature     = e.signature
             AND t.absolute_path = e.absolute_path
             AND t.event_ordinal = e.event_ordinal
          WHERE t.signature IS NULL
            AND e.program = 'pumpswap'
          ORDER BY e.slot
          LIMIT $1",
    )
    .bind(limit)
    .fetch_all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| UnrepairedEvent {
            signature: r.get("signature"),
            absolute_path: r.get("absolute_path"),
            event_ordinal: r.get("event_ordinal"),
            slot: r.get("slot"),
            block_time: r.get("block_time"),
            event_type: r.get("event_type"),
            payload: r.get("payload"),
        })
        .collect())
}

pub async fn write_repaired(db: &PgPool, rows: &[RepairedTrade]) -> Result<u64, sqlx::Error> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut q: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO trades
           (signature, absolute_path, event_ordinal, slot, block_time, program,
            pool, token_mint, side, sol_amount, token_amount, trader, fee) ",
    );

    q.push_values(rows, |mut b, r| {
        b.push_bind(&r.signature)
            .push_bind(&r.absolute_path)
            .push_bind(r.event_ordinal)
            .push_bind(r.slot)
            .push_bind(r.block_time)
            .push_bind("pumpswap")
            .push_bind(&r.trade.pool)
            .push_bind(&r.trade.token_mint)
            .push_bind(r.trade.side)
            .push_bind(r.trade.sol_amount)
            .push_bind(r.trade.token_amount)
            .push_bind(&r.trade.trader)
            .push_bind(r.trade.fee);
    });

    q.push(" ON CONFLICT DO NOTHING");

    Ok(q.build().execute(db).await?.rows_affected())
}
