use std::collections::HashMap;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::Value;
use solana_pubkey::Pubkey;
use sqlx::PgPool;

use crate::db::{self, RepairedTrade, TradeRow, UnrepairedEvent};
use crate::pools::PoolInfo;

enum Skipped {
    PoolUnknown,
    NoSolSide,
    BadPayload,
}

fn pubkey_from_json(v: Option<&Value>) -> Option<Pubkey> {
    let arr = v?.as_array()?;
    if arr.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, b) in arr.iter().enumerate() {
        bytes[i] = u8::try_from(b.as_u64()?).ok()?;
    }
    Some(Pubkey::new_from_array(bytes))
}

fn build(e: &UnrepairedEvent, pools: &HashMap<String, PoolInfo>) -> Result<TradeRow, Skipped> {
    match e.program.as_str() {
        "pumpfun" => build_pumpfun(e),
        "pumpswap" => build_pumpswap(e, pools),
        _ => Err(Skipped::BadPayload),
    }
}

/// pump.fun is a bonding curve, so the mint and the direction are both in the
/// event itself. There is no pool to look up and nothing that can be unknown
/// later, which is why these only ever failed on the old i64 amount limit.
fn build_pumpfun(e: &UnrepairedEvent) -> Result<TradeRow, Skipped> {
    let p = &e.payload;

    let mint = pubkey_from_json(p.get("mint")).ok_or(Skipped::BadPayload)?;
    let user = pubkey_from_json(p.get("user")).ok_or(Skipped::BadPayload)?;

    let is_buy = p
        .get("is_buy")
        .and_then(Value::as_bool)
        .ok_or(Skipped::BadPayload)?;

    let sol_amount = p
        .get("sol_amount")
        .and_then(Value::as_u64)
        .ok_or(Skipped::BadPayload)?;

    let token_amount = p
        .get("token_amount")
        .and_then(Value::as_u64)
        .ok_or(Skipped::BadPayload)?;

    Ok(TradeRow {
        pool: None,
        token_mint: mint.to_string(),
        side: if is_buy { "buy" } else { "sell" },
        sol_amount: Decimal::from(sol_amount),
        token_amount: Decimal::from(token_amount),
        trader: user.to_string(),
        fee: p.get("fee").and_then(Value::as_u64).map(Decimal::from),
    })
}

fn build_pumpswap(
    e: &UnrepairedEvent,
    pools: &HashMap<String, PoolInfo>,
) -> Result<TradeRow, Skipped> {
    let p = &e.payload;

    let pool = pubkey_from_json(p.get("pool")).ok_or(Skipped::BadPayload)?;
    let user = pubkey_from_json(p.get("user")).ok_or(Skipped::BadPayload)?;

    let (base_amount, quote_amount, acquiring_base) = match e.event_type.as_str() {
        "BuyEvent" => (
            p.get("base_amount_out")
                .and_then(Value::as_u64)
                .ok_or(Skipped::BadPayload)?,
            p.get("quote_amount_in")
                .and_then(Value::as_u64)
                .ok_or(Skipped::BadPayload)?,
            true,
        ),
        "SellEvent" => (
            p.get("base_amount_in")
                .and_then(Value::as_u64)
                .ok_or(Skipped::BadPayload)?,
            p.get("quote_amount_out")
                .and_then(Value::as_u64)
                .ok_or(Skipped::BadPayload)?,
            false,
        ),
        _ => return Err(Skipped::BadPayload),
    };

    let info = pools.get(&pool.to_string()).ok_or(Skipped::PoolUnknown)?;

    let t = info
        .orient(base_amount, quote_amount, acquiring_base)
        .ok_or(Skipped::NoSolSide)?;

    Ok(TradeRow {
        pool: Some(pool.to_string()),
        token_mint: t.token_mint.to_string(),
        side: if t.is_buy { "buy" } else { "sell" },
        sol_amount: Decimal::from(t.sol_amount),
        token_amount: Decimal::from(t.token_amount),
        trader: user.to_string(),
        fee: p
            .get("protocol_fee")
            .and_then(Value::as_u64)
            .map(Decimal::from),
    })
}

pub async fn run_repair(db: PgPool, limit: i64) -> Result<(), Box<dyn std::error::Error>> {
    let mut pools: HashMap<String, PoolInfo> = HashMap::new();
    for p in db::load_pools(&db).await? {
        let (Ok(base), Ok(quote)) = (
            Pubkey::from_str(&p.base_mint),
            Pubkey::from_str(&p.quote_mint),
        ) else {
            continue;
        };
        pools.insert(
            p.pool,
            PoolInfo {
                base_mint: base,
                quote_mint: quote,
                base_decimals: p.base_decimals.and_then(|d| u8::try_from(d).ok()),
                quote_decimals: p.quote_decimals.and_then(|d| u8::try_from(d).ok()),
            },
        );
    }
    println!("repair: {} pools known", pools.len());

    let events = db::load_unrepaired(&db, limit).await?;
    println!("repair: {} events with no trade row", events.len());

    let mut ready: Vec<RepairedTrade> = Vec::new();
    let mut written = 0u64;
    let (mut unknown, mut no_sol, mut bad) = (0usize, 0usize, 0usize);

    for e in &events {
        match build(e, &pools) {
            Ok(trade) => {
                ready.push(RepairedTrade {
                    signature: e.signature.clone(),
                    absolute_path: e.absolute_path.clone(),
                    event_ordinal: e.event_ordinal,
                    slot: e.slot,
                    block_time: e.block_time,
                    program: e.program.clone(),
                    trade,
                });
                if ready.len() >= 100 {
                    written += db::write_repaired(&db, &ready).await?;
                    ready.clear();
                }
            }
            Err(Skipped::PoolUnknown) => unknown += 1,
            Err(Skipped::NoSolSide) => no_sol += 1,
            Err(Skipped::BadPayload) => bad += 1,
        }
    }

    written += db::write_repaired(&db, &ready).await?;

    println!("repair: {written} trades written");
    println!("repair: {unknown} skipped, pool still not in the pools table");
    println!("repair: {no_sol} skipped, neither side of the pool is wrapped SOL");
    println!("repair: {bad} skipped, payload could not be read");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sqlx::Row;

    const WSOL_STR: &str = "So11111111111111111111111111111111111111112";

    #[tokio::test]
    async fn repair_fills_a_trade_once_the_pool_is_known() {
        let Some(db) = crate::db::test_pool().await else {
            return;
        };

        let pool_key = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let token = Pubkey::new_unique();

        sqlx::query(
            "INSERT INTO events (signature, absolute_path, event_ordinal, slot,
                                 block_time, program, event_type, payload)
             VALUES ($1, $2, 0, 1, NULL, 'pumpswap', 'BuyEvent', $3)",
        )
        .bind("repairsig")
        .bind(vec![1u8, 2])
        .bind(json!({
            "pool": pool_key.to_bytes().to_vec(),
            "user": user.to_bytes().to_vec(),
            "base_amount_out": 1_000,
            "quote_amount_in": 50,
            "protocol_fee": 7,
        }))
        .execute(&db)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO pools (pool, base_mint, quote_mint, base_decimals, quote_decimals)
             VALUES ($1, $2, $3, NULL, NULL)",
        )
        .bind(pool_key.to_string())
        .bind(token.to_string())
        .bind(WSOL_STR)
        .execute(&db)
        .await
        .unwrap();

        run_repair(db.clone(), 100).await.unwrap();

        let trades: i64 = sqlx::query_scalar("SELECT count(*) FROM trades")
            .fetch_one(&db)
            .await
            .unwrap();

        assert_eq!(trades, 1, "repair did not rebuild the trade");

        let row = sqlx::query("SELECT side, sol_amount, token_amount, token_mint FROM trades")
            .fetch_one(&db)
            .await
            .unwrap();

        assert_eq!(row.get::<String, _>("side"), "buy");
        assert_eq!(row.get::<Decimal, _>("sol_amount"), Decimal::from(50));
        assert_eq!(row.get::<Decimal, _>("token_amount"), Decimal::from(1_000));
        assert_eq!(row.get::<String, _>("token_mint"), token.to_string());

        run_repair(db.clone(), 100).await.unwrap();

        let after: i64 = sqlx::query_scalar("SELECT count(*) FROM trades")
            .fetch_one(&db)
            .await
            .unwrap();

        assert_eq!(after, 1, "a second run wrote a duplicate");
    }
}
