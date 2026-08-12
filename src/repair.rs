use std::collections::HashMap;
use std::str::FromStr;

use serde_json::Value;
use solana_pubkey::Pubkey;
use sqlx::PgPool;

use crate::db::{self, RepairedTrade, TradeRow, UnrepairedEvent};
use crate::pools::PoolInfo;

enum Skipped {
    PoolUnknown,
    NoSolSide,
    AmountTooLarge,
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

    let sol_amount = i64::try_from(t.sol_amount).map_err(|_| Skipped::AmountTooLarge)?;
    let token_amount = i64::try_from(t.token_amount).map_err(|_| Skipped::AmountTooLarge)?;

    Ok(TradeRow {
        pool: Some(pool.to_string()),
        token_mint: t.token_mint.to_string(),
        side: if t.is_buy { "buy" } else { "sell" },
        sol_amount,
        token_amount,
        trader: user.to_string(),
        fee: p
            .get("protocol_fee")
            .and_then(Value::as_u64)
            .and_then(|f| i64::try_from(f).ok()),
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
    let (mut unknown, mut no_sol, mut too_large, mut bad) = (0usize, 0usize, 0usize, 0usize);

    for e in &events {
        match build(e, &pools) {
            Ok(trade) => {
                ready.push(RepairedTrade {
                    signature: e.signature.clone(),
                    absolute_path: e.absolute_path.clone(),
                    event_ordinal: e.event_ordinal,
                    slot: e.slot,
                    block_time: e.block_time,
                    trade,
                });
                if ready.len() >= 100 {
                    written += db::write_repaired(&db, &ready).await?;
                    ready.clear();
                }
            }
            Err(Skipped::PoolUnknown) => unknown += 1,
            Err(Skipped::NoSolSide) => no_sol += 1,
            Err(Skipped::AmountTooLarge) => too_large += 1,
            Err(Skipped::BadPayload) => bad += 1,
        }
    }

    written += db::write_repaired(&db, &ready).await?;

    println!("repair: {written} trades written");
    println!("repair: {unknown} skipped, pool still not in the pools table");
    println!("repair: {no_sol} skipped, neither side of the pool is wrapped SOL");
    println!("repair: {too_large} skipped, amount does not fit in i64");
    println!("repair: {bad} skipped, payload could not be read");

    Ok(())
}
