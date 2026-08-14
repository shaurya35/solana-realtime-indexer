use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use sqlx::PgPool;

use crate::config::{GAP_OVERLAP_SLOTS, SOLANA_RPC_URL};
use crate::datasources::backfill::BackfillDatasource;
use crate::db;
use crate::pipeline::run_pipeline;

pub async fn run_recover(db_pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let gaps = db::open_gaps(&db_pool).await?;

    println!("recover: {} open gaps", gaps.len());

    let mut covered: Vec<(u64, u64)> = Vec::new();

    for gap in gaps {
        let from = (gap.start_slot as u64).saturating_sub(GAP_OVERLAP_SLOTS);
        let to = gap.end_slot as u64 + GAP_OVERLAP_SLOTS;

        if covered.iter().any(|(a, b)| from >= *a && to <= *b) {
            println!("recover: gap {} already covered this run", gap.gap_id);
            db::mark_gap(&db_pool, gap.gap_id, "closed").await?;
            continue;
        }

        println!("recover: gap {} covering slots {from} to {to}", gap.gap_id);

        db::mark_gap(&db_pool, gap.gap_id, "recovering").await?;

        run_pipeline(
            BackfillDatasource {
                rpc_url: SOLANA_RPC_URL.to_string(),
                start_slot: from,
                end_slot: to,
            },
            Some(RpcClient::new(SOLANA_RPC_URL.to_string())),
            None,
            Some(db_pool.clone()),
            None,
        )
        .await?;

        covered.push((from, to));

        db::mark_gap(&db_pool, gap.gap_id, "closed").await?;
        println!("recover: gap {} closed", gap.gap_id);
    }

    Ok(())
}
