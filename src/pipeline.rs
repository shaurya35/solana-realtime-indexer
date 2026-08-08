use sqlx::PgPool;

use std::collections::HashSet;

use carbon_core::datasource::Datasource;
use carbon_core::pipeline::Pipeline;
use carbon_pump_swap_decoder::PumpSwapDecoder;
use carbon_pumpfun_decoder::PumpfunDecoder;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use carbon_core::datasource::DatasourceDisconnection;
use tokio::sync::mpsc::Sender;

use crate::config::{PIPELINE_QUEUE_SIZE, POOL_LOOKUP_QUEUE_SIZE, WRITE_QUEUE_SIZE};
use crate::identity::EventLog;
use crate::pools::spawn_pool_resolver;
use crate::processors::pumpfun::TradeEventProcessor;
use crate::processors::pumpswap::PumpSwapEventProcessor;
use crate::writer::spawn_writer;

pub async fn run_pipeline(
    datasource: impl Datasource + 'static,
    rpc: Option<RpcClient>,
    events: EventLog,
    db: Option<PgPool>,
    gaps: Option<Sender<DatasourceDisconnection>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let resolver =
        rpc.map(|client| spawn_pool_resolver(client, POOL_LOOKUP_QUEUE_SIZE, db.clone()));

    if let Some(r) = &resolver {
        println!("pool cache primed with {} pools", r.prime().await?);
    }

    let watermark = Arc::new(AtomicU64::new(0));

    let (writer, writer_task) = match db {
        Some(d) => {
            let (w, h) = spawn_writer(d, WRITE_QUEUE_SIZE);
            (Some(w), Some(h))
        }
        None => (None, None),
    };

    let mut pipeline = Pipeline::builder()
        .datasource(datasource)
        .channel_buffer_size(PIPELINE_QUEUE_SIZE)
        .instruction(
            PumpfunDecoder,
            TradeEventProcessor {
                events: events.clone(),
                writer: writer.clone(),
                watermark: watermark.clone(),
                gaps: gaps.clone(),
            },
        )
        .instruction(
            PumpSwapDecoder,
            PumpSwapEventProcessor {
                resolver,
                requested: HashSet::new(),
                events,
                writer,
                watermark,
                gaps,
            },
        )
        .build()?;

    pipeline.run().await?;
    println!("shutdown: pipeline stopped!");

    drop(pipeline);
    println!("shutdown: pipeline dropped!");

    if let Some(task) = writer_task {
        let _ = task.await;
    }
    println!("shutdown: writer stopped!");

    Ok(())
}
