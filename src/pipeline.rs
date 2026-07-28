use std::collections::HashSet;

use carbon_core::datasource::Datasource;
use carbon_core::pipeline::Pipeline;
use carbon_pump_swap_decoder::PumpSwapDecoder;
use carbon_pumpfun_decoder::PumpfunDecoder;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use crate::config::{PIPELINE_QUEUE_SIZE, POOL_LOOKUP_QUEUE_SIZE};
use crate::identity::EventLog;
use crate::pools::spawn_pool_resolver;
use crate::processors::pumpfun::TradeEventProcessor;
use crate::processors::pumpswap::PumpSwapEventProcessor;

pub async fn run_pipeline(
    datasource: impl Datasource + 'static,
    rpc: Option<RpcClient>,
    events: EventLog,
) -> Result<(), Box<dyn std::error::Error>> {
    let resolver = rpc.map(|client| spawn_pool_resolver(client, POOL_LOOKUP_QUEUE_SIZE));

    Pipeline::builder()
        .datasource(datasource)
        .channel_buffer_size(PIPELINE_QUEUE_SIZE)
        .instruction(
            PumpfunDecoder,
            TradeEventProcessor {
                events: events.clone(),
            },
        )
        .instruction(
            PumpSwapDecoder,
            PumpSwapEventProcessor {
                resolver,
                requested: HashSet::new(),
                events,
            },
        )
        .build()?
        .run()
        .await?;

    Ok(())
}