use std::collections::HashMap;

use carbon_core::datasource::Datasource;
use carbon_core::pipeline::Pipeline;
use carbon_pump_swap_decoder::PumpSwapDecoder;
use carbon_pumpfun_decoder::PumpfunDecoder;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use crate::identity::EventLog;
use crate::processors::pumpfun::TradeEventProcessor;
use crate::processors::pumpswap::PumpSwapEventProcessor;

pub async fn run_pipeline(
    datasource: impl Datasource + 'static,
    rpc: Option<RpcClient>,
    events: EventLog,
) -> Result<(), Box<dyn std::error::Error>> {
    Pipeline::builder()
        .datasource(datasource)
        .instruction(
            PumpfunDecoder,
            TradeEventProcessor {
                events: events.clone(),
            },
        )
        .instruction(
            PumpSwapDecoder,
            PumpSwapEventProcessor {
                pools: HashMap::new(),
                rpc,
                events,
            },
        )
        .build()?
        .run()
        .await?;

    Ok(())
}
