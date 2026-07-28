use std::collections::HashMap;

use solana_pubkey::Pubkey;

use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use carbon_core::instruction::InstructionProcessorInputType;

use carbon_pump_swap_decoder::{
    accounts::pool::Pool,
    instructions::{CpiEvent as PumpSwapCpiEvent, PumpSwapInstruction},
};

use crate::identity::{EventId, EventLog};

pub struct PoolInfo {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_decimals: u8,
    quote_decimals: u8,
}

pub struct PumpSwapEventProcessor {
    pub pools: HashMap<Pubkey, PoolInfo>,
    pub rpc: Option<RpcClient>,
    pub events: EventLog,
}

impl PumpSwapEventProcessor {
    async fn ensure_pool(&mut self, pool: Pubkey){
        
        if self.pools.contains_key(&pool){
            return;
        }

        let Some(rpc) = &self.rpc else {
            return
        };

        match rpc.get_account_data(&pool).await {
            Ok(data) => {
                if let Some(p) =  Pool::decode(&data) {
                    self.pools.insert(
                        pool, 
                        PoolInfo { 
                            base_mint: p.base_mint, 
                            quote_mint: p.quote_mint, 
                            base_decimals: 0, 
                            quote_decimals: 0 
                        },
                    );
                } else {
                    println!(
                        "Decode failed for {} | len={} | first8={:?}",
                        pool,
                        data.len(),
                        &data[..data.len().min(8)],
                    );
                }
            }
            Err(e) => println!("RPC fetch failed for {}: {}", pool, e),
        }
    }
}

impl carbon_core::processor::Processor<InstructionProcessorInputType<'_, PumpSwapInstruction>> for PumpSwapEventProcessor {
    async fn process(
        &mut self,
        data: &InstructionProcessorInputType<'_, PumpSwapInstruction>,
    ) -> carbon_core::error::CarbonResult<()> {
        if data.metadata.transaction_metadata.meta.status.is_err(){
            return Ok(());
        };

        match data.decoded_instruction {
            PumpSwapInstruction::CpiEvent { data: cpi_data, .. } => match cpi_data {
                PumpSwapCpiEvent::BuyEvent(trade) => {
                    let meta = &data.metadata;
                    println!("Trade event found!");
                    println!("--- event ---");
                    println!("signature: {}", meta.transaction_metadata.signature);
                    println!("slot: {}", meta.transaction_metadata.slot);
                    println!("absolute_path: {:?}", meta.absolute_path);
                    println!("event_ordinal: 0");
                    {
                        let mut log = self.events.lock().unwrap();
                        log.push(EventId {
                            signature: meta.transaction_metadata.signature.to_string(),
                            absolute_path: meta.absolute_path.clone(),
                            event_ordinal: 0,
                        });
                    }
                    println!("Pool: {}", trade.pool);
                    println!("User: {}", trade.user);
                    println!("Token received: {}", trade.base_amount_out);
                    println!("SOL amount: {}", trade.quote_amount_in);
                    self.ensure_pool(trade.pool).await;
                    match self.pools.get(&trade.pool) {
                        Some(pool_info) => println!("Base mint: {}", pool_info.base_mint),
                        None => println!("Base mint: UNKNOWN"),
                    }
                }

                PumpSwapCpiEvent::SellEvent(trade) => {
                    let meta = &data.metadata;
                    println!("Trade event found!");
                    println!("--- event ---");
                    println!("signature: {}", meta.transaction_metadata.signature);
                    println!("slot: {}", meta.transaction_metadata.slot);
                    println!("absolute_path: {:?}", meta.absolute_path);
                    println!("event_ordinal: 0");
                    {
                        let mut log = self.events.lock().unwrap();
                        log.push(EventId {
                            signature: meta.transaction_metadata.signature.to_string(),
                            absolute_path: meta.absolute_path.clone(),
                            event_ordinal: 0,
                        });
                    }
                    println!("Pool: {}", trade.pool);
                    println!("User: {}", trade.user);
                    println!("Token sold: {}", trade.base_amount_in);
                    println!("SOL amount: {}", trade.quote_amount_out);
                    self.ensure_pool(trade.pool).await;
                    match self.pools.get(&trade.pool) {
                        Some(pool_info) => println!("Base mint: {}", pool_info.base_mint),
                        None => println!("Base mint: UNKNOWN"),
                    }
                }

                PumpSwapCpiEvent::CreatePoolEvent(pool_event) => {
                    println!("Pool created!");
                    println!("Pool: {}", pool_event.pool);
                    println!("Base mint: {}", pool_event.base_mint);
                    println!("Quote mint: {}", pool_event.quote_mint);
                    println!("Base decimals: {}", pool_event.base_mint_decimals);
                    println!("Quote decimals: {}", pool_event.quote_mint_decimals);
                    self.pools.insert(
                        pool_event.pool, 
                        PoolInfo {
                            base_mint: pool_event.base_mint,
                            quote_mint: pool_event.quote_mint,
                            base_decimals: pool_event.base_mint_decimals,
                            quote_decimals: pool_event.quote_mint_decimals,
                        },
                    );  
                }

                _ => {}
            },
            _ => {}
        }
        Ok(())
    }
}
