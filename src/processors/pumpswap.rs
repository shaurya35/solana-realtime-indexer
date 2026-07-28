use std::collections::HashSet;

use carbon_core::instruction::InstructionProcessorInputType;
use carbon_pump_swap_decoder::instructions::{CpiEvent as PumpSwapCpiEvent, PumpSwapInstruction};
use solana_pubkey::Pubkey;

use crate::identity::{EventId, EventLog};
use crate::metrics::{inc, EVENTS_DECODED, POOL_CACHE_HITS, POOL_CACHE_MISSES, SKIPPED_FAILED};
use crate::pools::{PoolInfo, PoolResolver};

pub struct PumpSwapEventProcessor {
    pub resolver: Option<PoolResolver>,
    pub requested: HashSet<Pubkey>,
    pub events: EventLog,
}

impl PumpSwapEventProcessor {
    fn resolve(&mut self, pool: Pubkey) -> Option<PoolInfo> {
        let resolver = self.resolver.as_ref()?;

        if let Some(info) = resolver.get(&pool) {
            inc(&POOL_CACHE_HITS);
            return Some(info);
        }

        inc(&POOL_CACHE_MISSES);

        if self.requested.insert(pool) {
            resolver.request(pool);
        }

        None
    }

    fn record(&self, meta: &carbon_core::instruction::InstructionMetadata) {
        let mut log = self.events.lock().unwrap();
        log.push(EventId {
            signature: meta.transaction_metadata.signature.to_string(),
            absolute_path: meta.absolute_path.clone(),
            event_ordinal: 0,
        });
        inc(&EVENTS_DECODED);
    }
}

impl carbon_core::processor::Processor<InstructionProcessorInputType<'_, PumpSwapInstruction>>
    for PumpSwapEventProcessor
{
    async fn process(
        &mut self,
        data: &InstructionProcessorInputType<'_, PumpSwapInstruction>,
    ) -> carbon_core::error::CarbonResult<()> {
        if data.metadata.transaction_metadata.meta.status.is_err() {
            inc(&SKIPPED_FAILED);
            return Ok(());
        }

        let PumpSwapInstruction::CpiEvent { data: cpi_data, .. } = data.decoded_instruction else {
            return Ok(());
        };

        let meta = &data.metadata;

        match cpi_data {
            PumpSwapCpiEvent::BuyEvent(trade) => {
                self.record(meta);
                let oriented = self
                    .resolve(trade.pool)
                    .and_then(|info| info.orient(trade.base_amount_out, trade.quote_amount_in, true));

                match oriented {
                    Some(t) => println!(
                        "pumpswap {} sig={} slot={} path={:?} ord=0 pool={} user={} mint={} sol={} token={}",
                        if t.is_buy { "buy " } else { "sell" },
                        meta.transaction_metadata.signature,
                        meta.transaction_metadata.slot,
                        meta.absolute_path,
                        trade.pool,
                        trade.user,
                        t.token_mint,
                        t.sol_amount,
                        t.token_amount,
                    ),
                    None => println!(
                        "pumpswap ???? sig={} slot={} path={:?} ord=0 pool={} user={} base={} quote={} (unresolved)",
                        meta.transaction_metadata.signature,
                        meta.transaction_metadata.slot,
                        meta.absolute_path,
                        trade.pool,
                        trade.user,
                        trade.base_amount_out,
                        trade.quote_amount_in,
                    ),
                }  
            }   

            PumpSwapCpiEvent::SellEvent(trade) => {
                self.record(meta);
                let oriented = self
                    .resolve(trade.pool)
                    .and_then(|info| info.orient(trade.base_amount_in, trade.quote_amount_out, false));

                match oriented {
                    Some(t) => println!(
                        "pumpswap {} sig={} slot={} path={:?} ord=0 pool={} user={} mint={} sol={} token={}",
                        if t.is_buy { "buy " } else { "sell" },
                        meta.transaction_metadata.signature,
                        meta.transaction_metadata.slot,
                        meta.absolute_path,
                        trade.pool,
                        trade.user,
                        t.token_mint,
                        t.sol_amount,
                        t.token_amount,
                    ),
                    None => println!(
                        "pumpswap ???? sig={} slot={} path={:?} ord=0 pool={} user={} base={} quote={} (unresolved)",
                        meta.transaction_metadata.signature,
                        meta.transaction_metadata.slot,
                        meta.absolute_path,
                        trade.pool,
                        trade.user,
                        trade.base_amount_in,
                        trade.quote_amount_out,
                    ),
                }
            }

            PumpSwapCpiEvent::CreatePoolEvent(ev) => {
                if let Some(resolver) = &self.resolver {
                    resolver.insert(
                        ev.pool,
                        PoolInfo {
                            base_mint: ev.base_mint,
                            quote_mint: ev.quote_mint,
                            base_decimals: ev.base_mint_decimals,
                            quote_decimals: ev.quote_mint_decimals,
                        },
                    );
                }
                println!(
                    "pool created {} base={} quote={}",
                    ev.pool, ev.base_mint, ev.quote_mint
                );
            }

            _ => {}
        }

        Ok(())
    }
}