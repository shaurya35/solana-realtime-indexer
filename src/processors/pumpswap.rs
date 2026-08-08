use std::collections::HashSet;

use carbon_core::instruction::InstructionProcessorInputType;
use carbon_pump_swap_decoder::instructions::{CpiEvent as PumpSwapCpiEvent, PumpSwapInstruction};
use solana_pubkey::Pubkey;

use serde_json::Value;

use crate::db::{EventRow, PendingWrite, TradeRow};
use crate::identity::{EventId, EventLog};
use crate::metrics::{EVENTS_DECODED, POOL_CACHE_HITS, POOL_CACHE_MISSES, SKIPPED_FAILED, inc};
use crate::pools::{PoolInfo, PoolResolver};
use crate::writer::Writer;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use carbon_core::datasource::DatasourceDisconnection;
use tokio::sync::mpsc::Sender;

use crate::gaps;

pub struct PumpSwapEventProcessor {
    pub resolver: Option<PoolResolver>,
    pub requested: HashSet<Pubkey>,
    pub events: EventLog,
    pub writer: Option<Writer>,
    pub watermark: Arc<AtomicU64>,
    pub gaps: Option<Sender<DatasourceDisconnection>>,
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

        gaps::check_slot(
            &self.watermark,
            self.gaps.as_ref(),
            data.metadata.transaction_metadata.slot,
        );

        let PumpSwapInstruction::CpiEvent { data: cpi_data, .. } = data.decoded_instruction else {
            return Ok(());
        };

        let meta = &data.metadata;

        match cpi_data {
            PumpSwapCpiEvent::BuyEvent(trade) => {
                self.record(meta);
                let oriented = self.resolve(trade.pool).and_then(|info| {
                    info.orient(trade.base_amount_out, trade.quote_amount_in, true)
                });

                if let Some(writer) = &self.writer {
                    let sig = meta.transaction_metadata.signature.to_string();

                    let event = EventRow {
                        signature: sig,
                        absolute_path: meta.absolute_path.clone(),
                        event_ordinal: 0,
                        slot: meta.transaction_metadata.slot as i64,
                        block_time: meta.transaction_metadata.block_time,
                        program: "pumpswap",
                        event_type: "BuyEvent",
                        payload: serde_json::to_value(trade).unwrap_or(Value::Null),
                    };

                    let row = oriented.as_ref().and_then(|t| {
                        Some(TradeRow {
                            pool: Some(trade.pool.to_string()),
                            token_mint: t.token_mint.to_string(),
                            side: if t.is_buy { "buy" } else { "sell" },
                            sol_amount: i64::try_from(t.sol_amount).ok()?,
                            token_amount: i64::try_from(t.token_amount).ok()?,
                            trader: trade.user.to_string(),
                            fee: i64::try_from(trade.protocol_fee).ok(),
                        })
                    });

                    writer.send(PendingWrite { event, trade: row }).await;
                }

                match oriented {
                    Some(t) => println!(
                        "pumpswap {} sig={} slot={} path={:?} ord=0 pool={} user={} mint={} sol={} token={} inverted={}",
                        if t.is_buy { "buy " } else { "sell" },
                        meta.transaction_metadata.signature,
                        meta.transaction_metadata.slot,
                        meta.absolute_path,
                        trade.pool,
                        trade.user,
                        t.token_mint,
                        t.sol_amount,
                        t.token_amount,
                        t.sol_is_base,
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
                let oriented = self.resolve(trade.pool).and_then(|info| {
                    info.orient(trade.base_amount_in, trade.quote_amount_out, false)
                });

                if let Some(writer) = &self.writer {
                    let sig = meta.transaction_metadata.signature.to_string();

                    let event = EventRow {
                        signature: sig,
                        absolute_path: meta.absolute_path.clone(),
                        event_ordinal: 0,
                        slot: meta.transaction_metadata.slot as i64,
                        block_time: meta.transaction_metadata.block_time,
                        program: "pumpswap",
                        event_type: "SellEvent",
                        payload: serde_json::to_value(trade).unwrap_or(Value::Null),
                    };

                    let row = oriented.as_ref().and_then(|t| {
                        Some(TradeRow {
                            pool: Some(trade.pool.to_string()),
                            token_mint: t.token_mint.to_string(),
                            side: if t.is_buy { "buy" } else { "sell" },
                            sol_amount: i64::try_from(t.sol_amount).ok()?,
                            token_amount: i64::try_from(t.token_amount).ok()?,
                            trader: trade.user.to_string(),
                            fee: i64::try_from(trade.protocol_fee).ok(),
                        })
                    });

                    writer.send(PendingWrite { event, trade: row }).await;
                }

                match oriented {
                    Some(t) => println!(
                        "pumpswap {} sig={} slot={} path={:?} ord=0 pool={} user={} mint={} sol={} token={} inverted={}",
                        if t.is_buy { "buy " } else { "sell" },
                        meta.transaction_metadata.signature,
                        meta.transaction_metadata.slot,
                        meta.absolute_path,
                        trade.pool,
                        trade.user,
                        t.token_mint,
                        t.sol_amount,
                        t.token_amount,
                        t.sol_is_base,
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
                    resolver
                        .insert(
                            ev.pool,
                            PoolInfo {
                                base_mint: ev.base_mint,
                                quote_mint: ev.quote_mint,
                                base_decimals: Some(ev.base_mint_decimals),
                                quote_decimals: Some(ev.quote_mint_decimals),
                            },
                        )
                        .await;
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
