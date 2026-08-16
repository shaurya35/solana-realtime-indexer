use std::collections::HashMap;

use carbon_core::instruction::InstructionProcessorInputType;
use carbon_pump_swap_decoder::instructions::{CpiEvent as PumpSwapCpiEvent, PumpSwapInstruction};
use solana_pubkey::Pubkey;

use rust_decimal::Decimal;

use serde_json::Value;

use crate::config::POOL_RETRY_AFTER;
use crate::db::{EventRow, PendingWrite, TradeRow};
use crate::identity::{EventId, EventLog};
use crate::metrics::{
    DECODE_TIME, EVENTS_DECODED, POOL_CACHE_HITS, POOL_CACHE_MISSES, SKIPPED_FAILED,
    TRADES_WRITTEN, UNORIENTED, inc,
};
use crate::pools::{PoolInfo, PoolResolver};
use crate::writer::Writer;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use carbon_core::datasource::DatasourceDisconnection;
use tokio::sync::mpsc::Sender;

use crate::gaps;

pub struct PumpSwapEventProcessor {
    pub resolver: Option<PoolResolver>,
    pub requested: HashMap<Pubkey, Instant>,
    pub events: Option<EventLog>,
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

        let stale = match self.requested.get(&pool) {
            Some(at) => at.elapsed() >= POOL_RETRY_AFTER,
            None => true,
        };

        if stale {
            self.requested.insert(pool, Instant::now());
            resolver.request(pool);
        }

        None
    }

    fn record(&self, meta: &carbon_core::instruction::InstructionMetadata, started: Instant) {
        inc(&EVENTS_DECODED);

        if let Some(events) = &self.events {
            let mut log = events.lock().unwrap();
            log.push(EventId {
                signature: meta.transaction_metadata.signature.to_string(),
                absolute_path: meta.absolute_path.clone(),
                event_ordinal: 0,
            });
        }

        DECODE_TIME.observe(started.elapsed());
    }
}

impl carbon_core::processor::Processor<InstructionProcessorInputType<'_, PumpSwapInstruction>>
    for PumpSwapEventProcessor
{
    async fn process(
        &mut self,
        data: &InstructionProcessorInputType<'_, PumpSwapInstruction>,
    ) -> carbon_core::error::CarbonResult<()> {
        let started = Instant::now();

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
                self.record(meta, started);
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

                    let row = oriented.as_ref().map(|t| TradeRow {
                        pool: Some(trade.pool.to_string()),
                        token_mint: t.token_mint.to_string(),
                        side: if t.is_buy { "buy" } else { "sell" },
                        sol_amount: Decimal::from(t.sol_amount),
                        token_amount: Decimal::from(t.token_amount),
                        trader: trade.user.to_string(),
                        fee: Some(Decimal::from(trade.protocol_fee)),
                    });

                    writer
                        .send(PendingWrite {
                            event,
                            trade: row,
                            queued_at: Instant::now(),
                        })
                        .await;
                }

                match oriented {
                    Some(_) => inc(&TRADES_WRITTEN),
                    None => inc(&UNORIENTED),
                }
            }

            PumpSwapCpiEvent::SellEvent(trade) => {
                self.record(meta, started);
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

                    let row = oriented.as_ref().map(|t| TradeRow {
                        pool: Some(trade.pool.to_string()),
                        token_mint: t.token_mint.to_string(),
                        side: if t.is_buy { "buy" } else { "sell" },
                        sol_amount: Decimal::from(t.sol_amount),
                        token_amount: Decimal::from(t.token_amount),
                        trader: trade.user.to_string(),
                        fee: Some(Decimal::from(trade.protocol_fee)),
                    });

                    writer
                        .send(PendingWrite {
                            event,
                            trade: row,
                            queued_at: Instant::now(),
                        })
                        .await;
                }

                match oriented {
                    Some(_) => inc(&TRADES_WRITTEN),
                    None => inc(&UNORIENTED),
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
