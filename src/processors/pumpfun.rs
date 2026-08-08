use carbon_core::instruction::InstructionProcessorInputType;
use carbon_pumpfun_decoder::instructions::{CpiEvent, PumpfunInstruction};

use crate::db::{EventRow, PendingWrite, TradeRow};
use crate::identity::{EventId, EventLog};
use crate::metrics::{EVENTS_DECODED, SKIPPED_FAILED, inc};

use crate::writer::Writer;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use carbon_core::datasource::DatasourceDisconnection;
use tokio::sync::mpsc::Sender;

use crate::gaps;

use serde_json::Value;

pub struct TradeEventProcessor {
    pub events: EventLog,
    pub writer: Option<Writer>,
    pub watermark: Arc<AtomicU64>,
    pub gaps: Option<Sender<DatasourceDisconnection>>,
}

impl carbon_core::processor::Processor<InstructionProcessorInputType<'_, PumpfunInstruction>>
    for TradeEventProcessor
{
    async fn process(
        &mut self,
        data: &InstructionProcessorInputType<'_, PumpfunInstruction>,
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

        let PumpfunInstruction::CpiEvent {
            data: CpiEvent::TradeEvent(trade),
            ..
        } = data.decoded_instruction
        else {
            return Ok(());
        };

        let meta = &data.metadata;

        {
            let mut log = self.events.lock().unwrap();
            log.push(EventId {
                signature: meta.transaction_metadata.signature.to_string(),
                absolute_path: meta.absolute_path.clone(),
                event_ordinal: 0,
            });
            inc(&EVENTS_DECODED);
        }

        if let Some(writer) = &self.writer {
            let sig = meta.transaction_metadata.signature.to_string();

            let event = EventRow {
                signature: sig,
                absolute_path: meta.absolute_path.clone(),
                event_ordinal: 0,
                slot: meta.transaction_metadata.slot as i64,
                block_time: meta.transaction_metadata.block_time,
                program: "pumpfun",
                event_type: "TradeEvent",
                payload: serde_json::to_value(trade).unwrap_or(Value::Null),
            };

            let row = match (
                i64::try_from(trade.sol_amount),
                i64::try_from(trade.token_amount),
            ) {
                (Ok(sol), Ok(token)) => Some(TradeRow {
                    pool: None,
                    token_mint: trade.mint.to_string(),
                    side: if trade.is_buy { "buy" } else { "sell" },
                    sol_amount: sol,
                    token_amount: token,
                    trader: trade.user.to_string(),
                    fee: i64::try_from(trade.fee).ok(),
                }),
                _ => None,
            };

            writer.send(PendingWrite { event, trade: row }).await;
        }

        Ok(())
    }
}
