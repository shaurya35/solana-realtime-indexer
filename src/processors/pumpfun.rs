use carbon_core::instruction::InstructionProcessorInputType;
use carbon_pumpfun_decoder::instructions::{CpiEvent, PumpfunInstruction};

use crate::identity::{EventId, EventLog};
use crate::db::{self, EventRow, TradeRow};
use crate::metrics::{EVENTS_DECODED, SKIPPED_FAILED, inc};

use serde_json::Value;
use sqlx::PgPool;

pub struct TradeEventProcessor {
    pub events: EventLog,
    pub db: Option<PgPool>,
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
        };

        match data.decoded_instruction {
            PumpfunInstruction::CpiEvent { data: cpi_data, .. } => match cpi_data {
                CpiEvent::TradeEvent(trade) => {
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
                        inc(&EVENTS_DECODED);
                    }

                    if let Some(pool) = &self.db {
                        let sig = meta.transaction_metadata.signature.to_string();

                        let event = EventRow {
                            signature: &sig,
                            absolute_path: &meta.absolute_path,
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

                        db::write(pool, &event, row.as_ref()).await;
                    }
                    
                    println!("Mint: {}", trade.mint);
                    println!("User: {}", trade.user);
                    println!("Is buy: {}", trade.is_buy);
                    println!("Token amount: {}", trade.token_amount);
                    println!("SOL amount: {}", trade.sol_amount);
                }
                _ => {}
            },
            _ => {}
        }

        Ok(())
    }
}
