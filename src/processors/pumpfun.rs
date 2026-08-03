use carbon_core::instruction::InstructionProcessorInputType;
use carbon_pumpfun_decoder::instructions::{CpiEvent, PumpfunInstruction};

use crate::identity::{EventId, EventLog};
use crate::metrics::{EVENTS_DECODED, SKIPPED_FAILED, inc};

pub struct TradeEventProcessor {
    pub events: EventLog,
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
