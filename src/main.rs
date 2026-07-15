use std::fs;
use std::str::FromStr;
use std::collections::HashMap;

use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;

use carbon_core::instruction::{InstructionDecoder, InstructionProcessorInputType};
use carbon_core::pipeline::Pipeline;
use carbon_pumpfun_decoder::{
    PumpfunDecoder, instructions::{CpiEvent, PumpfunInstruction},
};

use yellowstone_grpc_proto::geyser::SubscribeRequestFilterTransactions;
use carbon_yellowstone_grpc_datasource::YellowstoneGrpcGeyserClient;

struct TradeEventProcessor;

impl carbon_core::processor::Processor<InstructionProcessorInputType<'_, PumpfunInstruction>> for TradeEventProcessor {
    async fn process(
        &mut self, 
        data: &InstructionProcessorInputType<'_, PumpfunInstruction>) -> carbon_core::error::CarbonResult<()> {
            match data.decoded_instruction {
                PumpfunInstruction::CpiEvent { data: cpi_data, .. } => match cpi_data {
                    CpiEvent::TradeEvent(trade) => {
                        println!("Trade event found!");
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let pumpfun_filter = SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: Some(false),
        signature: None,
        account_include: vec![
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".to_string(),
        ],
        account_exclude: vec![],
        account_required: vec![],
    };

    let mut transaction_filters = HashMap::new();

    transaction_filters.insert(
        "pumpfun".to_string(),
        pumpfun_filter,
    );

    println!("Transaction filters: {}", transaction_filters.len());

    let grpc_client = YellowstoneGrpcGeyserClient::new(
        "https://solana-rpc.parafi.tech:10443".to_string(), 
        None, 
        None, 
        HashMap::new(), 
        transaction_filters, 
        Default::default(), Default::default(), 
        Default::default(), 
        None, 
        None,
    );

    env_logger::init();

    rustls::crypto::aws_lc_rs::default_provider().install_default().unwrap();

    Pipeline::builder()
        .datasource(grpc_client)
        .instruction(PumpfunDecoder, TradeEventProcessor)
        .build()?
        .run().await?;

    let trades = decode_fixture("fixtures/pumpfun-buy-via-flashx-01-parsed.json")?;

    println!("Accepted trades: {}", trades.len());

    Ok(())
}

fn decode_fixture(
    fixture_path: &str,
) -> Result<
    Vec<carbon_pumpfun_decoder::events::trade_event::TradeEventEvent>,
    Box<dyn std::error::Error>,
> {
    let pumpfun_program_id = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

    let decoder = PumpfunDecoder;

    let content = fs::read_to_string(fixture_path)?;

    let json: serde_json::Value = serde_json::from_str(&content)?;

    let mut trades = Vec::new();

    if !json["result"]["meta"]["err"].is_null() {
        println!("Skipping failed transaction");
        return Ok(trades);
    }

    let inner_groups = json["result"]["meta"]["innerInstructions"]
        .as_array()
        .unwrap();

    for group in inner_groups {
        println!("Checking inner group: {}", group["index"]);

        let instructions = group["instructions"].as_array().unwrap();

        for (position, instruction) in instructions.iter().enumerate() {
            if instruction["programId"].as_str() == Some(pumpfun_program_id) {
                let encoded_data = instruction["data"].as_str().unwrap();

                let decoded_data = bs58::decode(encoded_data).into_vec()?;

                let account_val = instruction["accounts"].as_array().unwrap();

                let mut accounts = Vec::new();

                for account in account_val {
                    let address = account.as_str().unwrap();
                    let pubkey = Pubkey::from_str(address)?;

                    accounts.push(AccountMeta::new_readonly(pubkey, false));
                }

                let program_id = Pubkey::from_str(instruction["programId"].as_str().unwrap())?;

                let solana_instruction = Instruction {
                    program_id,
                    accounts,
                    data: decoded_data,
                };

                match decoder.decode_instruction(&solana_instruction) {
                    Some(PumpfunInstruction::Buy { .. }) => {
                        println!("Position {} decoded as Buy", position);
                    }

                    Some(PumpfunInstruction::CpiEvent { data, .. }) => match data {
                        CpiEvent::TradeEvent(trade) => {
                            println!("Trade event found!");
                            println!("Mint: {}", trade.mint);
                            println!("User: {}", trade.user);
                            println!("Is buy: {}", trade.is_buy);
                            println!("Token amount: {}", trade.token_amount);
                            println!("SOL amount: {}", trade.sol_amount);
                            println!("Protocol fee: {}", trade.fee);
                            println!("Creator fee: {}", trade.creator_fee);
                            println!("Fee recipient: {}", trade.fee_recipient);
                            println!("Creator: {}", trade.creator);
                            println!("Instruction name: {}", trade.ix_name);

                            trades.push(trade);
                        }

                        _ => {
                            println!("Position {} contains another event", position);
                        }
                    },

                    Some(_) => {
                        println!(
                            "Position {} decoded as another Pumpfun instruction",
                            position
                        );
                    }

                    None => {
                        println!("Position {} could not be decoded", position);
                    }
                }
            }
        }
    }

    Ok(trades)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_successful_pumpfun_trade() {
        let trades = decode_fixture("fixtures/pumpfun-buy-via-flashx-01-parsed.json").unwrap();

        assert_eq!(trades.len(), 1);

        let trade = &trades[0];

        assert_eq!(
            trade.mint.to_string(),
            "2KjpDfEZeA3LHcq1ycHi5qYf9Lc5D1iJtLhSHKUypump"
        );
        assert!(trade.is_buy);
        assert_eq!(trade.token_amount, 3_940_708_338);
        assert_eq!(trade.sol_amount, 97_777);
        assert_eq!(trade.fee, 929);
        assert_eq!(trade.creator_fee, 294);
    }

    #[test]
    fn rejects_failed_transaction() {
        let trades = decode_fixture("fixtures/pumpfun-failed-01.json").unwrap();

        assert_eq!(trades.len(), 0);
    }
}
