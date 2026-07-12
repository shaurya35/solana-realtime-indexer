use std::fs;
use std::str::FromStr;

use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;

use carbon_core::instruction::InstructionDecoder;
use carbon_pumpfun_decoder::{
    PumpfunDecoder,
    instructions::{CpiEvent, PumpfunInstruction},
};

fn main() -> Result<(), Box<dyn std::error::Error>> { 

    let pumpfun_program_id = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

    let decoder = PumpfunDecoder;

    let content = 
        fs::read_to_string("fixtures/pumpfun-buy-via-flashx-01-parsed.json")?;

    let json: serde_json::Value = serde_json::from_str(&content)?;

    let inner_groups = json["result"]["meta"]["innerInstructions"]
        .as_array()
        .unwrap();

    for group in inner_groups {
        if group["index"].as_u64() == Some(3) {
            println!("Found group with index: {}", group["index"]);

            let instructions = group["instructions"]
                .as_array()
                .unwrap();

            for (position, instruction) in instructions.iter().enumerate() {
                if instruction["programId"].as_str() == Some(pumpfun_program_id) {
                    let encoded_data = instruction["data"]
                        .as_str()
                        .unwrap();

                    let decoded_data = bs58::decode(encoded_data)
                        .into_vec()?;

                    let account_val = instruction["accounts"]
                        .as_array()
                        .unwrap();

                    let mut accounts = Vec::new();

                    for account in account_val {
                        let address = account.as_str().unwrap();
                        let pubkey = Pubkey::from_str(address)?;

                        accounts.push(AccountMeta::new_readonly(pubkey, false));
                    }   

                    let program_id = Pubkey::from_str(
                        instruction["programId"].as_str().unwrap()
                    )?;

                    let solana_instruction = Instruction {
                        program_id,
                        accounts,
                        data: decoded_data,
                    };

                    match decoder.decode_instruction(&solana_instruction) {
                        Some(PumpfunInstruction::Buy { .. }) => {
                            println!("Position {} decoded as Buy", position);
                        }

                        Some(PumpfunInstruction::CpiEvent { data, .. }) => {
                            match data {
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
                                }

                                _ => {
                                    println!("Position {} contains another event", position);
                                }
                            }
                        }

                        Some(_) => {
                            println!("Position {} decoded as another Pumpfun instruction", position);
                        }

                        None => {
                            println!("Position {} could not be decoded", position);
                        }
                    }
                    
                }
            }
        }
    }

    Ok(())
}