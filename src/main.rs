mod api;
mod capture;
mod cli;
mod config;
mod datasources;
mod db;
mod gaps;
mod identity;
mod metrics;
mod pipeline;
mod pools;
mod processors;
mod recover;
mod repair;
mod verify;
mod writer;

use std::collections::HashMap;

use clap::Parser;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use carbon_yellowstone_grpc_datasource::YellowstoneGrpcGeyserClient;

use api::run_api;
use capture::run_capture;
use cli::{Cli, Commands};
use config::{
    GAP_QUEUE_SIZE, GRPC_ENDPOINT, SOLANA_RPC_URL, database_url, grpc_x_token, transaction_filters,
};
use datasources::replay::ReplayDatasource;
use gaps::spawn_gap_recorder;
use identity::EventLog;
use pipeline::run_pipeline;
use recover::run_recover;
use verify::{run_verify, run_verify_range};

use crate::datasources::backfill::BackfillDatasource;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    env_logger::init();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    let cli = Cli::parse();

    match cli.command {
        Commands::Capture { minutes } => {
            run_capture(minutes).await?;
            return Ok(());
        }

        Commands::Replay {
            path,
            repeat,
            resolve,
        } => {
            let events = EventLog::default();
            let rpc = if resolve {
                Some(RpcClient::new(SOLANA_RPC_URL.to_string()))
            } else {
                None
            };

            let db = match database_url() {
                Some(url) => Some(db::connect(&url).await?),
                None => None,
            };

            run_pipeline(
                ReplayDatasource { path, repeat },
                rpc,
                events.clone(),
                db,
                None,
            )
            .await?;
            println!("Collected {} events", events.lock().unwrap().len());
            return Ok(());
        }

        Commands::Verify { path } => {
            let db = db::connect(&database_url().expect("DATABASE_URL not set")).await?;

            let clean = run_verify(path, &db).await?;

            if !clean {
                std::process::exit(1);
            }

            return Ok(());
        }

        Commands::VerifyRange { from, to } => {
            let db = db::connect(&database_url().expect("DATABASE_URL not set")).await?;

            let clean = run_verify_range(from, to, &db).await?;

            if !clean {
                std::process::exit(1);
            }

            return Ok(());
        }

        Commands::Backfill { from, to } => {
            let db = db::connect(&database_url().expect("DATABASE_URL not set")).await?;

            run_pipeline(
                BackfillDatasource {
                    rpc_url: SOLANA_RPC_URL.to_string(),
                    start_slot: from,
                    end_slot: to,
                },
                Some(RpcClient::new(SOLANA_RPC_URL.to_string())),
                EventLog::default(),
                Some(db),
                None,
            )
            .await?;

            return Ok(());
        }

        Commands::Repair { limit } => {
            let db = db::connect(&database_url().expect("DATABASE_URL not set")).await?;
            repair::run_repair(db, limit).await?;
            return Ok(());
        }

        Commands::Recover => {
            let db = db::connect(&database_url().expect("DATABASE_URL not set")).await?;
            run_recover(db).await?;
            return Ok(());
        }

        Commands::Live => {}

        Commands::Api { port } => {
            let db = db::connect(&database_url().expect("DATABASE_URL not set")).await?;

            run_api(db, port).await?;
            return Ok(());
        }
    }

    let filters = transaction_filters();
    println!("Transaction filters: {}", filters.len());

    let db = db::connect(&database_url().expect("DATABASE_URL not set")).await?;

    let gaps = spawn_gap_recorder(db.clone(), GAP_QUEUE_SIZE);

    let grpc_client = YellowstoneGrpcGeyserClient::new(
        GRPC_ENDPOINT.to_string(),
        grpc_x_token(),
        None,
        HashMap::new(),
        filters,
        Default::default(),
        Default::default(),
        Default::default(),
        Some(gaps.clone()),
        None,
    );

    run_pipeline(
        grpc_client,
        Some(RpcClient::new(SOLANA_RPC_URL.to_string())),
        EventLog::default(),
        Some(db),
        Some(gaps),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::str::FromStr;

    use carbon_core::instruction::InstructionDecoder;
    use carbon_pumpfun_decoder::PumpfunDecoder;
    use carbon_pumpfun_decoder::instructions::{CpiEvent, PumpfunInstruction};
    use solana_instruction::{AccountMeta, Instruction};
    use solana_pubkey::Pubkey;

    use crate::identity::EventId;

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

    async fn replay_ids(path: &str) -> Vec<EventId> {
        let events = EventLog::default();

        run_pipeline(
            ReplayDatasource {
                path: path.to_string(),
                repeat: 1,
            },
            None,
            events.clone(),
            None,
            None,
        )
        .await
        .unwrap();

        let mut ids = events.lock().unwrap().clone();
        ids.sort();

        ids
    }

    #[tokio::test]
    async fn replay_is_deterministic() {
        let first = replay_ids("fixtures/golden-500.jsonl").await;
        let second = replay_ids("fixtures/golden-500.jsonl").await;

        assert!(
            !first.is_empty(),
            "no events decoded — fixture or decode path broken"
        );
        assert_eq!(first, second, "same fixture produced different event IDs");
    }
}
