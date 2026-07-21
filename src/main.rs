use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Duration;

use base64::Engine;

use futures::{SinkExt, StreamExt};  

use clap::{Parser, Subcommand};

use solana_pubkey::Pubkey;

use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use carbon_core::instruction::{InstructionProcessorInputType};
use carbon_core::pipeline::Pipeline;
use carbon_pumpfun_decoder::{
    PumpfunDecoder, instructions::{CpiEvent, PumpfunInstruction},
};
use carbon_pump_swap_decoder::{
    PumpSwapDecoder, instructions::{CpiEvent as PumpSwapCpiEvent, PumpSwapInstruction},
    accounts::pool::Pool,
};

use yellowstone_grpc_proto::geyser::SubscribeRequestFilterTransactions;
use yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestPing};
use yellowstone_grpc_proto::prost::Message;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

use carbon_yellowstone_grpc_datasource::YellowstoneGrpcGeyserClient;

use yellowstone_grpc_client::GeyserGrpcClient;

const GRPC_ENDPOINT: &str = "https://solana-rpc.parafi.tech:10443";
const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPSWAP_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

fn transaction_filters() -> HashMap<String, SubscribeRequestFilterTransactions> {
    let mut filters = HashMap::new();

    for (name, program) in [("pumpfun", PUMPFUN_PROGRAM), ("pumpswap", PUMPSWAP_PROGRAM)] {
        filters.insert(
            name.to_string(), 
            SubscribeRequestFilterTransactions{
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: vec![program.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            }
        );
    }

    filters
}

struct TradeEventProcessor;

impl carbon_core::processor::Processor<InstructionProcessorInputType<'_, PumpfunInstruction>> for TradeEventProcessor {
    async fn process(
        &mut self, 
        data: &InstructionProcessorInputType<'_, PumpfunInstruction>
    ) -> carbon_core::error::CarbonResult<()> {
            if data.metadata.transaction_metadata.meta.status.is_err() {
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

struct PoolInfo {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_decimals: u8,
    quote_decimals: u8,
}

struct PumpSwapEventProcessor {
    pools: HashMap<Pubkey, PoolInfo>,
    rpc: RpcClient,
}

impl PumpSwapEventProcessor {
    async fn ensure_pool(&mut self, pool: Pubkey){
        if self.pools.contains_key(&pool){
            return;
        }
        match self.rpc.get_account_data(&pool).await {
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

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Capture {
        #[arg(long, default_value_t = 5)]
        minutes: u64
    },
    Live,
}

async fn run_capture(minutes: u64) -> Result<(), Box<dyn std::error::Error>> {
    
    let stamp = chrono::Local::now().format("%Y%m%dT%H%M%S");
    let path = format!("fixtures/capture-{stamp}.jsonl");

    let mut out = BufWriter::new(File::create(&path)?);

    println!("Capturing to {path} for {minutes} minutes");

    let mut client = GeyserGrpcClient::build_from_shared(GRPC_ENDPOINT.to_string())?
        .x_token(None::<String>)?
        .tls_config(ClientTlsConfig::new().with_enabled_roots())?
        .connect()
        .await?;

    let request = SubscribeRequest {
        transactions: transaction_filters(),
        ..Default::default()
    };

    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(minutes * 60);
    let mut written = 0u64;

    while tokio::time::Instant::now() < deadline {
        let msg = match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(msg))) => msg,
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(None) => break,         
            Err(_) => continue,         
        };

        match msg.update_oneof {
            Some(UpdateOneof::Transaction(update)) => {
                let Some(info) = update.transaction else { continue };

                let signature = bs58::encode(&info.signature).into_string();

                let bytes = info.encode_to_vec();
                let data = base64::engine::general_purpose::STANDARD.encode(&bytes);

                let line = serde_json::json!({
                    "slot": update.slot,
                    "signature": signature,
                    "data": data,
                });

                writeln!(out, "{line}")?;
                written += 1;

                if written % 100 == 0 {
                    println!("{written} transactions written");
                }
            }

            Some(UpdateOneof::Ping(_)) => {
                subscribe_tx
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    })
                    .await?;
            }

            _ => {}
        }
    }

    out.flush()?;

    println!("done — {written} transactions in {path}");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

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
        Commands::Live => {}
    }

    let transaction_filters = transaction_filters();

    println!("Transaction filters: {}", transaction_filters.len());

    let grpc_client = YellowstoneGrpcGeyserClient::new(
        GRPC_ENDPOINT.to_string(), 
        None, 
        None, 
        HashMap::new(), 
        transaction_filters, 
        Default::default(), Default::default(), 
        Default::default(), 
        None, 
        None,
    );

    Pipeline::builder()
        .datasource(grpc_client)
        .instruction(PumpfunDecoder, TradeEventProcessor)
        .instruction(PumpSwapDecoder, PumpSwapEventProcessor { 
            pools: HashMap::new(),
            rpc: RpcClient::new("https://api.mainnet-beta.solana.com".to_string()),
        })
        .build()?
        .run().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::str::FromStr;
    use solana_instruction::{AccountMeta, Instruction};
    use carbon_core::instruction::InstructionDecoder;

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
}
