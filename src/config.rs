use std::collections::HashMap;
use std::env;
use std::sync::LazyLock;

use yellowstone_grpc_proto::geyser::SubscribeRequestFilterTransactions;

pub static GRPC_ENDPOINT: LazyLock<String> = LazyLock::new(|| {
    env::var("YELLOWSTONE_GRPC_ENDPOINT").expect("YELLOWSTONE_GRPC_ENDPOINT not set")
});

pub fn database_url() -> Option<String> {
    env::var("DATABASE_URL").ok()
}

pub const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const PUMPSWAP_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

pub const WRITE_QUEUE_SIZE: usize = 10_000;

pub const PIPELINE_QUEUE_SIZE: usize = 10_000;
pub const POOL_LOOKUP_QUEUE_SIZE: usize = 1_000;

pub const GAP_QUEUE_SIZE: usize = 64;

pub const SLOT_GAP_TOLERANCE: u64 = 50;

pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

pub static SOLANA_RPC_URL: LazyLock<String> =
    LazyLock::new(|| env::var("SOLANA_RPC_URL").expect("SOLANA_RPC_URL not set"));

pub fn grpc_x_token() -> Option<String> {
    env::var("YELLOWSTONE_X_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
}

pub fn transaction_filters() -> HashMap<String, SubscribeRequestFilterTransactions> {
    let mut filters = HashMap::new();

    for (name, program) in [("pumpfun", PUMPFUN_PROGRAM), ("pumpswap", PUMPSWAP_PROGRAM)] {
        filters.insert(
            name.to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: vec![program.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );
    }

    filters
}
