use std::collections::HashMap;

use yellowstone_grpc_proto::geyser::SubscribeRequestFilterTransactions;

pub const GRPC_ENDPOINT: &str = "https://solana-rpc.parafi.tech:10443";

pub const SOLANA_RPC_URL: &str = "https://api.mainnet-beta.solana.com";

pub const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const PUMPSWAP_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

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
