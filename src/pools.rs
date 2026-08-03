use std::collections::HashMap;
use std::str::FromStr;
use std::sync::LazyLock;
use std::sync::{Arc, RwLock};

use carbon_pump_swap_decoder::accounts::pool::Pool;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::mpsc;

use crate::config::WSOL_MINT;
use crate::metrics::{LOOKUPS_DROPPED, POOL_LOOKUP_ERRORS, POOL_LOOKUPS, inc};

static WSOL: LazyLock<Pubkey> = LazyLock::new(|| Pubkey::from_str(WSOL_MINT).unwrap());

pub struct Trade {
    pub sol_amount: u64,
    pub token_amount: u64,
    pub token_mint: Pubkey,
    pub is_buy: bool,
    pub sol_is_base: bool,
}

impl PoolInfo {
    pub fn orient(
        &self,
        base_amount: u64,
        quote_amount: u64,
        acquiring_base: bool,
    ) -> Option<Trade> {
        if self.quote_mint == *WSOL {
            Some(Trade {
                sol_amount: quote_amount,
                token_amount: base_amount,
                token_mint: self.base_mint,
                is_buy: acquiring_base,
                sol_is_base: false,
            })
        } else if self.base_mint == *WSOL {
            Some(Trade {
                sol_amount: base_amount,
                token_amount: quote_amount,
                token_mint: self.quote_mint,
                is_buy: !acquiring_base,
                sol_is_base: true,
            })
        } else {
            None
        }
    }
}

#[derive(Clone, Copy)]
pub struct PoolInfo {
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
}

pub type PoolCache = Arc<RwLock<HashMap<Pubkey, PoolInfo>>>;

#[derive(Clone)]
pub struct PoolResolver {
    cache: PoolCache,
    requests: mpsc::Sender<Pubkey>,
}

impl PoolResolver {
    pub fn get(&self, pool: &Pubkey) -> Option<PoolInfo> {
        self.cache.read().unwrap().get(pool).copied()
    }

    pub fn insert(&self, pool: Pubkey, info: PoolInfo) {
        self.cache.write().unwrap().insert(pool, info);
    }

    pub fn request(&self, pool: Pubkey) {
        if self.requests.try_send(pool).is_err() {
            inc(&LOOKUPS_DROPPED);
        }
    }
}

pub fn spawn_pool_resolver(rpc: RpcClient, capacity: usize) -> PoolResolver {
    let (tx, mut rx) = mpsc::channel::<Pubkey>(capacity);
    let cache: PoolCache = Arc::new(RwLock::new(HashMap::new()));
    let worker_cache = cache.clone();

    tokio::spawn(async move {
        while let Some(pool) = rx.recv().await {
            let already_cached = worker_cache.read().unwrap().contains_key(&pool);
            if already_cached {
                continue;
            }

            inc(&POOL_LOOKUPS);

            match rpc.get_account_data(&pool).await {
                Ok(data) => match Pool::decode(&data) {
                    Some(p) => {
                        worker_cache.write().unwrap().insert(
                            pool,
                            PoolInfo {
                                base_mint: p.base_mint,
                                quote_mint: p.quote_mint,
                                base_decimals: 0,
                                quote_decimals: 0,
                            },
                        );
                    }
                    None => inc(&POOL_LOOKUP_ERRORS),
                },
                Err(_) => inc(&POOL_LOOKUP_ERRORS),
            }
        }
    });

    PoolResolver {
        cache,
        requests: tx,
    }
}
