use std::collections::HashMap;
use std::str::FromStr;
use std::sync::LazyLock;
use std::sync::{Arc, RwLock};

use carbon_pump_swap_decoder::accounts::pool::Pool;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::mpsc;

use sqlx::PgPool;

use crate::config::WSOL_MINT;
use crate::db::{load_pools, write_pool};
use crate::metrics::{DB_WRITE_ERRORS, LOOKUPS_DROPPED, POOL_LOOKUP_ERRORS, POOL_LOOKUPS, inc};

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
    pub base_decimals: Option<u8>,
    pub quote_decimals: Option<u8>,
}

pub type PoolCache = Arc<RwLock<HashMap<Pubkey, PoolInfo>>>;

#[derive(Clone)]
pub struct PoolResolver {
    cache: PoolCache,
    requests: mpsc::Sender<Pubkey>,
    db: Option<PgPool>,
}

impl PoolResolver {
    pub fn get(&self, pool: &Pubkey) -> Option<PoolInfo> {
        self.cache.read().unwrap().get(pool).copied()
    }

    pub async fn insert(&self, pool: Pubkey, info: PoolInfo) {
        self.cache.write().unwrap().insert(pool, info);

        if let Some(db) = &self.db {
            save(db, &pool, &info).await;
        }
    }

    pub fn request(&self, pool: Pubkey) {
        if self.requests.try_send(pool).is_err() {
            inc(&LOOKUPS_DROPPED);
        }
    }

    pub async fn prime(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let Some(db) = &self.db else {
            return Ok(0);
        };

        let stored = load_pools(db).await?;
        let mut cache = self.cache.write().unwrap();

        for p in &stored {
            cache.insert(
                Pubkey::from_str(&p.pool)?,
                PoolInfo {
                    base_mint: Pubkey::from_str(&p.base_mint)?,
                    quote_mint: Pubkey::from_str(&p.quote_mint)?,
                    base_decimals: p.base_decimals.and_then(|d| u8::try_from(d).ok()),
                    quote_decimals: p.quote_decimals.and_then(|d| u8::try_from(d).ok()),
                },
            );
        }

        Ok(stored.len())
    }
}

async fn save(db: &PgPool, pool: &Pubkey, info: &PoolInfo) {
    let written = write_pool(
        db,
        &pool.to_string(),
        &info.base_mint.to_string(),
        &info.quote_mint.to_string(),
        info.base_decimals.map(i32::from),
        info.quote_decimals.map(i32::from),
    )
    .await;

    if written.is_err() {
        inc(&DB_WRITE_ERRORS);
    }
}

pub fn spawn_pool_resolver(rpc: RpcClient, capacity: usize, db: Option<PgPool>) -> PoolResolver {
    let (tx, mut rx) = mpsc::channel::<Pubkey>(capacity);
    let cache: PoolCache = Arc::new(RwLock::new(HashMap::new()));
    let worker_cache = cache.clone();
    let worker_db = db.clone();

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
                        let info = PoolInfo {
                            base_mint: p.base_mint,
                            quote_mint: p.quote_mint,
                            base_decimals: None,
                            quote_decimals: None,
                        };

                        worker_cache.write().unwrap().insert(pool, info);

                        if let Some(db) = &worker_db {
                            save(db, &pool, &info).await;
                        }
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
        db,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn info(base: Pubkey, quote: Pubkey) -> PoolInfo {
        PoolInfo {
            base_mint: base,
            quote_mint: quote,
            base_decimals: None,
            quote_decimals: None,
        }
    }

    #[test]
    fn normal_pool_sol_is_quote() {
        let token = Pubkey::new_unique();
        let p = info(token, *WSOL);

        let t = p.orient(1_000, 50, true).unwrap();

        assert_eq!(t.sol_amount, 50);
        assert_eq!(t.token_amount, 1_000);
        assert_eq!(t.token_mint, token);
        assert!(t.is_buy);
        assert!(!t.sol_is_base);
    }

    #[test]
    fn inverted_pool_sol_is_base() {
        let token = Pubkey::new_unique();
        let p = info(*WSOL, token);

        let t = p.orient(50, 1_000, true).unwrap();

        assert_eq!(t.sol_amount, 50);
        assert_eq!(t.token_amount, 1_000);
        assert_eq!(t.token_mint, token);
        assert!(!t.is_buy);
        assert!(t.sol_is_base);
    }

    #[test]
    fn pool_with_no_sol_side_is_refused() {
        let p = info(Pubkey::new_unique(), Pubkey::new_unique());
        assert!(p.orient(1_000, 50, true).is_none());
    }
}
