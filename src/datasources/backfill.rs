use async_trait::async_trait;

use carbon_core::datasource::{Datasource, DatasourceId, TransactionUpdate, Update, UpdateType};
use carbon_core::error::CarbonResult;
use carbon_core::transformers::transaction_metadata_from_original_meta;
use tokio_util::sync::CancellationToken;

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

pub struct BackfillDatasource {
    pub rpc_url: String,
    pub start_slot: u64,
    pub end_slot: u64,
}

#[async_trait]
impl Datasource for BackfillDatasource {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()> {
        let rpc = RpcClient::new(self.rpc_url.clone());

        let slots = rpc
            .get_blocks(self.start_slot, Some(self.end_slot))
            .await
            .map_err(|e| carbon_core::error::Error::FailedToConsumeDatasource(e.to_string()))?;

        println!(
            "backfill: {} blocks between slots {} and {}",
            slots.len(),
            self.start_slot,
            self.end_slot
        );

        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(false),
            commitment: None,
            max_supported_transaction_version: Some(0),
        };

        let mut sent = 0u64;

        for slot in slots {
            if cancellation_token.is_cancelled() {
                break;
            }

            let block = match rpc.get_block_with_config(slot, config).await {
                Ok(b) => b,
                Err(err) => {
                    eprintln!("backfill: slot {slot} failed: {err}");
                    continue;
                }
            };

            let block_time = block.block_time;

            let Some(transactions) = block.transactions else {
                continue;
            };

            for (index, tx) in transactions.into_iter().enumerate() {

                let Some(decoded) = tx.transaction.decode() else {
                    continue;
                };

                let Some(raw_meta) = tx.meta else {
                    continue;
                };

                let Ok(meta) = transaction_metadata_from_original_meta(raw_meta) else {
                    continue;
                };

                let Some(signature) = decoded.signatures.first().copied() else {
                    continue;
                };

                let update = Update::Transaction(Box::new(TransactionUpdate {
                    signature,
                    transaction: decoded,
                    meta,
                    is_vote: false,
                    slot,
                    index: Some(index as u64),
                    block_time,
                    block_hash: None,
                }));

                if sender.send((update, id.clone())).await.is_err() {
                    return Ok(());
                }

                sent += 1;
            }
        }

        println!("backfill finished: {sent} transactions sent");

        Ok(())
    }


    fn update_types(&self) -> Vec<UpdateType> {
        vec![UpdateType::Transaction]
    }
}
