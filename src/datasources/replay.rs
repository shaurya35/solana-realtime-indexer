use std::fs::File;
use std::io::BufRead;

use async_trait::async_trait;
use base64::Engine;

use carbon_core::datasource::{Datasource, DatasourceId, TransactionUpdate, Update, UpdateType};
use carbon_core::error::CarbonResult;
use tokio_util::sync::CancellationToken;

use solana_signature::Signature;
use yellowstone_grpc_proto::convert_from::{create_tx_meta, create_tx_versioned};
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo;
use yellowstone_grpc_proto::prost::Message;

use crate::metrics::{REPLAY_SKIPPED, inc};

pub struct ReplayDatasource {
    pub path: String,
    pub repeat: u32,
}

#[async_trait]
impl Datasource for ReplayDatasource {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()> {
        let mut sent = 0u64;
        let mut skipped = 0u64;
        let mut pass = 0u32;

        'passes: loop {
            if cancellation_token.is_cancelled() {
                break;
            }

            let file = File::open(&self.path)
                .map_err(|e| carbon_core::error::Error::FailedToConsumeDatasource(e.to_string()))?;

            let reader = std::io::BufReader::new(file);

            for line in reader.lines() {
                if cancellation_token.is_cancelled() {
                    break 'passes;
                }

                let line = line.map_err(|e| {
                    carbon_core::error::Error::FailedToConsumeDatasource(e.to_string())
                })?;

                let value: serde_json::Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => {
                        skipped += 1;
                        inc(&REPLAY_SKIPPED);
                        continue;
                    }
                };

                let slot = value["slot"].as_u64().unwrap_or(0);

                let Some(data) = value["data"].as_str() else {
                    skipped += 1;
                    inc(&REPLAY_SKIPPED);
                    continue;
                };

                let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(data) else {
                    skipped += 1;
                    inc(&REPLAY_SKIPPED);
                    continue;
                };

                let Ok(info) = SubscribeUpdateTransactionInfo::decode(&bytes[..]) else {
                    skipped += 1;
                    inc(&REPLAY_SKIPPED);
                    continue;
                };

                let Ok(signature) = Signature::try_from(info.signature) else {
                    skipped += 1;
                    inc(&REPLAY_SKIPPED);
                    continue;
                };

                let Some(raw_tx) = info.transaction else {
                    skipped += 1;
                    inc(&REPLAY_SKIPPED);
                    continue;
                };

                let Some(raw_meta) = info.meta else {
                    skipped += 1;
                    inc(&REPLAY_SKIPPED);
                    continue;
                };

                let Ok(transaction) = create_tx_versioned(raw_tx) else {
                    skipped += 1;
                    inc(&REPLAY_SKIPPED);
                    continue;
                };

                let Ok(meta) = create_tx_meta(raw_meta) else {
                    skipped += 1;
                    inc(&REPLAY_SKIPPED);
                    continue;
                };

                let update = Update::Transaction(Box::new(TransactionUpdate {
                    signature,
                    transaction,
                    meta,
                    is_vote: info.is_vote,
                    slot,
                    index: Some(info.index),
                    block_time: None,
                    block_hash: None,
                }));

                if sender.send((update, id.clone())).await.is_err() {
                    break 'passes;
                }

                sent += 1;
            }

            pass += 1;

            if self.repeat != 0 && pass >= self.repeat {
                break;
            }

            println!("replay pass {pass} complete, {sent} sent so far");
        }

        println!("replay finished: {pass} passes, {sent} sent, {skipped} skipped");

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![UpdateType::Transaction]
    }
}
