use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Duration;

use base64::Engine;
use futures::{SinkExt, StreamExt};

use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestPing, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::prost::Message;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

use crate::config::{GRPC_ENDPOINT, grpc_x_token, transaction_filters};

pub async fn run_capture(minutes: u64) -> Result<(), Box<dyn std::error::Error>> {
    let stamp = chrono::Local::now().format("%Y%m%dT%H%M%S");
    let path = format!("fixtures/capture-{stamp}.jsonl");

    let mut out = BufWriter::new(File::create(&path)?);

    println!("Capturing to {path} for {minutes} minutes");

    let mut client = GeyserGrpcClient::build_from_shared(GRPC_ENDPOINT.clone())?
        .x_token(grpc_x_token())?
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
                let Some(info) = update.transaction else {
                    continue;
                };

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

                if written.is_multiple_of(100) {
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
