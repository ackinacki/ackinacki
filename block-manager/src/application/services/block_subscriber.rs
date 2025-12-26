// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::sync::mpsc;

use anyhow::bail;
use transport_layer::msquic::MsQuicTransport;
use transport_layer::NetConnection;
use transport_layer::NetCredential;
use transport_layer::NetTransport;

use crate::application::metrics::Metrics;
use crate::application::metrics::ERR_OPEN_CONN;
use crate::application::metrics::ERR_RECV_BLOCK;
use crate::domain::models::WorkerCommand;

pub async fn run(
    tx: mpsc::Sender<WorkerCommand>,
    socket_addr: SocketAddr,
    metrics: Option<Metrics>,
) -> anyhow::Result<()> {
    loop {
        let transport = MsQuicTransport::new();
        match transport
            .connect(
                socket_addr,
                &["ALPN"],
                NetCredential::generate_self_signed(Some(vec![socket_addr.to_string()]), &[])?,
            )
            .await
        {
            Ok(conn) => loop {
                match conn.recv().await {
                    Ok((message, duration)) => {
                        tracing::debug!(
                            duration = duration.as_millis(),
                            "Received: {} bytes",
                            message.len()
                        );

                        if tx.send(WorkerCommand::Data(message)).is_err() {
                            bail!("Can't send WorkerCommand. Receiver is deallocated");
                        };
                    }
                    Err(error) => {
                        if let Some(m) = &metrics {
                            m.bm.report_errors(ERR_RECV_BLOCK);
                        }
                        tracing::error!("Error receiving a message: {error}");
                        break;
                    }
                }
            },
            Err(error) => {
                tracing::error!("Can't connect to  {socket_addr}: {error}");
                if let Some(m) = &metrics {
                    m.bm.report_errors(ERR_OPEN_CONN);
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
