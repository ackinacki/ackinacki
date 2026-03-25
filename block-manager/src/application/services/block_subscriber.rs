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

/// Single connection attempt: connect, receive blocks until error, then return Err.
pub async fn run_once(
    tx: &mpsc::Sender<WorkerCommand>,
    socket_addr: SocketAddr,
    metrics: &Option<Metrics>,
) -> anyhow::Result<()> {
    let transport = MsQuicTransport::new();
    let conn = match transport
        .connect(
            socket_addr,
            &["ALPN"],
            NetCredential::generate_self_signed(Some(vec![socket_addr.to_string()]), &[])?,
        )
        .await
    {
        Ok(conn) => {
            tracing::info!(peer = socket_addr.to_string(), "Connection established");
            conn
        }
        Err(error) => {
            tracing::error!("Can't connect to {socket_addr}: {error}");
            if let Some(m) = metrics {
                m.bm.report_errors(ERR_OPEN_CONN);
            }
            bail!("Can't connect to {socket_addr}: {error}");
        }
    };

    loop {
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
                if let Some(m) = metrics {
                    m.bm.report_errors(ERR_RECV_BLOCK);
                }
                tracing::error!("Error receiving a message: {error}");
                bail!("Error receiving a message from {socket_addr}: {error}");
            }
        }
    }
}

/// Infinite retry loop wrapping run_once() — backwards compatible with existing single-node mode.
pub async fn run(
    tx: mpsc::Sender<WorkerCommand>,
    socket_addr: SocketAddr,
    metrics: Option<Metrics>,
) -> anyhow::Result<()> {
    loop {
        if let Err(e) = run_once(&tx, socket_addr, &metrics).await {
            tracing::warn!("Connection to {socket_addr} failed: {e}, retrying in 1s...");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
