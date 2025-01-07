use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;
use tracing::error;
use wtransport::tls::Certificate;
use wtransport::Connection;

use super::executor::Message;
use crate::config::serde::ConnectionConfig;

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Active(Connection),
    Inactive,
}

#[derive(Debug, Clone)]
pub struct ConnectionWrapper {
    pub name: String,
    pub config: ConnectionConfig,
    pub connection: Connection,
}

impl ConnectionWrapper {
    pub fn new(
        name: String,
        config: ConnectionConfig,
        connection: Connection,
    ) -> ConnectionWrapper {
        ConnectionWrapper { name, config, connection }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionPool {
    connections: HashMap<String, Arc<ConnectionWrapper>>,
    tokio_handles: HashMap<String, Arc<tokio::task::JoinHandle<anyhow::Result<()>>>>,
}

impl ConnectionPool {
    pub fn new() -> ConnectionPool {
        ConnectionPool { connections: HashMap::new(), tokio_handles: HashMap::new() }
    }

    pub async fn add_connection(
        &mut self,
        connection_wrapper: ConnectionWrapper,
        incoming_msg_pub: Sender<Message>,
        broadcast_sub: tokio::sync::broadcast::Receiver<Message>,
    ) {
        let name = connection_wrapper.name.clone();
        let connection_wrapper = Arc::new(connection_wrapper);

        let handler = tokio::spawn({
            let connection_wrapper = connection_wrapper.clone();
            async move {
                connection_supervisor(connection_wrapper, incoming_msg_pub, broadcast_sub).await
            }
        });
        self.tokio_handles.insert(name.clone(), Arc::new(handler));
        self.connections.insert(name, connection_wrapper);
    }

    pub fn get_by_name(&self, name: &str) -> Option<&ConnectionWrapper> {
        self.connections.get(name).map(|c| c.as_ref())
    }

    pub fn get_by_role(&self, _certificate: &Certificate) -> Option<&ConnectionWrapper> {
        // TODO: pub vs sub
        todo!()
    }
}

pub async fn connection_supervisor(
    connection_wrapper: Arc<ConnectionWrapper>,
    incoming_msg_pub: Sender<Message>,
    broadcast_sub: tokio::sync::broadcast::Receiver<Message>,
) -> anyhow::Result<()> {
    let conenction = connection_wrapper.connection.clone();

    let sender_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn({
        let connection = conenction.clone();
        let connection_id = connection_wrapper.name.clone();
        async move { sender(&connection, connection_id, broadcast_sub).await }
    });

    let receiver_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn({
        let connection = conenction.clone();
        let connection_id = connection_wrapper.name.clone();
        async move { receiver(&connection, connection_id, incoming_msg_pub).await }
    });

    tokio::select! {
        v = sender_handle => {
            error!("Sender process error: {:?}", v);
            v?
        }
        v = receiver_handle => {
            error!("Receiver process error: {:?}", v);
            v?
        }
    }
}

pub async fn sender(
    connection: &Connection,
    connection_id: String,
    mut brx: tokio::sync::broadcast::Receiver<Message>,
) -> anyhow::Result<()> {
    'connection: loop {
        let message = brx.recv().await.inspect_err(|err| {
            error!("brx err: {}", err);
        })?;

        if message.connection_id == connection_id {
            // don't send data to self
            continue 'connection;
        }
        tracing::debug!("Received data len {:?}", message.data.len());

        for attempt_no in 1..4 {
            // 3 attempts to send data
            match handle_send(connection, &message.data).await {
                Ok(_) => {
                    tracing::debug!("Data sent to connection {}", connection.stable_id());
                    continue 'connection;
                }
                Err(err) => {
                    error!(
                        "Failed to send data to connection {} (attempt #{}): {}",
                        connection.stable_id(),
                        attempt_no,
                        err
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        anyhow::bail!(
            "Connection {} closed: Failed to send data (max attempts reached)",
            connection.stable_id()
        );
    }
}

async fn handle_send(connection: &Connection, data: &[u8]) -> anyhow::Result<()> {
    tracing::trace!("Handling data len {:?}", data.len());
    let mut send_stream = connection
        .open_uni()
        .await
        .inspect_err(|err| {
            error!("Failed to open send stream: {}", err);
        })?
        .await
        .inspect_err(|err| {
            error!("Failed to accept send stream: {}", err);
        })?;
    send_stream.write_all(data).await.inspect_err(|err| {
        error!("Failed to write data to send stream: {}", err);
    })?;
    send_stream.finish().await.inspect_err(|err| {
        error!("Failed to write data to send stream: {}", err);
    })?;
    Ok(())
}

pub async fn receiver(
    connection: &Connection,
    connection_id: String,
    tx: Sender<Message>,
) -> anyhow::Result<()> {
    loop {
        match handle_recv(connection).await {
            Ok(data) => {
                tracing::debug!("Received data len {}", data.len());
                tx.send(Message { connection_id: connection_id.clone(), data })?;
            }
            Err(err) => {
                error!("Failed to receive data from connection: {}", err);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

pub async fn handle_recv(connection: &Connection) -> anyhow::Result<Vec<u8>> {
    let mut stream = connection.accept_uni().await.context("failed to accept uni stream")?;
    let mut data = Vec::with_capacity(1024);
    let n = stream.read_to_end(&mut data).await.context("failed to read data from stream")?;
    tracing::info!("Received data len {}", n);
    Ok(data)
}
