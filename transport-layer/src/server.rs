// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::sync::Arc;

use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::msquic::MsQuicNetIncomingRequest;
use crate::msquic::MsQuicTransport;
use crate::NetConnection;
use crate::NetCredential;
use crate::NetIncomingRequest;
use crate::NetListener;
use crate::NetTransport;

const DEFAULT_BROADCAST_CAPACITY: usize = 10;

#[derive(Debug, Clone)]
pub struct LiteServer {
    pub bind: SocketAddr,
}

impl LiteServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }

    pub async fn start<TBPResolver, A>(
        self,
        raw_block_receiver: InstrumentedReceiver<(A, Vec<u8>)>,
        bp_resolver: TBPResolver,
    ) -> anyhow::Result<()>
    where
        TBPResolver: Send + Sync + Clone + 'static + FnMut(A) -> Option<String>,
        A: Send + 'static,
    {
        let (incoming_request_tx, incoming_request_rx) =
            tokio::sync::mpsc::unbounded_channel::<MsQuicNetIncomingRequest>();
        let (outgoing_message_tx, _ /* we will subscribe() later */) =
            tokio::sync::broadcast::channel(DEFAULT_BROADCAST_CAPACITY);

        let listener_task = tokio::spawn(listener_handler(self.bind, incoming_request_tx));

        let incoming_requests_task = tokio::spawn(incoming_requests_handler(
            incoming_request_rx,
            outgoing_message_tx.clone(),
        ));

        let multiplexer_task = tokio::task::spawn_blocking(move || {
            message_multiplexor_handler(
                raw_block_receiver,
                outgoing_message_tx.clone(),
                bp_resolver,
            )
        });

        tokio::select! {
            v = listener_task => v??,
            v = multiplexer_task => v??,
            v = incoming_requests_task => v??,
        }

        Ok(())
    }
}

async fn listener_handler(
    bind: SocketAddr,
    incoming_request_tx: tokio::sync::mpsc::UnboundedSender<MsQuicNetIncomingRequest>,
) -> anyhow::Result<()> {
    let transport = MsQuicTransport::new();
    let listener = transport
        .create_listener(
            bind,
            &["ALPN"],
            NetCredential::generate_self_signed(Some(vec![bind.to_string()]), None)?,
        )
        .await?;
    tracing::info!("LiteServer started on port {}", bind.port());
    loop {
        match listener.accept().await {
            Ok(incoming_request) => {
                tracing::info!("New incoming request");
                incoming_request_tx.send(incoming_request)?;
            }
            Err(error) => tracing::error!("LiteServer can't accept request: {error}"),
        }
    }
}
async fn incoming_requests_handler(
    mut incoming_request_rx: tokio::sync::mpsc::UnboundedReceiver<MsQuicNetIncomingRequest>,
    outgoing_message_tx: tokio::sync::broadcast::Sender<Arc<Vec<u8>>>,
) -> anyhow::Result<()> {
    loop {
        match incoming_request_rx.recv().await {
            Some(incoming_request) => {
                tokio::spawn(connection_supervisor(
                    incoming_request,
                    outgoing_message_tx.subscribe(),
                ));
            }
            None => {
                anyhow::bail!("Connections handler failed: incoming request receiver was closed");
            }
        }
    }
}

async fn connection_supervisor(
    incoming_request: MsQuicNetIncomingRequest,
    outgoing_message_rx: tokio::sync::broadcast::Receiver<Arc<Vec<u8>>>,
) {
    match connection_handler(incoming_request, outgoing_message_rx).await {
        Ok(_) => {}
        Err(err) => {
            tracing::error!("Connection handler failed: {err}");
        }
    }
}

async fn connection_handler(
    incoming_request: MsQuicNetIncomingRequest,
    mut outgoing_message_rx: tokio::sync::broadcast::Receiver<Arc<Vec<u8>>>,
) -> anyhow::Result<()> {
    tracing::info!("Establishing connection");
    let connection = incoming_request.accept().await?;
    tracing::info!(remote_addr = connection.remote_addr().to_string(), "Connection established");
    loop {
        let data = match outgoing_message_rx.recv().await {
            Ok(data) => data,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged)) => {
                anyhow::bail!(
                    "Connection handler failed: outgoing message receiver lagged by {} messages",
                    lagged
                );
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                anyhow::bail!("Connection handler failed: outgoing message receiver was closed");
            }
        };
        let peer = connection.remote_addr().to_string();
        tracing::trace!("Received {} bytes for {peer}", data.len());
        match connection.send(&data).await {
            Ok(_) => {
                tracing::info!("Sent {} bytes to {peer}", data.len())
            }
            Err(err) => {
                tracing::error!("Can't {} bytes to {peer}: {err}", data.len());
                anyhow::bail!(err);
            }
        }
    }
}

fn message_multiplexor_handler<TBKAddrResolver, A>(
    incoming_message_rx: InstrumentedReceiver<(A, Vec<u8>)>,
    outgoing_message_tx: tokio::sync::broadcast::Sender<Arc<Vec<u8>>>,
    mut bp_resolver: TBKAddrResolver,
) -> anyhow::Result<()>
where
    TBKAddrResolver: Send + Sync + Clone + 'static + FnMut(A) -> Option<String>,
    A: Send,
{
    tracing::info!("Message multiplexor started");
    loop {
        let Ok((node_id, message)) = incoming_message_rx.recv() else {
            anyhow::bail!("Message multiplexor failed: incoming message sender was closed");
        };
        tracing::trace!(
            broadcast_channel_len = message.len(),
            message = format!("{:?}", &message[..10]),
            "Received message for broadcast"
        );
        let node_addr = bp_resolver(node_id);
        match outgoing_message_tx.send(Arc::new(bincode::serialize(&(node_addr, message))?)) {
            Ok(number_subscribers) => {
                tracing::info!("Message forwarded to {} broadcast senders", number_subscribers);
            }
            Err(_err) => {
                tracing::warn!("Message not forwarded: no broadcast senders");
            }
        }
    }
}
