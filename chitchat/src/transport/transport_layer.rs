use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use async_channel::Receiver;
use async_channel::Sender;
use tokio::task::JoinHandle;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;
use transport_layer::NetConnection;
use transport_layer::NetCredential;
use transport_layer::NetIncomingRequest;
use transport_layer::NetListener;
use transport_layer::NetRecvRequest;
use transport_layer::NetTransport;

use crate::transport::Socket;
use crate::ChitchatMessage;
use crate::Deserializable;
use crate::Serializable;

const MAX_QUIC_DATAGRAM_PAYLOAD_SIZE: usize = 10_000_000;
const CHANNEL_CAPACITY: usize = 1000;
const MESSAGE_SOFT_TTL: Duration = Duration::from_secs(30);
const SOFT_LEN_THRESHOLD: usize = 1000;
const GOSSIP_ALPN: &str = "gossip";

#[derive(Clone)]
pub struct TransportLayerTransport<Transport: NetTransport + Clone> {
    transport: Transport,
    credential: NetCredential,
}

impl<Transport: NetTransport> TransportLayerTransport<Transport> {
    pub fn new(transport: Transport, credential: NetCredential) -> Self {
        info!("Creating chitchat transport");
        Self { transport, credential }
    }
}

pub struct TransportLayerSocket<T: NetTransport> {
    transport: T,
    credential: NetCredential,
    outgoing_connections: HashMap<SocketAddr, Arc<OutgoingConnection>>,
    incoming_message_rx: Receiver<(SocketAddr, Instant, ChitchatMessage)>,
    listener_stop_tx: tokio::sync::watch::Sender<bool>,
}

impl<T: NetTransport> Drop for TransportLayerSocket<T> {
    fn drop(&mut self) {
        self.listener_stop_tx.send_replace(true);
    }
}

struct OutgoingConnection {
    task: JoinHandle<()>,
    message_tx: Sender<ChitchatMessage>,
}

impl<Transport: NetTransport + 'static> TransportLayerSocket<Transport> {
    pub fn new(
        transport: Transport,
        credential: NetCredential,
        incoming_message_rx: Receiver<(SocketAddr, Instant, ChitchatMessage)>,
        listener_stop_tx: tokio::sync::watch::Sender<bool>,
    ) -> Self {
        Self {
            transport,
            credential,
            outgoing_connections: HashMap::new(),
            incoming_message_rx,
            listener_stop_tx,
        }
    }

    async fn get_or_create_outgoing_connection(
        &mut self,
        to_addr: SocketAddr,
    ) -> anyhow::Result<Arc<OutgoingConnection>> {
        if let Some(existing_connection) = self.outgoing_connections.get(&to_addr) {
            if !existing_connection.task.is_finished() {
                return Ok(existing_connection.clone());
            }
            warn!("existing connection to {to_addr} is finished");
        }
        warn!("create outgoing connection to {to_addr}");
        let connection =
            self.transport.connect(to_addr, &[GOSSIP_ALPN], self.credential.clone()).await?;
        let (message_tx, message_rx) = async_channel::bounded(CHANNEL_CAPACITY);
        let task = tokio::spawn(handle_outgoing_connection(message_rx, connection));
        let new_connection = Arc::new(OutgoingConnection { message_tx, task });
        self.outgoing_connections.insert(to_addr, new_connection.clone());
        info!(open_connections = self.open_connections(), "open connections");
        Ok(new_connection)
    }

    pub fn open_connections(&self) -> usize {
        self.outgoing_connections.len()
    }
}

#[async_trait::async_trait]
impl<Transport: NetTransport + 'static> crate::transport::Transport
    for TransportLayerTransport<Transport>
{
    fn max_datagram_payload_size(&self) -> usize {
        MAX_QUIC_DATAGRAM_PAYLOAD_SIZE
    }

    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let (incoming_message_tx, incoming_message_rx) = async_channel::bounded(CHANNEL_CAPACITY);
        let (listener_stop_tx, listener_stop_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(handle_socket_listener(
            self.transport.clone(),
            listen_addr,
            self.credential.clone(),
            incoming_message_tx,
            listener_stop_rx,
        ));
        Ok(Box::new(TransportLayerSocket::new(
            self.transport.clone(),
            self.credential.clone(),
            incoming_message_rx,
            listener_stop_tx,
        )))
    }
}

#[async_trait::async_trait]
impl<Transport: NetTransport + 'static> Socket for TransportLayerSocket<Transport> {
    async fn send(&mut self, to: SocketAddr, msg: ChitchatMessage) -> anyhow::Result<()> {
        let connection = self
            .get_or_create_outgoing_connection(to)
            .await
            .with_context(|| "failed to create connection")?;

        match connection.message_tx.force_send(msg) {
            Ok(None) => {}
            Ok(Some(_)) => {
                error!("message dropped");
            }
            Err(_err) => {
                anyhow::bail!("outgoing channel was closed");
            }
        }
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        let (from_addr, message) = {
            loop {
                let (from_addr, received_at, message) = self
                    .incoming_message_rx
                    .recv()
                    .await
                    .with_context(|| "Incoming channel is broken")?;
                debug!("message received: {:?}", message);
                // skip message by soft ttl if channel surpasses upper bound
                if self.incoming_message_rx.len() >= SOFT_LEN_THRESHOLD
                    && received_at.elapsed() > MESSAGE_SOFT_TTL
                {
                    info!(
                        "Incoming message expired by soft ttl, queue len: {}, received: {:?}",
                        self.incoming_message_rx.len(),
                        received_at.elapsed()
                    );
                    continue;
                }
                break (from_addr, message);
            }
        };

        info!("chitchat recv message from {}", from_addr);

        Ok((from_addr, message))
    }
}

async fn handle_socket_listener<Transport: NetTransport + 'static>(
    transport: Transport,
    listen_addr: SocketAddr,
    cred: NetCredential,
    incoming_tx: Sender<(SocketAddr, Instant, ChitchatMessage)>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    info!("Creating chitchat socket listener");
    let listener = transport.create_listener(listen_addr, &[GOSSIP_ALPN], cred).await?;
    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow_and_update() {
                    break;
                }
            }
            accept_result = listener.accept() => match accept_result {
                Ok(connection_request) => {
                    tokio::spawn(handle_incoming_connection(connection_request, incoming_tx.clone()));
                }
                Err(err) => {
                    warn!(%err, "Chitchat socket listener accept failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
    Ok(())
}

async fn handle_incoming_connection<IncomingRequest: NetIncomingRequest + 'static>(
    connection_request: IncomingRequest,
    incoming_tx: Sender<(SocketAddr, Instant, ChitchatMessage)>,
) -> anyhow::Result<()> {
    let connection = connection_request.accept().await?;
    let from_addr = connection.remote_addr();
    info!("new connection from {from_addr}");
    loop {
        let recv_request = connection.accept_recv().await?;
        let start = Instant::now();
        let message = recv_request.recv().await?;
        let len = message.len();
        let message = ChitchatMessage::deserialize(&mut message.as_slice())?;

        tracing::trace!(len, from_addr = from_addr.to_string(), "Message received");

        tracing::trace!(
            "sending for processing {:?} sender_count {} sender_len {}",
            start.elapsed(),
            incoming_tx.sender_count(),
            incoming_tx.len(),
        );

        match incoming_tx.force_send((from_addr, Instant::now(), message)) {
            Ok(None) => {}
            Ok(Some(_)) => {
                error!("message dropped");
            }
            Err(err) => {
                warn!(%err, "incoming connection handler stopped");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_outgoing_connection<Connection: NetConnection + 'static>(
    messages_rx: Receiver<ChitchatMessage>,
    connection: Connection,
) {
    while let Ok(message) = messages_rx.recv().await {
        let delivery_time = Instant::now();
        let serialized = message.serialize_to_vec();
        match connection.send(&serialized).await.context("failed to open uni stream") {
            Ok(_) => {
                let delivery_duration = delivery_time.elapsed();
                if delivery_duration > Duration::from_millis(600) {
                    warn!("message delivery took {:?} ({})", delivery_duration, serialized.len());
                }
            }
            Err(err) => {
                error!(%err, "failed to send message");
                break;
            }
        }
    }
}
