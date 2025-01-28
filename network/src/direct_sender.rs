use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::mpsc::Receiver;
use std::time::Duration;

use async_trait::async_trait;
use url::Url;

use crate::detailed;
use crate::tls::create_client_config;
use crate::NetworkMessage;
use crate::NetworkPeerId;

const RESOLVE_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const CONNECT_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

#[async_trait]
pub trait PeerResolver
where
    <Self::PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<Self::PeerId as FromStr>::Err>,
{
    type PeerId: NetworkPeerId;
    async fn resolve_peer(&self, peer_id: &Self::PeerId) -> Option<Url>;
}

struct DirectPeer<Message: NetworkMessage> {
    messages_tx: tokio::sync::mpsc::Sender<Message>,
    _task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl<Message: NetworkMessage> DirectPeer<Message> {
    pub fn new(
        messages_tx: tokio::sync::mpsc::Sender<Message>,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
    ) -> Self {
        Self { messages_tx, _task: task }
    }
}

pub async fn run_direct_sender<PeerId: NetworkPeerId, Message: NetworkMessage + 'static>(
    self_id: PeerId,
    messages_rx: Receiver<(PeerId, Message)>,
    send_buffer_size: usize,
    peer_resolver: impl PeerResolver<PeerId = PeerId> + Clone + Send + Sync + 'static,
    config: crate::pub_sub::Config,
) -> anyhow::Result<()>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    assert!(send_buffer_size > 1);
    let _ = tokio::task::spawn_blocking(move || {
        tracing::trace!("Start direct message sender");
        let mut peers = HashMap::<PeerId, DirectPeer<Message>>::new();
        while let Ok((peer_id, message)) = messages_rx.recv() {
            if peer_id == self_id {
                tracing::trace!("Skip message to self");
                continue;
            }
            tracing::trace!(peer_id = peer_id.to_string(), "Received direct message");
            if let Some(peer) = peers.get(&peer_id) {
                let _ = peer.messages_tx.try_send(message);
            } else {
                let (peer_messages_tx, peer_messages_rx) =
                    tokio::sync::mpsc::channel(send_buffer_size);
                let task = tokio::spawn(peer_sender(
                    peer_id.clone(),
                    peer_messages_rx,
                    peer_resolver.clone(),
                    config.clone(),
                ));
                let _ = peer_messages_tx.try_send(message);
                peers.insert(peer_id, DirectPeer::new(peer_messages_tx, task));
            }
        }

        Ok::<_, anyhow::Error>(())
    })
    .await;
    Ok(())
}

async fn peer_sender<PeerId: NetworkPeerId, Message: NetworkMessage>(
    peer_id: PeerId,
    mut messages_rx: tokio::sync::mpsc::Receiver<Message>,
    peer_resolver: impl PeerResolver<PeerId = PeerId> + Clone + Send + Sync + 'static,
    config: crate::pub_sub::Config,
) -> anyhow::Result<()>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    loop {
        let url = resolve_peer_url(&peer_resolver, &peer_id).await?;
        let connection = match connect_to_peer(&config, &peer_id, &url).await {
            Ok(connection) => connection,
            Err(err) => {
                tracing::error!(
                    peer_id = peer_id.to_string(),
                    url = url.to_string(),
                    "Failed to connect to peer: {err}"
                );
                continue;
            }
        };
        while let Some(message) = messages_rx.recv().await {
            tracing::trace!(peer_id = peer_id.to_string(), "Sending direct message");
            let message = if let Ok(data) = bincode::serialize(&message) {
                data
            } else {
                tracing::error!(
                    peer_id = peer_id.to_string(),
                    "Failed to serialize direct message"
                );
                continue;
            };
            match send_message(&connection, &message, &peer_id, &url).await {
                Ok(()) => {}
                Err(err) => {
                    tracing::error!(
                        peer_id = peer_id.to_string(),
                        url = url.to_string(),
                        "Failed to send direct message: {err}"
                    );
                    connection.close(1u8.into(), b"Failed to send message");
                    break;
                }
            }
        }
    }
}

async fn resolve_peer_url<PeerId: NetworkPeerId>(
    peer_resolver: &impl PeerResolver<PeerId = PeerId>,
    peer_id: &PeerId,
) -> anyhow::Result<Url>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    loop {
        match peer_resolver.resolve_peer(peer_id).await {
            Some(url) => {
                return Ok(url);
            }
            None => {
                tracing::warn!(peer_id = peer_id.to_string(), "Failed to resolve peer url");
                tokio::time::sleep(RESOLVE_RETRY_TIMEOUT).await;
            }
        }
    }
}

async fn connect_to_peer<PeerId: NetworkPeerId>(
    config: &crate::pub_sub::Config,
    peer_id: &PeerId,
    url: &Url,
) -> anyhow::Result<wtransport::Connection>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    let client_config = create_client_config(config)?;
    let endpoint = wtransport::Endpoint::client(client_config)?;
    loop {
        match endpoint.connect(&url).await {
            Ok(connection) => {
                tracing::trace!(
                    peer_id = peer_id.to_string(),
                    url = url.to_string(),
                    "Direct message connection established"
                );
                return Ok(connection);
            }
            Err(e) => {
                tracing::warn!(
                    peer_id = peer_id.to_string(),
                    url = url.to_string(),
                    "Failed to establish direct message connection: {}",
                    detailed(&e)
                );
                tokio::time::sleep(CONNECT_RETRY_TIMEOUT).await;
            }
        }
    }
}

async fn send_message<PeerId: NetworkPeerId>(
    connection: &wtransport::Connection,
    message: &[u8],
    peer_id: &PeerId,
    url: &Url,
) -> anyhow::Result<()>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    match send_stream(connection, message, peer_id, url).await {
        Err(e) => {
            tracing::warn!(
                peer_id = peer_id.to_string(),
                url = url.to_string(),
                "Failed to send direct message: {}",
                detailed(&e)
            );
            anyhow::bail!("Failed to send message: {e}");
        }
        Ok(()) => {
            tracing::trace!(
                peer_id = peer_id.to_string(),
                url = url.to_string(),
                "Direct message sent successfully"
            );
        }
    }
    Ok(())
}

async fn send_stream<PeerId: NetworkPeerId>(
    established_connection: &wtransport::Connection,
    data: &[u8],
    peer_id: &PeerId,
    url: &Url,
) -> anyhow::Result<()>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    tracing::trace!("Start sending data");
    let opening = established_connection
        .open_uni()
        .await
        .map_err(|e| anyhow::anyhow!("failed to open stream: {}", e))?;

    let mut send = opening.await?;

    // TODO: add this later to handle NAT
    //
    // if rebind {
    // let socket = std::net::UdpSocket::bind("[::]:0").unwrap();
    // let addr = socket.local_addr().unwrap();
    // eprintln!("rebinding to {addr}");
    // endpoint.rebind(socket).expect("rebind failed");
    // }

    send.write_all(data)
        .await
        .map_err(|e| anyhow::anyhow!("failed to send request: {}", detailed(&e)))?;
    send.finish()
        .await
        .map_err(|e| anyhow::anyhow!("failed to shutdown stream: {}", detailed(&e)))?;
    tracing::trace!(
        peer_id = peer_id.to_string(),
        url = url.to_string(),
        "Finish sending direct message data"
    );
    Ok(())
}
