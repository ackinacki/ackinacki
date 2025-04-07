use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;
use std::time::Instant;

use futures::future::Either;
use itertools::Itertools;
use url::Url;

use crate::detailed;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::start_critical_task_ex;
use crate::tls::create_client_config;
use crate::tls::TlsConfig;
use crate::DeliveryPhase;
use crate::SendMode;

const RESOLVE_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const CONNECT_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

struct DirectPeer {
    messages_tx: tokio::sync::mpsc::Sender<(NetMessage, Instant)>,
}

impl DirectPeer {
    pub fn new(messages_tx: tokio::sync::mpsc::Sender<(NetMessage, Instant)>) -> Self {
        Self { messages_tx }
    }
}

pub async fn run_direct_sender<PeerId>(
    metrics: Option<NetMetrics>,
    mut messages_rx: tokio::sync::mpsc::UnboundedReceiver<(PeerId, NetMessage, Instant)>,
    peers_rx: tokio::sync::watch::Receiver<HashMap<PeerId, Url>>,
    config: TlsConfig,
) where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    let (peer_sender_stopped_tx, mut peer_sender_stopped_rx) =
        tokio::sync::mpsc::unbounded_channel::<PeerId>();

    let mut peers = HashMap::<PeerId, DirectPeer>::new();
    loop {
        let result = tokio::select! {
            result = messages_rx.recv() => Either::Left(result),
            result = peer_sender_stopped_rx.recv() => Either::Right(result),
        };
        match result {
            Either::Left(Some((peer_id, net_message, buffer_duration))) => {
                tracing::debug!(
                    peer_id = peer_id.to_string(),
                    msg_id = net_message.id,
                    msg_type = net_message.label,
                    broadcast = false,
                    "Message delivery: accepted by peer dispatcher"
                );
                let messages_tx = if let Some(peer) = peers.get(&peer_id) {
                    &peer.messages_tx
                } else {
                    let (peer_messages_tx, peer_messages_rx) = tokio::sync::mpsc::channel(10);
                    start_critical_task_ex(
                        "Direct peer sender",
                        peer_id.clone(),
                        peer_sender_stopped_tx.clone(),
                        peer_sender(
                            metrics.clone(),
                            peer_id.clone(),
                            peer_messages_rx,
                            peers_rx.clone(),
                            config.clone(),
                        ),
                    );
                    peers.insert(peer_id.clone(), DirectPeer::new(peer_messages_tx));
                    &peers.get(&peer_id).unwrap().messages_tx
                };
                let label = net_message.label.clone();
                if messages_tx.send((net_message, buffer_duration)).await.is_err() {
                    tracing::error!(
                        peer_id = peer_id.to_string(),
                        msg_type = label,
                        broadcast = false,
                        "Message delivery: forwarding to peer sender failed"
                    );
                    metrics.as_ref().inspect(|x| {
                        x.finish_delivery_phase(
                            DeliveryPhase::OutgoingBuffer,
                            1,
                            &label,
                            SendMode::Direct,
                            buffer_duration.elapsed(),
                        )
                    });
                }
            }
            Either::Right(Some(peer_id)) => {
                tracing::trace!(peer_id = peer_id.to_string(), "Peer sender stopped");
                peers.remove(&peer_id);
            }
            Either::Left(None) | Either::Right(None) => {
                break;
            }
        }
    }
}

async fn peer_sender<PeerId>(
    metrics: Option<NetMetrics>,
    peer_id: PeerId,
    mut messages_rx: tokio::sync::mpsc::Receiver<(NetMessage, Instant)>,
    mut peers_rx: tokio::sync::watch::Receiver<HashMap<PeerId, Url>>,
    config: TlsConfig,
) -> anyhow::Result<()>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    loop {
        let urls = resolve_peer_urls(&mut peers_rx, &peer_id).await;
        let (connection, url) = match connect_to_peer(&config, &peer_id, &urls).await {
            Ok(connection) => connection,
            Err(err) => {
                tracing::error!(
                    peer_id = peer_id.to_string(),
                    urls = urls.iter().map(|x| x.to_string()).join(","),
                    "Failed to connect to peer: {err}"
                );
                continue;
            }
        };
        let (transfer_result_tx, mut transfer_result_rx) = tokio::sync::mpsc::channel(10);
        loop {
            tokio::select! {
                result = messages_rx.recv() => {
                    if let Some((mut net_message, buffer_duration)) = result {
                        let metrics = metrics.clone();
                        let connection = connection.clone();
                        let peer_id = peer_id.clone();
                        let transfer_result_tx = transfer_result_tx.clone();

                        // It is not critical task because it serves single message transfer
                        // and we do not need a result
                        tokio::spawn(async move {
                            metrics.as_ref().inspect(|x| {
                                x.finish_delivery_phase(
                                    DeliveryPhase::OutgoingBuffer,
                                    1,
                                    &net_message.label,
                                    SendMode::Direct,
                                    buffer_duration.elapsed(),
                                );
                                x.start_delivery_phase(
                                    DeliveryPhase::OutgoingTransfer,
                                    1,
                                    &net_message.label,
                                    SendMode::Direct,
                                );
                            });
                            tracing::debug!(
                                peer_id = peer_id.to_string(),
                                msg_id = net_message.id,
                                msg_type = net_message.label,
                                broadcast = false,
                                "Message delivery: outgoing transfer started"
                            );
                            let transfer_duration = Instant::now();
                            let transfer_result = transfer(connection, &mut net_message).await;
                            metrics.as_ref().inspect(|x|x.finish_delivery_phase(
                                DeliveryPhase::OutgoingTransfer,
                                1,
                                &net_message.label,
                                SendMode::Direct,
                                transfer_duration.elapsed(),
                            ));
                            let _ = transfer_result_tx.send((transfer_result, net_message)).await;
                        });
                    } else {
                        break;
                    }
                },
                transfer_result = transfer_result_rx.recv() => {
                    match transfer_result.unwrap() {
                        (Ok(_), message) => {
                            tracing::debug!(
                                peer_id = peer_id.to_string(),
                                msg_id = message.id,
                                msg_type = message.label,
                                broadcast = false,
                                url = url.to_string(),
                                "Message delivery: outgoing transfer finished"
                            );
                        }
                        (Err(err), net_message) => {
                            tracing::error!(
                                broadcast = false,
                                msg_type = net_message.label,
                                msg_id = net_message.id,
                                peer_id = peer_id.to_string(),
                                url = url.to_string(),
                                "Message delivery: outgoing transfer failed: {}",
                                detailed(&err)
                            );
                            metrics.as_ref().inspect(|x| {
                                x.report_outgoing_transfer_error(&net_message.label, SendMode::Direct);
                            });
                            connection.close(1u8.into(), b"Outgoing transfer failed");
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn resolve_peer_urls<P>(
    peers_rx: &mut tokio::sync::watch::Receiver<HashMap<P, Url>>,
    peer_id: &P,
) -> Vec<Url>
where
    P: Display + Hash + Eq,
{
    // track attempt counter for tracing
    let mut attempt = 0;
    loop {
        if let Some(url) = peers_rx.borrow_and_update().get(peer_id) {
            return vec![url.clone()];
        }
        tracing::warn!(peer_id = peer_id.to_string(), attempt, "Failed to resolve peer url");
        tokio::time::sleep(RESOLVE_RETRY_TIMEOUT).await;
        attempt += 1;
    }
}

async fn connect_to_peer<'u, PeerId>(
    config: &TlsConfig,
    peer_id: &PeerId,
    urls: &'u [Url],
) -> anyhow::Result<(wtransport::Connection, &'u Url)>
where
    PeerId: Display,
{
    let client_config = create_client_config(config)?;
    let endpoint = wtransport::Endpoint::client(client_config)?;
    // track attempt counter for tracing
    let mut attempt = 0;
    loop {
        for url in urls {
            match endpoint.connect(&url).await {
                Ok(connection) => {
                    tracing::trace!(
                        broadcast = false,
                        peer_id = peer_id.to_string(),
                        url = url.to_string(),
                        "Outgoing connection established"
                    );
                    return Ok((connection, url));
                }
                Err(e) => {
                    tracing::warn!(
                        broadcast = false,
                        peer_id = peer_id.to_string(),
                        url = url.to_string(),
                        attempt,
                        "Failed to establish outgoing connection: {}",
                        detailed(&e)
                    );
                }
            }
        }
        tokio::time::sleep(CONNECT_RETRY_TIMEOUT).await;
        attempt += 1;
    }
}

async fn transfer(
    connection: wtransport::Connection,
    wrapped: &mut NetMessage,
) -> anyhow::Result<()> {
    let bytes = bincode::serialize(&wrapped)?;
    let opening = connection
        .open_uni()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open outgoing stream: {}", e))?;
    let mut send = opening.await?;
    send.write_all(&bytes)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to transfer outgoing bytes: {}", detailed(&e)))?;
    send.finish()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to close outgoing stream: {}", detailed(&e)))?;
    Ok(())
}
