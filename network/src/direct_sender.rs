use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

use futures::future::Either;
use itertools::Itertools;
use transport_layer::NetConnection;
use transport_layer::NetTransport;

use crate::detailed;
use crate::host_id_prefix;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::network::PeerData;
use crate::pub_sub::connection::connection_remote_host_id;
use crate::pub_sub::start_critical_task_ex;
use crate::tls::TlsConfig;
use crate::transfer::transfer;
use crate::DeliveryPhase;
use crate::SendMode;
use crate::ACKI_NACKI_DIRECT_PROTOCOL;

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

pub async fn run_direct_sender<Transport, PeerId>(
    transport: Transport,
    metrics: Option<NetMetrics>,
    mut messages_rx: tokio::sync::mpsc::UnboundedReceiver<(PeerId, NetMessage, Instant)>,
    peers_rx: tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
    config: TlsConfig,
) where
    Transport: NetTransport + 'static,
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    tracing::info!("Direct sender started with host id {}", config.my_host_id_prefix());
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
                            transport.clone(),
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

async fn peer_sender<Transport, PeerId>(
    transport: Transport,
    metrics: Option<NetMetrics>,
    peer_id: PeerId,
    mut messages_rx: tokio::sync::mpsc::Receiver<(NetMessage, Instant)>,
    mut peers_rx: tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
    config: TlsConfig,
) -> anyhow::Result<()>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
    Transport: NetTransport,
    Transport::Connection: 'static,
{
    loop {
        let addrs = resolve_peer_addrs(&mut peers_rx, &peer_id).await;
        let (connection, host_id, addr) =
            match connect_to_peer(&transport, &config, &peer_id, &addrs).await {
                Ok(connection) => connection,
                Err(err) => {
                    tracing::error!(
                        peer_id = peer_id.to_string(),
                        addrs = addrs.iter().map(|x| x.to_string()).join(","),
                        "Failed to connect to peer: {err}"
                    );
                    continue;
                }
            };
        let (transfer_result_tx, mut transfer_result_rx) = tokio::sync::mpsc::channel(10);
        loop {
            tokio::select! {
                result = messages_rx.recv() => {
                    if let Some((net_message, buffer_duration)) = result {
                        let metrics = metrics.clone();
                        let connection = connection.clone();
                        let host_id = host_id.clone();
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
                                host_id = host_id_prefix(&host_id),
                                msg_id = net_message.id,
                                msg_type = net_message.label,
                                broadcast = false,
                                "Message delivery: outgoing transfer started"
                            );
                            let transfer_duration = Instant::now();
                            let transfer_result = transfer(&connection, &net_message, &metrics).await;
                            metrics.as_ref().inspect(|x|x.finish_delivery_phase(
                                DeliveryPhase::OutgoingTransfer,
                                1,
                                &net_message.label,
                                SendMode::Direct,
                                transfer_duration.elapsed(),
                            ));
                            if let Err(err) = transfer_result_tx.send((transfer_result, net_message, transfer_duration)).await {
                                tracing::error!("Can not report message delivery result: {}",err);
                            }
                        });
                    } else {
                        break;
                    }
                },
                transfer_result = transfer_result_rx.recv() => {
                    match transfer_result.unwrap() {
                        (Ok(bytes_sent), message, transfer_duration) => {
                            tracing::debug!(
                                host_id = host_id_prefix(&host_id),
                                msg_id = message.id,
                                msg_type = message.label,
                                broadcast = false,
                                addr = addr.to_string(),
                                duration = transfer_duration.elapsed().as_millis(),
                                "Message delivery: outgoing transfer finished"
                            );
                            metrics.as_ref().inspect(|m| {
                                m.report_sent_bytes(bytes_sent, &message.label, SendMode::Direct);
                            });
                        }
                        (Err(err), net_message, _) => {
                            tracing::error!(
                                broadcast = false,
                                msg_type = net_message.label,
                                msg_id = net_message.id,
                                host_id = host_id_prefix(&host_id),
                                addr = addr.to_string(),
                                "Message delivery: outgoing transfer failed: {}",
                                detailed(&err)
                            );
                            metrics.as_ref().inspect(|x| {
                                x.report_outgoing_transfer_error(&net_message.label, SendMode::Direct, err);
                            });
                            connection.close(0).await;
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn resolve_peer_addrs<PeerId>(
    peers_rx: &mut tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
    peer_id: &PeerId,
) -> Vec<SocketAddr>
where
    PeerId: Display + Hash + Eq,
{
    // track attempt counter for tracing
    let mut attempt = 0;
    loop {
        if let Some(peer_data) = peers_rx.borrow_and_update().get(peer_id) {
            return vec![peer_data.peer_addr];
        }
        tracing::warn!(peer_id = peer_id.to_string(), attempt, "Failed to resolve peer addr");
        tokio::time::sleep(RESOLVE_RETRY_TIMEOUT).await;
        attempt += 1;
    }
}

async fn connect_to_peer<Transport, PeerId>(
    transport: &Transport,
    config: &TlsConfig,
    peer_id: &PeerId,
    addrs: &[SocketAddr],
) -> anyhow::Result<(Transport::Connection, String, SocketAddr)>
where
    Transport: NetTransport,
    PeerId: Display,
{
    let credential = config.credential();
    // track attempt counter for tracing
    let mut attempt = 0;
    loop {
        for addr in addrs {
            match transport.connect(*addr, &[ACKI_NACKI_DIRECT_PROTOCOL], credential.clone()).await
            {
                Ok(connection) => {
                    let host_id = connection_remote_host_id(&connection);
                    tracing::trace!(
                        broadcast = false,
                        host_id = host_id_prefix(&host_id),
                        addr = addr.to_string(),
                        "Outgoing connection established"
                    );
                    return Ok((connection, host_id, *addr));
                }
                Err(e) => {
                    tracing::warn!(
                        broadcast = false,
                        peer_id = peer_id.to_string(),
                        addr = addr.to_string(),
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
