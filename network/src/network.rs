// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;

use chitchat::ChitchatRef;
use serde::Serialize;
use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedChannelMetrics;
use telemetry_utils::mpsc::InstrumentedReceiver;
use transport_layer::NetTransport;

use crate::channel::NetBroadcastSender;
use crate::channel::NetDirectSender;
use crate::config::NetworkConfig;
use crate::detailed;
use crate::direct_sender;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::spawn_critical_task;
use crate::pub_sub::IncomingSender;
use crate::resolver::watch_gossip;
use crate::resolver::SubscribeStrategy;
use crate::resolver::WatchGossipConfig;

pub const BROADCAST_RETENTION_CAPACITY: usize = 100;
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

#[derive(Clone, Debug)]
pub struct PeerData {
    pub peer_addr: SocketAddr,
    pub bk_api_socket: Option<SocketAddr>,
}

pub struct BasicNetwork<Transport: NetTransport> {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    transport: Transport,
}

impl<Transport: NetTransport + 'static> BasicNetwork<Transport> {
    pub fn new(
        shutdown_tx: tokio::sync::watch::Sender<bool>,
        config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
        transport: Transport,
    ) -> Self {
        Self { shutdown_tx, config_rx, transport }
    }

    pub async fn start<PeerId, Message, ChannelMetrics>(
        &self,
        watch_gossip_config_rx: tokio::sync::watch::Receiver<WatchGossipConfig>,
        metrics: Option<NetMetrics>,
        channel_metrics: Option<ChannelMetrics>,
        self_peer_id: PeerId,
        is_proxy: bool,
        chitchat: ChitchatRef,
    ) -> anyhow::Result<(
        NetDirectSender<PeerId, Message>,
        NetBroadcastSender<Message>,
        InstrumentedReceiver<IncomingMessage>,
        tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
    )>
    where
        Message:
            Debug + for<'de> serde::Deserialize<'de> + Serialize + Send + Sync + Clone + 'static,
        PeerId: Clone + Display + Send + Sync + Hash + Eq + FromStr<Err: Display> + 'static,
        ChannelMetrics: InstrumentedChannelMetrics + Send + Sync + 'static,
    {
        tracing::info!("Starting network with configuration: {:?}", *self.config_rx.borrow());

        let (incoming_tx, incoming_rx) =
            instrumented_channel::<IncomingMessage>(channel_metrics, "network_incoming");
        let (outgoing_broadcast_tx, _) = tokio::sync::broadcast::channel::<OutgoingMessage>(1000);
        let (outgoing_direct_tx, outgoing_direct_rx) =
            tokio::sync::mpsc::unbounded_channel::<(PeerId, NetMessage, std::time::Instant)>();

        let (subscribe_tx, subscribe_rx) = tokio::sync::watch::channel(Vec::new());
        let (peers_tx, peers_rx) = tokio::sync::watch::channel(HashMap::new());
        let (gossip_subscribe_tx, gossip_subscribe_rx) = tokio::sync::watch::channel(Vec::new());
        tokio::spawn(combine_subscribe(
            self.shutdown_tx.subscribe(),
            self.config_rx.clone(),
            gossip_subscribe_rx,
            subscribe_tx,
        ));

        spawn_critical_task(
            "Gossip",
            watch_gossip(
                self.shutdown_tx.subscribe(),
                watch_gossip_config_rx,
                SubscribeStrategy::Peer(self_peer_id.clone()),
                chitchat.clone(),
                gossip_subscribe_tx,
                peers_tx,
                metrics.clone(),
            ),
        );

        // listen for pub/sub connections
        let metrics_clone = metrics.clone();
        let outgoing_broadcast_tx_clone = outgoing_broadcast_tx.clone();
        let transport_clone = self.transport.clone();
        let shutdown_rx_clone = self.shutdown_tx.subscribe();
        let config_rx_clone = self.config_rx.clone();
        spawn_critical_task("Pub/Sub", async move {
            if let Err(e) = crate::pub_sub::run(
                shutdown_rx_clone,
                config_rx_clone,
                transport_clone,
                is_proxy,
                metrics_clone,
                DEFAULT_MAX_CONNECTIONS,
                subscribe_rx,
                outgoing_broadcast_tx_clone,
                IncomingSender::SyncUnbounded(incoming_tx),
            )
            .await
            {
                tracing::warn!("pub/sub run task failed: {}", detailed(&e));
            }
        });

        // listen for outgoing directed messages
        let peers_rx_clone = peers_rx.clone();
        let metrics_clone = metrics.clone();
        let transport_clone = self.transport.clone();
        spawn_critical_task(
            "Direct sender",
            direct_sender::run_direct_sender(
                self.shutdown_tx.subscribe(),
                self.config_rx.clone(),
                transport_clone,
                metrics_clone,
                outgoing_direct_rx,
                peers_rx_clone,
            ),
        );

        Ok((
            NetDirectSender::<PeerId, Message>::new(
                outgoing_direct_tx,
                metrics.clone(),
                self_peer_id,
            ),
            NetBroadcastSender::<Message>::new(outgoing_broadcast_tx, metrics.clone()),
            incoming_rx,
            peers_rx,
        ))
    }
}

async fn combine_subscribe(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut network_config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    mut gossip_subscribe_rx: tokio::sync::watch::Receiver<Vec<Vec<SocketAddr>>>,
    subscribe_tx: tokio::sync::watch::Sender<Vec<Vec<SocketAddr>>>,
) {
    let mut network_config = network_config_rx.borrow().clone();
    let mut gossip_subscribe = gossip_subscribe_rx.borrow().clone();
    loop {
        if !network_config.subscribe.is_empty() {
            subscribe_tx.send_replace(network_config.subscribe.clone());
        } else if !network_config.proxies.is_empty() {
            subscribe_tx.send_replace(vec![network_config.proxies.clone()]);
        } else {
            subscribe_tx.send_replace(gossip_subscribe.clone());
        };
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                return;
            },
            sender = network_config_rx.changed() => if sender.is_err() {
                return;
            } else {
                network_config = network_config_rx.borrow().clone();
            },
            sender = gossip_subscribe_rx.changed() => if sender.is_err() {
                return;
            } else {
                gossip_subscribe = gossip_subscribe_rx.borrow().clone();
            }
        }
    }
}
