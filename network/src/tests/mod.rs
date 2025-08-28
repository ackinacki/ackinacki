mod test_hot_reload;
mod transport_test;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use chitchat::ChitchatHandle;
use chitchat::ChitchatRef;
use gossip::GossipConfig;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use serde::Serialize;
use telemetry_utils::mpsc::InstrumentedChannelMetrics;
use telemetry_utils::mpsc::InstrumentedReceiver;
use tokio::task::JoinHandle;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;
use transport_layer::NetTransport;

use crate::channel::NetBroadcastSender;
use crate::channel::NetDirectSender;
use crate::config::NetworkConfig;
use crate::network::BasicNetwork;
use crate::network::PeerData;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::connection::MessageDelivery;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::CertFile;
use crate::pub_sub::CertStore;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PrivateKeyFile;
use crate::resolver::watch_gossip;
use crate::resolver::GossipPeer;
use crate::resolver::SubscribeStrategy;
use crate::resolver::WatchGossipConfig;

fn make_addr(group: usize, index: usize) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], (11000 + group * 10 + index) as u16))
}

pub fn node_addr(group: usize) -> SocketAddr {
    make_addr(group, 0)
}

pub fn gossip_addr(group: usize) -> SocketAddr {
    make_addr(group, 1)
}

static LOG_INIT: OnceCell<()> = OnceCell::new();

pub fn init_logs() {
    LOG_INIT.get_or_init(|| {
        let log_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/test.log");
        let file = std::fs::File::create(log_path).expect("Failed to create log file");
        let writer = BoxMakeWriter::new(file);
        FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .with_writer(writer)
            .init();
        // tracing_subscriber::fmt()
        //     .with_env_filter(EnvFilter::from_default_env())
        //     .with_test_writer()
        //     .init();
    });
}

pub struct NoChannelMetrics;

impl InstrumentedChannelMetrics for NoChannelMetrics {
    fn report_channel(&self, _channel: &'static str, _delta: isize) {}
}

pub struct Proxy<Transport: NetTransport + 'static> {
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub config_tx: tokio::sync::watch::Sender<NodeConfig>,
    pub watch_gossip_config_tx: tokio::sync::watch::Sender<WatchGossipConfig>,
    pub transport: Transport,
    pub chitchat: ChitchatRef,
    pub chitchat_handle: ChitchatHandle,
    pub gossip_rest_handle: JoinHandle<anyhow::Result<()>>,
}

impl<Transport: NetTransport + 'static> Drop for Proxy<Transport> {
    fn drop(&mut self) {
        self.shutdown_tx.send(true).unwrap();
    }
}

impl<Transport: NetTransport + 'static> Proxy<Transport> {
    pub(crate) async fn start(config: NodeConfig, transport: Transport) -> anyhow::Result<Self> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (config_tx, config_rx) = tokio::sync::watch::channel(config.clone());
        tracing::info!("Gossip advertise addr: {:?}", config.gossip.advertise_addr);

        let (network_config_tx, network_config_rx) =
            tokio::sync::watch::channel(config.network.clone());
        let (gossip_config_tx, gossip_config_rx) =
            tokio::sync::watch::channel(config.gossip.clone());
        tokio::spawn(split_config(
            shutdown_rx.clone(),
            config_rx,
            network_config_tx,
            gossip_config_tx,
        ));

        let (chitchat_handle, gossip_rest_handle) =
            gossip::run(shutdown_rx.clone(), gossip_config_rx, chitchat::transport::UdpTransport)
                .await?;

        let (outgoing_messages_tx, _ /* we will subscribe() later */) =
            tokio::sync::broadcast::channel(100);
        let (incoming_messages_tx, incoming_messages_rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(Self::message_multiplexor(incoming_messages_rx, outgoing_messages_tx.clone()));

        let (watch_gossip_config_tx, watch_gossip_config_rx) =
            tokio::sync::watch::channel(WatchGossipConfig { trusted_pubkeys: HashSet::new() });
        let (subscribe_tx, subscribe_rx) = tokio::sync::watch::channel(Vec::new());
        let (peers_tx, _) = tokio::sync::watch::channel(HashMap::new());
        tokio::spawn(watch_gossip(
            shutdown_rx.clone(),
            watch_gossip_config_rx,
            SubscribeStrategy::<String>::Proxy(vec![config.network.bind]),
            chitchat_handle.chitchat(),
            subscribe_tx.clone(),
            peers_tx,
            None,
        ));

        let transport_clone = transport.clone();
        tokio::spawn(crate::pub_sub::run(
            shutdown_rx,
            network_config_rx,
            transport_clone,
            true,
            None,
            10000,
            subscribe_rx,
            outgoing_messages_tx,
            IncomingSender::AsyncUnbounded(incoming_messages_tx),
        ));

        let chitchat = chitchat_handle.chitchat();
        Ok(Self {
            shutdown_tx,
            config_tx,
            watch_gossip_config_tx,
            transport,
            chitchat_handle,
            gossip_rest_handle,
            chitchat,
        })
    }

    async fn message_multiplexor(
        mut incoming_messages: tokio::sync::mpsc::UnboundedReceiver<IncomingMessage>,
        outgoing_messages: tokio::sync::broadcast::Sender<OutgoingMessage>,
    ) -> anyhow::Result<()> {
        tracing::info!("Proxy multiplexor bridge started");
        loop {
            match incoming_messages.recv().await {
                Some(incoming) => {
                    let label = incoming.message.label.clone();
                    tracing::debug!("Proxy multiplexor forwarded incoming {}", label);
                    if let Ok(_sent_count) = outgoing_messages.send(OutgoingMessage {
                        delivery: MessageDelivery::BroadcastExcluding(incoming.connection_info),
                        message: incoming.message,
                        duration_before_transfer: Instant::now(),
                    }) {}
                }
                None => {
                    tracing::info!("Proxy multiplexor bridge stopped");
                    break;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn print_stat(&self) {
        let live_nodes = self.chitchat.lock().live_nodes().try_len().unwrap_or_default();
        println!("LiveNodes: {live_nodes}");
    }
}

#[derive(Clone)]
pub struct NodeConfig {
    pub(crate) node_id: String,
    network: NetworkConfig,
    gossip: GossipConfig,
}

impl NodeConfig {
    pub fn new(
        node_addr: SocketAddr,
        gossip_addr: SocketAddr,
        node_id: String,
        gossip_seeds: Vec<SocketAddr>,
    ) -> Self {
        let key_file = PrivateKeyFile::default();
        let cert_file = CertFile::default();
        Self {
            node_id,
            network: NetworkConfig::new(
                node_addr,
                cert_file,
                key_file,
                None,
                CertStore::default(),
                HashSet::new(),
                vec![],
                vec![],
                None,
            )
            .unwrap(),
            gossip: GossipConfig {
                advertise_addr: None,
                listen_addr: gossip_addr,
                seeds: gossip_seeds,
                cluster_id: "transport_test".to_string(),
            },
        }
    }
}

pub async fn split_config(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut config_rx: tokio::sync::watch::Receiver<NodeConfig>,
    network_config_tx: tokio::sync::watch::Sender<NetworkConfig>,
    gossip_config_tx: tokio::sync::watch::Sender<GossipConfig>,
) {
    loop {
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                return;
            },
            sender = config_rx.changed() => if sender.is_err() {
                return;
            } else {
                network_config_tx.send_replace(config_rx.borrow().network.clone());
                gossip_config_tx.send_replace(config_rx.borrow().gossip.clone());
            }
        }
    }
}

pub struct Node<Transport: NetTransport + 'static> {
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub config_tx: tokio::sync::watch::Sender<NodeConfig>,
    pub watch_gossip_config_tx: tokio::sync::watch::Sender<WatchGossipConfig>,
    pub transport: Transport,
    pub(crate) chitchat: ChitchatRef,
    pub(crate) state: Arc<NodeState>,
    pub chitchat_handle: ChitchatHandle,
    pub gossip_rest_handle: JoinHandle<anyhow::Result<()>>,
}

pub struct NodeState {
    pub(crate) data_sent: AtomicUsize,
    pub(crate) data_received: AtomicUsize,
    pub(crate) ack_sent: AtomicUsize,
    pub(crate) ack_received: AtomicUsize,
    pub(crate) unconfirmed: std::sync::Mutex<HashMap<u64, usize>>,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            unconfirmed: std::sync::Mutex::new(HashMap::new()),
            data_sent: AtomicUsize::new(0),
            data_received: AtomicUsize::new(0),
            ack_sent: AtomicUsize::new(0),
            ack_received: AtomicUsize::new(0),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Message {
    Data(String, u64, Vec<u8>),
    Ack(u64),
}

impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Data(sender, id, data) => {
                write!(f, "Data: {}, {}, {} bytes", sender, id, data.len())
            }
            Message::Ack(id) => write!(f, "Ack: {id}"),
        }
    }
}

pub struct NodeChannels {
    pub(crate) direct_tx: NetDirectSender<String, Message>,
    pub(crate) broadcast_tx: NetBroadcastSender<Message>,
    pub(crate) incoming_rx: InstrumentedReceiver<IncomingMessage>,
    pub(crate) peers_rx: tokio::sync::watch::Receiver<HashMap<String, PeerData>>,
}

impl<Transport: NetTransport + 'static> Drop for Node<Transport> {
    fn drop(&mut self) {
        self.shutdown_tx.send_replace(true);
    }
}

impl<Transport: NetTransport + 'static> Node<Transport> {
    pub async fn start(
        config: NodeConfig,
        transport: Transport,
    ) -> anyhow::Result<(Self, NodeChannels)> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (config_tx, config_rx) = tokio::sync::watch::channel(config);
        let config = config_rx.borrow().clone();
        let (network_config_tx, network_config_rx) =
            tokio::sync::watch::channel(config.network.clone());
        let (gossip_config_tx, _gossip_config_rx) =
            tokio::sync::watch::channel(config.gossip.clone());
        let (watch_gossip_config_tx, watch_gossip_config_rx) =
            tokio::sync::watch::channel(WatchGossipConfig { trusted_pubkeys: HashSet::new() });
        tokio::spawn(split_config(
            shutdown_rx.clone(),
            config_rx,
            network_config_tx,
            gossip_config_tx,
        ));

        tracing::info!("Gossip advertise addr: {:?}", config.gossip.advertise_addr);

        let (_, gossip_config_rx) = tokio::sync::watch::channel(config.gossip.clone());
        let (chitchat_handle, gossip_rest_handle) =
            gossip::run(shutdown_rx.clone(), gossip_config_rx, chitchat::transport::UdpTransport)
                .await?;

        let gossip_node = GossipPeer::new(
            config.node_id.clone(),
            config.network.bind,
            config.network.proxies.clone(),
            None,
            None,
            None,
        )?;
        chitchat_handle
            .with_chitchat(|c| {
                gossip_node.set_to(c.self_node_state());
            })
            .await;

        let network = BasicNetwork::new(shutdown_tx.clone(), network_config_rx, transport.clone());
        let chitchat = chitchat_handle.chitchat();

        let (direct_tx, broadcast_tx, incoming_rx, peers_rx) = network
            .start(
                watch_gossip_config_rx,
                None,
                Option::<NoChannelMetrics>::None,
                config.node_id.clone(),
                false,
                chitchat.clone(),
            )
            .await?;

        let state = Arc::new(NodeState::default());

        Ok((
            Self {
                shutdown_tx,
                config_tx,
                watch_gossip_config_tx,
                transport,
                chitchat_handle,
                gossip_rest_handle,
                chitchat,
                state,
            },
            NodeChannels { direct_tx, broadcast_tx, incoming_rx, peers_rx },
        ))
    }

    pub(crate) fn print_stat(&self, node_count: usize) {
        let unconfirmed = self.state.unconfirmed.lock().unwrap().values().sum::<usize>();
        if unconfirmed <= node_count {
            return;
        }
        let data_sent = self.state.data_sent.load(Ordering::Relaxed);
        let data_received = self.state.data_received.load(Ordering::Relaxed);
        let ack_sent = self.state.ack_sent.load(Ordering::Relaxed);
        let ack_received = self.state.ack_received.load(Ordering::Relaxed);
        let live_nodes = self.chitchat.lock().live_nodes().try_len().unwrap_or_default();
        println!(
            "LiveNodes: {live_nodes} of {node_count}, Unconfirmed: {unconfirmed}, Data sent: {data_sent}, Data received: {data_received}, Ack sent: {ack_sent}, Ack received: {ack_received}"
        );
    }
}
