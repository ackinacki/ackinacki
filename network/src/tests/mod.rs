mod test_direct_broadcast;
mod test_hot_reload;
mod transport_test;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use chitchat::ChitchatHandle;
use chitchat::ChitchatRef;
use gossip::default_gossip_peer_ttl_seconds;
use gossip::gossip_peer::GossipPeer;
use gossip::run_gossip_with_reload;
use gossip::GossipConfig;
use gossip::GossipReloadConfig;
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
use url::Url;

use crate::channel::NetBroadcastSender;
use crate::channel::NetDirectSender;
use crate::config::DirectSendMode;
use crate::config::NetworkConfig;
use crate::config::SocketAddrSet;
use crate::network::BasicNetwork;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::connection::MessageDelivery;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::CertFile;
use crate::pub_sub::CertStore;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PrivateKeyFile;
use crate::resolver::watch_gossip;
use crate::resolver::WatchGossipConfig;
use crate::topology::NetEndpoint;
use crate::topology::NetPeer;
use crate::topology::NetTopology;
use crate::DirectReceiver;

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

const ADDR_FIELDS_PER_INSTANCE: usize = 10;
fn make_instance_addr(instance: usize, field: usize) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], (11000 + instance * ADDR_FIELDS_PER_INSTANCE + field) as u16))
}

pub fn instance_addr(instance: usize) -> SocketAddr {
    make_instance_addr(instance, 0)
}

pub fn instance_gossip_addr(instance: usize) -> SocketAddr {
    make_instance_addr(instance, 1)
}
pub fn generate_owner_keys(
    count: usize,
) -> (Vec<transport_layer::SigningKey>, HashSet<transport_layer::VerifyingKey>) {
    let keys = (0..count)
        .map(|_| transport_layer::SigningKey::generate(&mut rand::thread_rng()))
        .collect::<Vec<_>>();
    let pubkeys = HashSet::from_iter(keys.iter().map(|x| x.verifying_key()));
    (keys, pubkeys)
}

fn network_config(
    instance: usize,
    owner_keys: &[transport_layer::SigningKey],
    trusted_pubkeys: HashSet<transport_layer::VerifyingKey>,
) -> NetworkConfig {
    let key_file = PrivateKeyFile::default();
    let cert_file = CertFile::default();
    NetworkConfig::new(
        instance_addr(instance),
        DirectSendMode::Broadcast,
        cert_file,
        key_file,
        owner_keys,
        CertStore::default(),
        trusted_pubkeys,
        None,
    )
    .unwrap()
}

fn gossip_config(instance: usize, seed_instances: &[usize]) -> GossipConfig {
    GossipConfig {
        advertise_addr: None,
        listen_addr: instance_gossip_addr(instance),
        seeds: seed_instances.iter().copied().map(instance_gossip_addr).collect(),
        cluster_id: "transport_test".to_string(),
        peer_ttl_seconds: default_gossip_peer_ttl_seconds(),
    }
}

#[derive(Clone)]
pub struct NodeConfig {
    pub(crate) id: String,
    network: NetworkConfig,
    gossip: GossipConfig,
    _owner_key: transport_layer::SigningKey,
    proxies: SocketAddrSet,
}

impl NodeConfig {
    pub fn new(
        instance: usize,
        owner_key: transport_layer::SigningKey,
        proxy_instances: &[usize],
        gossip_seed_instances: &[usize],
        trusted_pubkeys: HashSet<transport_layer::VerifyingKey>,
    ) -> Self {
        Self {
            id: instance.to_string(),
            network: network_config(instance, std::slice::from_ref(&owner_key), trusted_pubkeys),
            gossip: gossip_config(instance, gossip_seed_instances),
            _owner_key: owner_key,
            proxies: proxy_instances.iter().copied().map(instance_addr).collect(),
        }
    }

    fn peer_endpoint(&self) -> NetEndpoint<String> {
        NetEndpoint::Peer(NetPeer::with_id_and_addr(self.id.clone(), self.network.bind))
    }
}

#[derive(Clone)]
pub struct ProxyConfig {
    _my_addrs: SocketAddrSet,
    network: NetworkConfig,
    gossip: GossipConfig,
}

impl ProxyConfig {
    pub fn new(
        instance: usize,
        my_addrs: &[usize],
        gossip_seed_instances: &[usize],
        owner_keys: Vec<transport_layer::SigningKey>,
        trusted_pubkeys: HashSet<transport_layer::VerifyingKey>,
    ) -> Self {
        Self {
            _my_addrs: my_addrs.iter().copied().map(instance_addr).collect(),
            network: network_config(instance, &owner_keys, trusted_pubkeys),
            gossip: gossip_config(instance, gossip_seed_instances),
        }
    }
}

pub struct NetConfig {
    proxies: Vec<ProxyConfig>,
    nodes: Vec<NodeConfig>,
}

pub struct Net<Transport: NetTransport + 'static, Node> {
    proxies: Vec<Proxy<Transport>>,
    nodes: Vec<Node>,
}

impl NetConfig {
    pub fn with_nodes(node_infos: Vec<Vec<usize>>) -> Self {
        let mut nodes = Vec::new();
        let mut proxies = Vec::new();
        let mut next_proxy_instance = node_infos.len();
        let mut proxy_info_by_id =
            HashMap::<usize, (usize, Vec<transport_layer::SigningKey>)>::new();
        let (owner_keys, trusted_pubkeys) = generate_owner_keys(node_infos.len());
        let gossip_seed_instances = (0..node_infos.len()).take(3).collect::<Vec<_>>();
        for (node_instance, proxy_ids) in node_infos.into_iter().enumerate() {
            let mut proxy_instances = Vec::new();
            let owner_key = &owner_keys[node_instance];
            for proxy_id in proxy_ids {
                let proxy_instance = if let Some((proxy_instance, owner_pubkeys)) =
                    proxy_info_by_id.get_mut(&proxy_id)
                {
                    owner_pubkeys.push(owner_key.clone());
                    *proxy_instance
                } else {
                    proxy_info_by_id
                        .insert(proxy_id, (next_proxy_instance, vec![owner_key.clone()]));
                    next_proxy_instance += 1;
                    next_proxy_instance - 1
                };
                proxy_instances.push(proxy_instance);
            }
            nodes.push(NodeConfig::new(
                node_instance,
                owner_key.clone(),
                &proxy_instances,
                &gossip_seed_instances,
                trusted_pubkeys.clone(),
            ));
        }
        for (_, (instance, owner_keys)) in proxy_info_by_id.into_iter() {
            proxies.push(ProxyConfig::new(
                instance,
                &[instance],
                &gossip_seed_instances,
                owner_keys,
                trusted_pubkeys.clone(),
            ));
        }
        Self { nodes, proxies }
    }

    pub async fn start<Transport: NetTransport + 'static, Node, N>(
        &self,
        transport: Transport,
        start_node: impl Fn(Transport, NodeConfig) -> N,
    ) -> Net<Transport, Node>
    where
        N: Future<Output = Node>,
    {
        let mut proxies = Vec::new();
        for proxy_config in &self.proxies {
            proxies.push(Proxy::start(transport.clone(), proxy_config.clone()).await.unwrap());
        }
        let mut nodes = Vec::new();
        for node_config in &self.nodes {
            nodes.push(start_node(transport.clone(), node_config.clone()).await);
        }
        Net { proxies, nodes }
    }
}

pub struct NoChannelMetrics;

impl InstrumentedChannelMetrics for NoChannelMetrics {
    fn report_channel(&self, _channel: &'static str, _delta: isize) {}
}

pub struct Proxy<Transport: NetTransport + 'static> {
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub config_tx: tokio::sync::watch::Sender<ProxyConfig>,
    pub watch_gossip_config_tx: tokio::sync::watch::Sender<WatchGossipConfig<String>>,
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
    pub(crate) async fn start(transport: Transport, config: ProxyConfig) -> anyhow::Result<Self> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (config_tx, config_rx) = tokio::sync::watch::channel(config.clone());
        tracing::info!("Gossip advertise addr: {:?}", config.gossip.advertise_addr);

        let (network_config_tx, network_config_rx) =
            tokio::sync::watch::channel(config.network.clone());
        let (gossip_config_tx, gossip_config_rx) =
            tokio::sync::watch::channel(config.gossip.clone());
        tokio::spawn(split_proxy_config(
            shutdown_rx.clone(),
            config_rx,
            network_config_tx,
            gossip_config_tx,
        ));

        let (chitchat_handle, gossip_rest_handle) = gossip::run_gossip_no_reload(
            "proxy",
            shutdown_rx.clone(),
            gossip_config_rx,
            chitchat::transport::UdpTransport,
        )
        .await?;

        let (outgoing_messages_tx, _ /* we will subscribe() later */) =
            tokio::sync::broadcast::channel(100);
        let (incoming_messages_tx, incoming_messages_rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(Self::message_multiplexor(incoming_messages_rx, outgoing_messages_tx.clone()));

        let (watch_gossip_config_tx, watch_gossip_config_rx) =
            tokio::sync::watch::channel(WatchGossipConfig {
                endpoint: NetEndpoint::Proxy(SocketAddrSet::with_addr(config.network.bind)),
                max_nodes_with_same_id: 5,
                override_subscribe: vec![],
                peer_ttl_seconds: default_gossip_peer_ttl_seconds(),
                trusted_pubkeys: HashSet::new(),
            });
        let (topology_tx, topology_rx) = tokio::sync::watch::channel(NetTopology::default());
        tokio::spawn(watch_gossip(
            shutdown_rx.clone(),
            watch_gossip_config_rx,
            chitchat_handle.chitchat(),
            topology_tx.clone(),
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
            topology_rx,
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
        mut incoming_messages: tokio::sync::mpsc::UnboundedReceiver<IncomingMessage<String>>,
        outgoing_messages: tokio::sync::broadcast::Sender<OutgoingMessage<String>>,
    ) -> anyhow::Result<()> {
        tracing::info!("Proxy multiplexor bridge started");
        loop {
            match incoming_messages.recv().await {
                Some(incoming) => {
                    let label = incoming.message.label.clone();
                    tracing::debug!("Proxy multiplexor forwarded incoming {}", label);
                    if let Ok(_sent_count) = outgoing_messages.send(OutgoingMessage {
                        delivery: MessageDelivery::BroadcastExcludingSender(
                            incoming.connection_info,
                        ),
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

pub async fn split_node_config(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut config_rx: tokio::sync::watch::Receiver<NodeConfig>,
    network_config_tx: tokio::sync::watch::Sender<NetworkConfig>,
    gossip_reload_config_tx: tokio::sync::watch::Sender<GossipReloadConfig<String>>,
    watch_gossip_config_tx: tokio::sync::watch::Sender<WatchGossipConfig<String>>,
) {
    loop {
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                return;
            },
            sender = config_rx.changed() => if sender.is_err() {
                return;
            } else {
                let config = config_rx.borrow();
                network_config_tx.send_replace(config.network.clone());
                watch_gossip_config_tx.send_replace(WatchGossipConfig {
                    endpoint: config.peer_endpoint(),
                    max_nodes_with_same_id: 6,
                    override_subscribe: Vec::new(),
                    peer_ttl_seconds: default_gossip_peer_ttl_seconds(),
                    trusted_pubkeys: config.network.credential.trusted_pubkeys.clone(),
                });
                gossip_reload_config_tx.send_replace(
                    GossipReloadConfig {
                        gossip_config: config.gossip.clone(),
                        my_ed_key_secret: vec![],
                        my_ed_key_path: vec![],
                        peer_config: Some(GossipPeer {
                            id: config.id.clone(),
                            node_protocol_addr: config.network.bind,
                            proxies: vec![],
                            bm_api_addr: None,
                            bk_api_host_port: None,
                            bk_api_url_for_storage_sync: Some(Url::parse(&format!("http://{}", config.network.bind)).unwrap()),
                            bk_api_addr_deprecated: None,
                            pubkey_signature: None,
                        })
                    }
                );
            }
        }
    }
}

pub async fn split_proxy_config(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut config_rx: tokio::sync::watch::Receiver<ProxyConfig>,
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
    pub transport: Transport,
    pub(crate) state: Arc<NodeState>,
    pub(crate) direct_tx: NetDirectSender<String, Message>,
    pub(crate) broadcast_tx: NetBroadcastSender<String, Message>,
    pub(crate) incoming_rx: Option<InstrumentedReceiver<IncomingMessage<String>>>,
    pub(crate) net_topology_rx: tokio::sync::watch::Receiver<NetTopology<String>>,
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

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum Message {
    Data(String, u64, Vec<u8>),
    Ack(u64),
}

impl Message {
    pub fn data<const N: usize>(sender: &str, id: u64, data: [u8; N]) -> Self {
        Self::Data(sender.to_string(), id, data.to_vec())
    }
}

impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Data(sender, id, data) => {
                write!(f, "Data({} bytes, sender:{}, id:{})", data.len(), sender, id)
            }
            Message::Ack(id) => write!(f, "Ack({id})"),
        }
    }
}

impl<Transport: NetTransport + 'static> Drop for Node<Transport> {
    fn drop(&mut self) {
        self.shutdown_tx.send_replace(true);
    }
}

impl<Transport: NetTransport + 'static> Node<Transport> {
    pub async fn start(config: NodeConfig, transport: Transport) -> anyhow::Result<Self> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (config_tx, config_rx) = tokio::sync::watch::channel(config);
        let config = config_rx.borrow().clone();
        let (network_config_tx, network_config_rx) =
            tokio::sync::watch::channel(config.network.clone());
        let (watch_gossip_config_tx, watch_gossip_config_rx) =
            tokio::sync::watch::channel(WatchGossipConfig {
                endpoint: config.peer_endpoint(),
                max_nodes_with_same_id: 5,
                override_subscribe: Vec::new(),
                peer_ttl_seconds: default_gossip_peer_ttl_seconds(),
                trusted_pubkeys: HashSet::new(),
            });
        tracing::info!("Gossip advertise addr: {:?}", config.gossip.advertise_addr);
        let (gossip_reload_config_tx, gossip_reload_config_rx) =
            tokio::sync::watch::channel(GossipReloadConfig {
                gossip_config: config.gossip.clone(),
                my_ed_key_secret: vec![],
                my_ed_key_path: vec![],
                peer_config: None,
            });
        tokio::spawn(split_node_config(
            shutdown_rx.clone(),
            config_rx.clone(),
            network_config_tx,
            gossip_reload_config_tx,
            watch_gossip_config_tx,
        ));

        let (chitchat_rx, _) = run_gossip_with_reload(
            "node",
            shutdown_rx.clone(),
            gossip_reload_config_rx,
            chitchat::transport::UdpTransport,
        )
        .await?;

        let chitchat_ref =
            chitchat_rx.borrow().clone().expect("ChitchatRef is guaranteed to have Some value");

        let gossip_node = GossipPeer {
            id: config.id.clone(),
            node_protocol_addr: config.network.bind,
            proxies: config.proxies.clone().into(),
            bm_api_addr: None,
            bk_api_host_port: None,
            bk_api_url_for_storage_sync: Some(Url::parse(&format!(
                "http://{}",
                config.network.bind
            ))?),
            bk_api_addr_deprecated: None,
            pubkey_signature: None,
        };

        {
            let mut chitchat = chitchat_ref.lock();
            gossip_node.update_node_state(chitchat.self_node_state(), &[], &[]);
        }
        let network = BasicNetwork::new(shutdown_tx.clone(), network_config_rx, transport.clone());

        let (direct_tx, broadcast_tx, incoming_rx, net_topology_rx) = network
            .start(
                watch_gossip_config_rx,
                None,
                Option::<NoChannelMetrics>::None,
                config.id,
                config.network.bind,
                false,
                chitchat_rx.clone(),
            )
            .await?;

        let state = Arc::new(NodeState::default());

        Ok(Self {
            shutdown_tx,
            config_tx,
            transport,
            state,
            direct_tx,
            broadcast_tx,
            incoming_rx: Some(incoming_rx),
            net_topology_rx,
        })
    }
}

pub trait NodeWrapper<Transport: NetTransport + 'static> {
    fn node(&self) -> &Node<Transport>;

    fn broadcast(&self, message: Message) {
        self.node().broadcast_tx.send(message).unwrap();
    }

    fn send_direct(&self, receiver: String, message: Message) {
        self.node().direct_tx.send((DirectReceiver::Peer(receiver), message)).unwrap();
    }

    fn print_stat(&self, node_count: usize) {
        let state = &*self.node().state;
        let unconfirmed = state.unconfirmed.lock().unwrap().values().sum::<usize>();
        if unconfirmed <= node_count {
            return;
        }
        let data_sent = state.data_sent.load(Ordering::Relaxed);
        let data_received = state.data_received.load(Ordering::Relaxed);
        let ack_sent = state.ack_sent.load(Ordering::Relaxed);
        let ack_received = state.ack_received.load(Ordering::Relaxed);
        println!(
            "Node count: {node_count}, Unconfirmed: {unconfirmed}, Data sent: {data_sent}, Data received: {data_received}, Ack sent: {ack_sent}, Ack received: {ack_received}"
        );
    }
}

impl<Transport: NetTransport + 'static> NodeWrapper<Transport> for Node<Transport> {
    fn node(&self) -> &Node<Transport> {
        self
    }
}
