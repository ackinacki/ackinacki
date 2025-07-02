use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use chitchat::ChitchatHandle;
use chitchat::ChitchatRef;
use itertools::Itertools;
use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use network::config::NetworkConfig;
use network::network::BasicNetwork;
use network::network::PeerData;
use network::pub_sub::connection::IncomingMessage;
use network::pub_sub::connection::MessageDelivery;
use network::pub_sub::connection::OutgoingMessage;
use network::pub_sub::CertFile;
use network::pub_sub::CertStore;
use network::pub_sub::IncomingSender;
use network::pub_sub::PrivateKeyFile;
use network::resolver::watch_gossip;
use network::resolver::GossipPeer;
use network::resolver::SubscribeStrategy;
use once_cell::sync::OnceCell;
use rand::RngCore;
use serde::Deserialize;
use serde::Serialize;
use telemetry_utils::mpsc::InstrumentedChannelMetrics;
use telemetry_utils::mpsc::InstrumentedReceiver;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;
use transport_layer::msquic::MsQuicTransport;
use transport_layer::NetTransport;

static LOG_INIT: OnceCell<()> = OnceCell::new();

fn init_logs() {
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
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_msquic_transport() {
    test_transport(MsQuicTransport::new()).await;
}

async fn test_transport(transport: impl NetTransport + 'static) {
    init_logs();
    let dc_count = 2;
    let dc_proxy_count = 0;
    let dc_node_count = 2;

    let total_node_count = dc_count * dc_node_count;

    let seed_indexes = [0, 1, 2, 3, 4];
    // `let seed_nodes = 0..node_count;`

    let mut gossip_seeds = Vec::new();
    for node_index in 0..total_node_count {
        let addr = Addrs::new(node_index).gossip;
        if seed_indexes.contains(&node_index) {
            gossip_seeds.push(addr);
        }
    }

    let mut nodes = Vec::new();
    let mut proxies = Vec::new();
    let mut host_index = 0;
    for dc_index in 0..dc_count {
        let mut dc_proxies = vec![];
        for dc_proxy_index in 0..dc_proxy_count {
            let addrs = Addrs::new(host_index);
            dc_proxies.push(addrs.node);
            host_index += 1;
            let config = NodeConfig::new(
                addrs,
                format!("proxy_{dc_index}_{dc_proxy_index}"),
                gossip_seeds.clone(),
            );
            proxies.push(Proxy::start(config, transport.clone()).await.unwrap());
        }

        for dc_node_index in 0..dc_node_count {
            let addrs = Addrs::new(host_index);
            host_index += 1;
            let config = NodeConfig::new(
                addrs,
                format!("node_{dc_index}_{dc_node_index}"),
                gossip_seeds.clone(),
            );
            nodes.push(Node::start(config, transport.clone(), total_node_count).await.unwrap());
        }
    }

    for sec in 0..60 {
        println!("\n=== {sec} ===\n");
        for node in &nodes {
            node.print_stat(total_node_count);
        }
        for proxy in &proxies {
            proxy.print_stat();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

struct Addrs {
    node: SocketAddr,
    gossip: SocketAddr,
}

impl Addrs {
    fn new(index: usize) -> Self {
        let ip = [127, 0, 0, 1];
        let port = (11000 + index * 10) as u16;
        Self { node: SocketAddr::from((ip, port)), gossip: SocketAddr::from((ip, port + 1)) }
    }
}

struct NodeConfig {
    node_id: String,
    network: NetworkConfig,
    gossip_addr: SocketAddr,
    gossip_seeds: Vec<SocketAddr>,
    gossip_cluster_id: String,
}

impl NodeConfig {
    fn new(addrs: Addrs, node_id: String, gossip_seeds: Vec<SocketAddr>) -> Self {
        let key_file = PrivateKeyFile::debug().unwrap();
        let cert_file = CertFile::debug().unwrap();
        Self {
            node_id,
            network: NetworkConfig {
                bind: addrs.node,
                my_cert: cert_file,
                my_key: key_file,
                peer_certs: CertStore::debug().unwrap(),
                subscribe: vec![],
                proxies: vec![],
            },
            gossip_addr: addrs.gossip,
            gossip_seeds,
            gossip_cluster_id: "transport_test".to_string(),
        }
    }
}
struct Node<Transport: NetTransport> {
    _transport: Transport,
    _producer_task: std::thread::JoinHandle<anyhow::Result<()>>,
    _consumer_task: std::thread::JoinHandle<anyhow::Result<()>>,
    chitchat: ChitchatRef,
    state: Arc<NodeState>,
    _chitchat_handle: ChitchatHandle,
    _gossip_rest_handle: JoinHandle<anyhow::Result<()>>,
}

struct NodeState {
    data_sent: AtomicUsize,
    data_received: AtomicUsize,
    ack_sent: AtomicUsize,
    ack_received: AtomicUsize,
    unconfirmed: std::sync::Mutex<HashMap<u64, usize>>,
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
enum Message {
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
struct NoChannelMetrics;

impl InstrumentedChannelMetrics for NoChannelMetrics {
    fn report_channel(&self, _channel: &'static str, _delta: isize) {}
}

impl<Transport: NetTransport + 'static> Node<Transport> {
    async fn start(
        config: NodeConfig,
        transport: Transport,
        node_count: usize,
    ) -> anyhow::Result<Self> {
        let gossip_advertise_addr = config.gossip_addr;
        tracing::info!("Gossip advertise addr: {:?}", gossip_advertise_addr);

        let (chitchat_handle, gossip_rest_handle) = gossip::run(
            config.gossip_addr,
            chitchat::transport::UdpTransport,
            gossip_advertise_addr,
            config.gossip_seeds.iter().map(|x| x.to_string()).collect(),
            config.gossip_cluster_id.clone(),
        )
        .await?;

        let gossip_node = GossipPeer::new(
            config.node_id.clone(),
            config.network.bind,
            config.network.proxies.clone(),
            None,
            None,
        );
        chitchat_handle
            .with_chitchat(|c| {
                gossip_node.set_to(c.self_node_state());
            })
            .await;

        let network = BasicNetwork::from(transport.clone(), config.network.clone());
        let chitchat = chitchat_handle.chitchat();

        let (direct_tx, broadcast_tx, incoming_rx, nodes_rx) = network
            .start(
                None,
                Option::<NoChannelMetrics>::None,
                config.node_id.clone(),
                false,
                chitchat.clone(),
            )
            .await?;

        let state = Arc::new(NodeState::default());

        let state_clone = state.clone();
        let chitchat_clone = chitchat.clone();
        let producer_task = std::thread::Builder::new().spawn(move || {
            Self::run_producer(
                state_clone,
                chitchat_clone,
                node_count,
                config.node_id.clone(),
                broadcast_tx,
            )
        })?;
        let state_clone = state.clone();
        let consumer_task = std::thread::Builder::new()
            .spawn(move || Self::run_consumer(state_clone, direct_tx, incoming_rx, nodes_rx))?;
        Ok(Self {
            _producer_task: producer_task,
            _consumer_task: consumer_task,
            _transport: transport,
            _chitchat_handle: chitchat_handle,
            _gossip_rest_handle: gossip_rest_handle,
            chitchat,
            state,
        })
    }

    fn run_producer(
        state: Arc<NodeState>,
        chitchat: ChitchatRef,
        node_count: usize,
        node_id: String,
        broadcast_tx: NetBroadcastSender<Message>,
    ) -> anyhow::Result<()> {
        tracing::info!("Producer started");
        let mut id = 1;
        let mut started = false;
        std::thread::sleep(Duration::from_secs(3));
        loop {
            if started {
                let mut data = vec![0u8; 150_000];
                rand::thread_rng().fill_bytes(&mut data);
                let Ok(_) = broadcast_tx.send(Message::Data(node_id.clone(), id, data)) else {
                    break;
                };
                match state.unconfirmed.lock() {
                    Ok(mut x) => {
                        x.insert(id, node_count - 1);
                    }
                    Err(_) => break,
                }
                state.data_sent.fetch_add(node_count - 1, Ordering::Relaxed);
                id += 1;
            } else {
                let live_node_count = chitchat.lock().live_nodes().try_len().unwrap_or_default();
                if live_node_count >= node_count {
                    println!("All nodes are live. Starting producer");
                    started = true;
                }
            }
            std::thread::sleep(Duration::from_millis(300));
        }
        tracing::info!("Producer finished");
        Ok(())
    }

    fn run_consumer(
        state: Arc<NodeState>,
        direct_tx: NetDirectSender<String, Message>,
        incoming_rx: InstrumentedReceiver<IncomingMessage>,
        _nodes_rx: Receiver<HashMap<String, PeerData>>,
    ) -> anyhow::Result<()> {
        tracing::info!("Consumer started");
        while let Ok(message) = incoming_rx.recv() {
            let message = message.finish::<Message>(&None).unwrap();
            match message {
                Message::Data(sender, id, _data) => {
                    state.data_received.fetch_add(1, Ordering::Relaxed);
                    direct_tx.send((sender, Message::Ack(id)))?;
                    state.ack_sent.fetch_add(1, Ordering::Relaxed);
                }
                Message::Ack(id) => {
                    state.ack_received.fetch_add(1, Ordering::Relaxed);
                    match state.unconfirmed.lock() {
                        Ok(mut unconfirmed) => {
                            let should_remove = if let Some(count) = unconfirmed.get_mut(&id) {
                                *count = count.saturating_sub(1);
                                *count == 0
                            } else {
                                false
                            };
                            if should_remove {
                                unconfirmed.remove(&id);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        tracing::info!("Consumer finished");
        Ok(())
    }

    fn print_stat(&self, node_count: usize) {
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

struct Proxy<Transport: NetTransport> {
    _transport: Transport,
    chitchat: ChitchatRef,
    _chitchat_handle: ChitchatHandle,
    _gossip_rest_handle: JoinHandle<anyhow::Result<()>>,
}

impl<Transport: NetTransport + 'static> Proxy<Transport> {
    async fn start(config: NodeConfig, transport: Transport) -> anyhow::Result<Self> {
        let gossip_advertise_addr = config.gossip_addr;
        tracing::info!("Gossip advertise addr: {:?}", gossip_advertise_addr);

        let (chitchat_handle, gossip_rest_handle) = gossip::run(
            config.gossip_addr,
            chitchat::transport::UdpTransport,
            gossip_advertise_addr,
            config.gossip_seeds.iter().map(|x| x.to_string()).collect(),
            config.gossip_cluster_id.clone(),
        )
        .await?;

        let (outgoing_messages_tx, _ /* we will subscribe() later */) =
            tokio::sync::broadcast::channel(100);
        let (incoming_messages_tx, incoming_messages_rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn({
            let outgoing_messages_sender = outgoing_messages_tx.clone();
            async move {
                Self::message_multiplexor(incoming_messages_rx, outgoing_messages_sender).await
            }
        });

        let (subscribe_tx, _) = tokio::sync::watch::channel(Vec::new());
        tokio::spawn(watch_gossip(
            SubscribeStrategy::<String>::Proxy(config.network.bind),
            chitchat_handle.chitchat(),
            Some(subscribe_tx.clone()),
            None,
            None,
        ));

        let transport_clone = transport.clone();
        tokio::spawn(async move {
            network::pub_sub::run(
                transport_clone,
                true,
                None,
                10000,
                config.network.bind,
                config.network.tls_config(),
                subscribe_tx,
                outgoing_messages_tx,
                IncomingSender::AsyncUnbounded(incoming_messages_tx),
            )
            .await
        });

        let chitchat = chitchat_handle.chitchat();
        Ok(Self {
            _transport: transport,
            _chitchat_handle: chitchat_handle,
            _gossip_rest_handle: gossip_rest_handle,
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

    fn print_stat(&self) {
        let live_nodes = self.chitchat.lock().live_nodes().try_len().unwrap_or_default();
        println!("LiveNodes: {live_nodes}");
    }
}
