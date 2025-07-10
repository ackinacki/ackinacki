use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use chitchat::ChitchatRef;
use itertools::Itertools;
use rand::RngCore;
use telemetry_utils::mpsc::InstrumentedReceiver;
use transport_layer::msquic::MsQuicTransport;
use transport_layer::NetTransport;

use crate::channel::NetBroadcastSender;
use crate::channel::NetDirectSender;
use crate::network::PeerData;
use crate::pub_sub::connection::IncomingMessage;
use crate::tests::gossip_addr;
use crate::tests::init_logs;
use crate::tests::node_addr;
use crate::tests::Message;
use crate::tests::Node;
use crate::tests::NodeConfig;
use crate::tests::NodeState;
use crate::tests::Proxy;

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
        let addr = gossip_addr(node_index);
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
            let node_addr = node_addr(host_index);
            let gossip_addr = gossip_addr(host_index);
            dc_proxies.push(node_addr);
            host_index += 1;
            let config = NodeConfig::new(
                node_addr,
                gossip_addr,
                format!("proxy_{dc_index}_{dc_proxy_index}"),
                gossip_seeds.clone(),
            );
            proxies.push(Proxy::start(config, transport.clone()).await.unwrap());
        }

        for dc_node_index in 0..dc_node_count {
            let node_addr = node_addr(host_index);
            let gossip_addr = gossip_addr(host_index);
            host_index += 1;
            let config = NodeConfig::new(
                node_addr,
                gossip_addr,
                format!("node_{dc_index}_{dc_node_index}"),
                gossip_seeds.clone(),
            );
            nodes.push(NodeTest::start(config, transport.clone(), total_node_count).await.unwrap());
        }
    }

    for sec in 0..6 {
        println!("\n=== {sec} ===\n");
        for node in &nodes {
            node.node.print_stat(total_node_count);
        }
        for proxy in &proxies {
            proxy.print_stat();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

struct NodeTest<Transport: NetTransport + 'static> {
    node: Node<Transport>,
    _producer_task: std::thread::JoinHandle<anyhow::Result<()>>,
    _consumer_task: std::thread::JoinHandle<anyhow::Result<()>>,
}

impl<Transport: NetTransport + 'static> NodeTest<Transport> {
    async fn start(
        config: NodeConfig,
        transport: Transport,
        node_count: usize,
    ) -> anyhow::Result<Self> {
        let node_id = config.node_id.clone();
        let (node, channels) = Node::start(config, transport).await?;

        let state_clone = node.state.clone();
        let chitchat_clone = node.chitchat.clone();
        let producer_task = std::thread::Builder::new().spawn(move || {
            Self::run_producer(
                state_clone,
                chitchat_clone,
                node_count,
                node_id,
                channels.broadcast_tx,
            )
        })?;
        let state_clone = node.state.clone();
        let consumer_task = std::thread::Builder::new().spawn(move || {
            Self::run_consumer(
                state_clone,
                channels.direct_tx,
                channels.incoming_rx,
                channels.peers_rx,
            )
        })?;
        Ok(Self { node, _producer_task: producer_task, _consumer_task: consumer_task })
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
        _peers_rx: tokio::sync::watch::Receiver<HashMap<String, PeerData>>,
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
}
