use std::sync::Arc;
use std::time::Duration;

use telemetry_utils::mpsc::InstrumentedReceiver;
use transport_layer::msquic::MsQuicTransport;

use crate::pub_sub::connection::IncomingMessage;
use crate::tests::init_logs;
use crate::tests::instance_addr;
use crate::tests::Message;
use crate::tests::Net;
use crate::tests::NetConfig;
use crate::tests::Node;
use crate::tests::NodeConfig;
use crate::tests::NodeWrapper;

pub struct HotReloadNode {
    node: Node<MsQuicTransport>,
    received_messages: Arc<std::sync::Mutex<Vec<Message>>>,
    _receiver_task: std::thread::JoinHandle<()>,
}

impl NodeWrapper<MsQuicTransport> for HotReloadNode {
    fn node(&self) -> &Node<MsQuicTransport> {
        &self.node
    }
}

impl HotReloadNode {
    async fn start(transport: MsQuicTransport, config: NodeConfig) -> Self {
        let receiver = config.id.clone();
        let mut node = Node::start(config, transport).await.unwrap();
        let received_messages = Arc::new(std::sync::Mutex::new(vec![]));
        let received_messages_clone = received_messages.clone();
        let incoming_rx = node.incoming_rx.take().unwrap();
        let receiver_task = std::thread::Builder::new()
            .spawn(move || Self::run_receiver(receiver, incoming_rx, received_messages_clone))
            .unwrap();
        Self { node, received_messages, _receiver_task: receiver_task }
    }

    fn received_messages(&self) -> Vec<Message> {
        self.received_messages.lock().unwrap().clone()
    }

    fn run_receiver(
        receiver: String,
        message_rx: InstrumentedReceiver<IncomingMessage<String>>,
        received_messages: Arc<std::sync::Mutex<Vec<Message>>>,
    ) {
        while let Ok(message) = message_rx.recv() {
            if let Some((message, _)) = message.finish(&None) {
                tracing::trace!("Message received by {receiver}: {:?}", message);
                received_messages.lock().unwrap().push(message);
            }
        }
        tracing::trace!("Receiver task for {receiver} finished");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_hot_reload() {
    init_logs();
    let transport = MsQuicTransport::new();
    let node_count = 2;
    let mut net_config = NetConfig::with_nodes(vec![vec![], vec![]]);
    let net = net_config.start(transport.clone(), HotReloadNode::start).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    net.nodes[0].broadcast(Message::Data("1".to_string(), 1, vec![1, 1, 1]));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(net.nodes[0].received_messages().len(), 0);
    for node in &net.nodes[1..node_count] {
        assert_eq!(node.received_messages().len(), 1);
    }

    // Check direct message
    net.nodes[1].node.send_direct(net_config.nodes[0].id.clone(), Message::Ack(1));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(net.nodes[0].received_messages().len(), 2);

    tracing::info!("Change node 0 advertise addr");
    net_config.nodes[0].network.bind = instance_addr(node_count + 1);
    net.nodes[0].node.config_tx.send_replace(net_config.nodes[0].clone());
    tokio::time::sleep(Duration::from_secs(3)).await;

    tracing::info!("Check direct message after changing addr");
    net.nodes[1].node.send_direct(net_config.nodes[0].id.clone(), Message::Ack(1));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(net.nodes[0].received_messages().len(), 3);

    tracing::info!("Shutdown test");
    drop(net);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // nodes[0].node.config_tx.send_modify(|config| {
    //     config.network.bind = node_addr(10);
    // });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_gossip_hot_reload() {
    init_logs();
    let transport = MsQuicTransport::new();
    let node_count = 2;
    let mut config = NetConfig::with_nodes(vec![vec![], vec![]]);
    let net = config.start(transport, HotReloadNode::start).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    net.nodes[0].broadcast(Message::data("Broadcast from 0", 1, [1]));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(received(&net), vec![0, 1], "Broadcast from 0");

    net.nodes[1]
        .send_direct(config.nodes[0].id.clone(), Message::data("Direct from 1 to 0", 2, [2]));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(received(&net), vec![1, 1], "Direct from 1 to 0");

    tracing::info!("Change node 0 advertise addr");
    config.nodes[0].network.bind = instance_addr(node_count + 1);
    net.nodes[0].node.config_tx.send_replace(config.nodes[0].clone());
    tokio::time::sleep(Duration::from_secs(3)).await;

    net.nodes[0].broadcast(Message::data("Broadcast from 0 after hot reload", 3, [3]));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(received(&net), vec![1, 2], "Broadcast from 0 after hot reload");

    net.nodes[1].send_direct(
        config.nodes[0].id.clone(),
        Message::data("Direct from 1 to 0 after hot reload", 4, [4]),
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(received(&net), vec![2, 2], "Direct from 1 to 0 after hot reload");

    net.nodes[1].broadcast(Message::data("Broadcast from 1 after hot reload", 5, [5]));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(received(&net), vec![3, 2], "Broadcast from 1 after hot reload");

    tracing::info!("Check other direct message after hot reload");
    net.nodes[0].send_direct(
        config.nodes[1].id.clone(),
        Message::data("Direct from 0 to 1 after hot reload", 6, [6]),
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(received(&net), vec![3, 3], "Direct from 0 to 1 after hot reload");

    tracing::info!("Shutdown test");
    drop(net);
    tokio::time::sleep(Duration::from_secs(1)).await;
}

fn received(net: &Net<MsQuicTransport, HotReloadNode>) -> Vec<usize> {
    net.nodes.iter().map(|n| n.received_messages().len()).collect()
}
