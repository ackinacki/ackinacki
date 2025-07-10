use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use telemetry_utils::mpsc::InstrumentedReceiver;
use transport_layer::msquic::MsQuicTransport;

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

pub struct HotReloadNode {
    _node: Node<MsQuicTransport>,
    _direct_tx: NetDirectSender<String, Message>,
    broadcast_tx: NetBroadcastSender<Message>,
    _peers_rx: tokio::sync::watch::Receiver<HashMap<String, PeerData>>,
    received_messages: Arc<std::sync::Mutex<Vec<Message>>>,
    _receiver_task: std::thread::JoinHandle<()>,
}

impl HotReloadNode {
    async fn start(transport: MsQuicTransport, config: NodeConfig) -> Self {
        let receiver = config.node_id.clone();
        let (node, channels) = Node::start(config, transport).await.unwrap();
        let received_messages = Arc::new(std::sync::Mutex::new(vec![]));
        let received_messages_clone = received_messages.clone();
        let incoming_rx = channels.incoming_rx;
        let receiver_task = std::thread::Builder::new()
            .spawn(move || Self::run_receiver(receiver, incoming_rx, received_messages_clone))
            .unwrap();
        Self {
            _node: node,
            received_messages,
            _direct_tx: channels.direct_tx,
            broadcast_tx: channels.broadcast_tx,
            _peers_rx: channels.peers_rx,
            _receiver_task: receiver_task,
        }
    }

    fn received_messages(&self) -> Vec<Message> {
        self.received_messages.lock().unwrap().clone()
    }

    fn run_receiver(
        receiver: String,
        message_rx: InstrumentedReceiver<IncomingMessage>,
        received_messages: Arc<std::sync::Mutex<Vec<Message>>>,
    ) {
        while let Ok(message) = message_rx.recv() {
            if let Some(message) = message.finish(&None) {
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
    let mut nodes = Vec::new();
    for i in 0..node_count {
        let config = NodeConfig::new(
            node_addr(i),
            gossip_addr(i),
            format!("node{i}"),
            vec![gossip_addr(0), gossip_addr(1)],
        );
        nodes.push(HotReloadNode::start(transport.clone(), config).await);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    nodes[0].broadcast_tx.send(Message::Data("1".to_string(), 1, vec![1, 1, 1])).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(nodes[0].received_messages().len(), 0);
    for node in &nodes[1..node_count] {
        assert_eq!(node.received_messages().len(), 1);
    }
    drop(nodes);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // nodes[0].node.config_tx.send_modify(|config| {
    //     config.network.bind = node_addr(10);
    // });
}
