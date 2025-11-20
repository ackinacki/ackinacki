use std::sync::Arc;
use std::time::Duration;

use telemetry_utils::mpsc::InstrumentedReceiver;
use transport_layer::msquic::MsQuicTransport;

use crate::pub_sub::connection::IncomingMessage;
use crate::tests::init_logs;
use crate::tests::Message;
use crate::tests::NetConfig;
use crate::tests::Node;
use crate::tests::NodeConfig;
use crate::tests::NodeWrapper;

pub struct TestNode {
    config: NodeConfig,
    node: Node<MsQuicTransport>,
    received_messages: Arc<std::sync::Mutex<Vec<Message>>>,
    _receiver_task: std::thread::JoinHandle<()>,
}

impl NodeWrapper<MsQuicTransport> for TestNode {
    fn node(&self) -> &Node<MsQuicTransport> {
        &self.node
    }
}

impl TestNode {
    async fn start(transport: MsQuicTransport, config: NodeConfig) -> Self {
        let receiver = config.id.clone();
        let mut node = Node::start(config.clone(), transport).await.unwrap();
        let received_messages = Arc::new(std::sync::Mutex::new(vec![]));
        let received_messages_clone = received_messages.clone();
        let incoming_rx = node.incoming_rx.take().unwrap();
        let receiver_task = std::thread::Builder::new()
            .spawn(move || Self::run_receiver(receiver, incoming_rx, received_messages_clone))
            .unwrap();
        Self { config, node, received_messages, _receiver_task: receiver_task }
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
async fn test_direct_broadcast() {
    init_logs();
    let transport = MsQuicTransport::new();
    let net = NetConfig::with_nodes(vec![vec![], vec![], vec![]])
        .start(transport.clone(), TestNode::start)
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let msg = Message::data("1", 1, [1, 1, 1]);
    net.nodes[0].send_direct(net.nodes[1].config.id.clone(), msg.clone());
    tokio::time::sleep(Duration::from_secs(1)).await;
    for (i, node) in net.nodes.iter().enumerate() {
        if i == 1 {
            assert_eq!(node.received_messages(), vec![msg.clone(), msg.clone()], "node{i}");
        } else {
            assert_eq!(node.received_messages(), vec![], "node{i}");
        }
    }
    drop(net);
    tokio::time::sleep(Duration::from_secs(1)).await;
}
