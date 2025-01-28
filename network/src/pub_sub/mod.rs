mod config;
mod executor;
mod receiver;
mod sender;
mod server;
mod subscribe;
mod sync_bridge;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

pub use config::CertFile;
pub use config::CertStore;
pub use config::Config;
pub use config::PrivateKeyFile;
pub use executor::run;
pub use sync_bridge::incoming_bridge;
pub use sync_bridge::outgoing_bridge;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use url::Url;
use wtransport::endpoint::ConnectOptions;
use wtransport::error::ConnectionError;
use wtransport::tls::Sha256Digest;
use wtransport::Connection;
use wtransport::Endpoint;

use crate::detailed;
use crate::tls::create_client_config;

pub const ACKI_NACKI_SUBSCRIBE_HEADER: &str = "AckiNacki-Subscribe";

pub struct ConnectionWrapper {
    pub url: Option<Url>,
    pub peer_id: Sha256Digest,
    pub connection: Connection,
    pub self_is_subscriber: bool,
    pub peer_is_subscriber: bool,
}

impl ConnectionWrapper {
    pub fn new(
        is_debug: bool,
        url: Option<Url>,
        peer_id: Sha256Digest,
        connection: Connection,
        self_is_subscriber: bool,
        peer_is_subscriber: bool,
    ) -> Self {
        let peer_id = if is_debug {
            let mut bytes = [0u8; 32];
            let addr = connection.remote_address().to_string();
            let addr_bytes = addr.as_bytes();
            let len = 32.min(addr_bytes.len());
            bytes[0..len].copy_from_slice(&addr_bytes[0..len]);
            Sha256Digest::new(bytes)
        } else {
            peer_id
        };
        Self { url, peer_id, connection, self_is_subscriber, peer_is_subscriber }
    }

    pub fn addr(&self) -> String {
        if let Some(url) = &self.url {
            url.to_string()
        } else {
            self.connection.remote_address().to_string()
        }
    }

    pub fn peer_info(&self) -> String {
        let mut info = self.addr();
        if self.self_is_subscriber {
            info.push_str(" (publisher)");
        }
        if self.peer_is_subscriber {
            info.push_str(" (subscriber)");
        }
        info
    }

    pub fn allow_sending(&self, message: &OutgoingMessage) -> bool {
        match &message.delivery {
            MessageDelivery::Broadcast => self.peer_is_subscriber,
            MessageDelivery::BroadcastExcluding(excluding) => {
                self.peer_is_subscriber && self.peer_id != *excluding
            }
            MessageDelivery::Url(url) => self.url.as_ref().map(|x| x == url).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub peer: Sha256Digest,
    pub data: Arc<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum MessageDelivery {
    Broadcast,
    BroadcastExcluding(Sha256Digest),
    Url(Url),
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub delivery: MessageDelivery,
    pub data: Arc<Vec<u8>>,
}

#[derive(Clone)]
pub struct PubSub(Arc<parking_lot::RwLock<PubSubInner>>);

pub struct PubSubInner {
    connections: HashMap<usize, Arc<ConnectionWrapper>>,
    tasks: HashMap<usize, Arc<JoinHandle<anyhow::Result<()>>>>,
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSub {
    pub fn new() -> PubSub {
        PubSub(Arc::new(parking_lot::RwLock::new(PubSubInner {
            connections: HashMap::new(),
            tasks: HashMap::new(),
        })))
    }

    pub fn schedule_subscriptions(
        &self,
        config: &Config,
    ) -> (Vec<Url>, Vec<Arc<ConnectionWrapper>>) {
        let inner = self.0.read();
        let mut should_be_unsubscribed = Vec::new();
        let mut should_be_subscribed = config.subscribe.iter().cloned().collect::<HashSet<_>>();
        for connection in inner.connections.values() {
            if let (true, Some(url)) = (connection.self_is_subscriber, &connection.url) {
                if !should_be_subscribed.remove(url) {
                    should_be_unsubscribed.push(connection.clone());
                }
            }
        }
        (should_be_subscribed.into_iter().collect(), should_be_unsubscribed)
    }

    pub async fn connect_to_peer(
        &self,
        incoming_messages: &mpsc::Sender<IncomingMessage>,
        outgoing_messages: &broadcast::Sender<OutgoingMessage>,
        connection_closed: &mpsc::Sender<Arc<ConnectionWrapper>>,
        config: &Config,
        url: &Url,
        subscribe: bool,
    ) -> anyhow::Result<()> {
        let client_config = create_client_config(config)?;
        let mut connect_options = ConnectOptions::builder(url.clone());
        if subscribe {
            connect_options = connect_options.add_header(ACKI_NACKI_SUBSCRIBE_HEADER, "true");
        }

        self.add_connection_handler(
            config.my_cert.is_debug(),
            incoming_messages,
            outgoing_messages,
            connection_closed,
            Endpoint::client(client_config)?.connect(connect_options.build()).await?,
            Some(url.clone()),
            subscribe,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_connection_handler(
        &self,
        is_debug: bool,
        incoming_messages: &mpsc::Sender<IncomingMessage>,
        outgoing_messages: &broadcast::Sender<OutgoingMessage>,
        connection_closed_tx: &mpsc::Sender<Arc<ConnectionWrapper>>,
        connection: Connection,
        url: Option<Url>,
        self_is_subscriber: bool,
        peer_is_subscriber: bool,
    ) -> anyhow::Result<()> {
        let Some(identity) = connection.peer_identity() else {
            anyhow::bail!("Connection peer has no certificate chain");
        };

        let Some(first_cert) = identity.as_slice().first() else {
            anyhow::bail!("Connection peer has no certificates");
        };

        let connection = Arc::new(ConnectionWrapper::new(
            is_debug,
            url,
            first_cert.hash(),
            connection,
            self_is_subscriber,
            peer_is_subscriber,
        ));

        let task = tokio::spawn(connection_supervisor(
            self.clone(),
            connection.clone(),
            incoming_messages.clone(),
            outgoing_messages.subscribe(),
            connection_closed_tx.clone(),
        ));

        let mut inner = self.0.write();
        inner.tasks.insert(connection.connection.stable_id(), Arc::new(task));
        inner.connections.insert(connection.connection.stable_id(), connection.clone());

        tracing::info!(
            connection_count = inner.connections.len(),
            peer = connection.peer_info(),
            "Added new connection"
        );
        Ok(())
    }

    pub fn remove_connection(&self, connection: &ConnectionWrapper) {
        let mut inner = self.0.write();
        inner.tasks.remove(&connection.connection.stable_id());
        inner.connections.remove(&connection.connection.stable_id());
        tracing::info!(
            connection_count = inner.connections.len(),
            peer = connection.peer_info(),
            "Removed connection"
        );
        if connection.self_is_subscriber {
            tracing::info!(publisher = connection.addr(), "Disconnected from publisher")
        }
        if connection.peer_is_subscriber {
            tracing::info!(subscriber = connection.addr(), "Subscriber disconnected")
        }
    }
}

async fn connection_supervisor(
    pub_sub: PubSub,
    connection: Arc<ConnectionWrapper>,
    incoming_messages_tx: mpsc::Sender<IncomingMessage>,
    outgoing_messages_rx: broadcast::Receiver<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionWrapper>>,
) -> anyhow::Result<()> {
    let (stop_sender_tx, stop_sender_rx) = watch::channel(false);
    let (stop_receiver_tx, stop_receiver_rx) = watch::channel(false);
    let result = tokio::select! {
        result = tokio::spawn(sender::sender(connection.clone(), stop_sender_rx, outgoing_messages_rx)) =>
            trace_task_result(result, "Sender", &connection),
        result = tokio::spawn(receiver::receiver(connection.clone(), stop_receiver_rx, incoming_messages_tx)) =>
            trace_task_result(result, "Receiver", &connection)
    };
    pub_sub.remove_connection(&connection);
    tracing::trace!(peer = connection.peer_info(), "Connection supervisor finished");
    let _ = stop_sender_tx.send(true);
    let _ = stop_receiver_tx.send(true);
    let _ = connection_closed_tx.send(connection).await;
    result?
}

fn trace_task_result(
    result: Result<anyhow::Result<()>, JoinError>,
    name: &str,
    connection: &ConnectionWrapper,
) -> Result<anyhow::Result<()>, JoinError> {
    match &result {
        Ok(result) => match result {
            Ok(_) => {
                tracing::info!(peer = connection.peer_info(), "{name} task finished");
            }
            Err(err) => {
                tracing::error!(
                    peer = connection.peer_info(),
                    "{name} task error: {}",
                    detailed(err)
                );
            }
        },
        Err(err) => {
            tracing::error!(
                peer = connection.peer_info(),
                "{name} task panicked: {}",
                detailed(err)
            )
        }
    }
    result
}

pub fn is_safely_closed(err: &ConnectionError) -> bool {
    match err {
        ConnectionError::ApplicationClosed(close) if close.code().into_inner() == 0 => true,
        ConnectionError::ConnectionClosed(_) => true,
        ConnectionError::LocallyClosed => true,
        // todo: investigate the reason of the H3_CLOSED_CRITICAL_STREAM
        ConnectionError::LocalH3Error(_) => true,
        _ => false,
    }
}
