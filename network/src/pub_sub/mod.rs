pub mod config;
pub mod connection;
mod executor;
mod receiver;
mod sender;
mod server;
mod subscribe;

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;

pub use config::CertFile;
pub use config::CertStore;
pub use config::PrivateKeyFile;
use connection::ConnectionWrapper;
use connection::OutgoingMessage;
pub use executor::run;
pub use executor::IncomingSender;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use transport_layer::NetConnection;
use transport_layer::NetCredential;
use transport_layer::NetTransport;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::connection_remote_host_id;
use crate::pub_sub::connection::ConnectionInfo;
use crate::pub_sub::connection::ConnectionRoles;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL;

#[derive(Clone)]
pub struct PubSub<Transport: NetTransport + 'static> {
    pub transport: Transport,
    pub is_proxy: bool,
    inner: Arc<parking_lot::RwLock<PubSubInner<Transport::Connection>>>,
}

pub struct PubSubInner<Connection: NetConnection> {
    next_connection_id: u64,
    connections: HashMap<u64, Arc<ConnectionWrapper<Connection>>>,
    tasks: HashMap<u64, Arc<JoinHandle<anyhow::Result<()>>>>,
}

impl<Connection: NetConnection> PubSubInner<Connection> {
    fn generate_connection_id(&mut self) -> u64 {
        let id = self.next_connection_id;
        self.next_connection_id = self.next_connection_id.wrapping_add(1).max(1);
        id
    }
}

impl<Transport: NetTransport> PubSub<Transport> {
    pub fn new(transport: Transport, is_proxy: bool) -> Self {
        PubSub {
            transport,
            is_proxy,
            inner: Arc::new(parking_lot::RwLock::new(PubSubInner::<Transport::Connection> {
                next_connection_id: 1,
                connections: HashMap::new(),
                tasks: HashMap::new(),
            })),
        }
    }

    pub fn open_connections(&self) -> usize {
        self.inner.read().connections.len()
    }

    pub fn schedule_subscriptions(
        &self,
        subscribe: &Vec<Vec<SocketAddr>>,
    ) -> (Vec<Vec<SocketAddr>>, Vec<Arc<ConnectionWrapper<Transport::Connection>>>) {
        let inner = self.inner.read();
        let subscribed = inner.connections.values().filter_map(|connection| {
            if let (addr, true) = (&connection.info.remote_addr, connection.info.roles.subscriber) {
                Some((*addr, connection.clone()))
            } else {
                None
            }
        });
        diff(subscribed, subscribe)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe_to_publisher(
        &self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        metrics: Option<NetMetrics>,
        incoming_messages: &IncomingSender,
        outgoing_messages: &broadcast::Sender<OutgoingMessage>,
        connection_closed: &mpsc::Sender<Arc<ConnectionInfo>>,
        credential: &NetCredential,
        publisher_addrs: &[SocketAddr],
    ) -> anyhow::Result<()> {
        let (connection, peer_host_id, peer_addr) = 'connect: {
            for publisher_addr in publisher_addrs {
                let alpn = [if self.is_proxy {
                    ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL
                } else {
                    ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL
                }];
                tracing::debug!(
                    publisher_addr = publisher_addr.to_string(),
                    "Connecting to publisher"
                );
                match self.transport.connect(*publisher_addr, &alpn, credential.clone()).await {
                    Ok(connection) => {
                        let host_id = connection_remote_host_id(&connection);
                        break 'connect (connection, host_id, publisher_addr);
                    }
                    Err(err) => {
                        tracing::error!(
                            addr = publisher_addr.to_string(),
                            "Failed to connect to peer: {err}"
                        )
                    }
                };
            }
            return Err(anyhow::anyhow!("Failed to connect to peer: no more addrs"));
        };

        self.add_connection_handler(
            shutdown_rx,
            metrics.clone(),
            incoming_messages,
            outgoing_messages,
            connection_closed,
            connection,
            peer_host_id,
            Some(*peer_addr),
            false,
            ConnectionRoles::subscriber(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_connection_handler(
        &self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        metrics: Option<NetMetrics>,
        incoming_messages_tx: &IncomingSender,
        outgoing_messages_tx: &broadcast::Sender<OutgoingMessage>,
        connection_closed_tx: &mpsc::Sender<Arc<ConnectionInfo>>,
        connection: Transport::Connection,
        remote_host_id: String,
        remote_addr: Option<SocketAddr>,
        remote_is_proxy: bool,
        roles: ConnectionRoles,
    ) -> anyhow::Result<()> {
        let id = { self.inner.write().generate_connection_id() };
        let connection = Arc::new(ConnectionWrapper::new(
            id,
            self.is_proxy,
            remote_addr,
            remote_host_id,
            remote_is_proxy,
            connection,
            roles,
        ));

        let (outgoing_messages_tx, incoming_messages_tx) = if roles.publisher {
            (Some(outgoing_messages_tx.subscribe()), None)
        } else {
            (None, Some(incoming_messages_tx.clone()))
        };
        let task = tokio::spawn(connection::connection_supervisor(
            shutdown_rx,
            self.clone(),
            metrics.clone(),
            connection.clone(),
            incoming_messages_tx,
            outgoing_messages_tx,
            connection_closed_tx.clone(),
        ));

        let mut inner = self.inner.write();
        inner.tasks.insert(connection.info.id, Arc::new(task));
        inner.connections.insert(connection.info.id, connection.clone());

        tracing::info!(
            connection_count = inner.connections.len(),
            peer = connection.info.remote_info(),
            host_id = connection.info.remote_host_id_prefix,
            "Added new connection"
        );
        Ok(())
    }

    pub fn remove_connection(&self, conn: &ConnectionInfo) {
        let mut inner = self.inner.write();
        inner.tasks.remove(&conn.id);
        inner.connections.remove(&conn.id);
        tracing::info!(
            connection_count = inner.connections.len(),
            peer = conn.remote_info(),
            host_id = conn.remote_host_id_prefix,
            "Removed connection"
        );
        if conn.roles.subscriber {
            tracing::info!(publisher = conn.remote_addr.to_string(), "Disconnected from publisher");
        }
        if conn.roles.publisher {
            tracing::info!(subscriber = conn.remote_addr.to_string(), "Subscriber disconnected");
        }
    }
}

pub fn trace_task_result(
    result: Result<anyhow::Result<()>, JoinError>,
    name: &str,
) -> Result<anyhow::Result<()>, JoinError> {
    match &result {
        Ok(result) => match result {
            Ok(_) => {
                tracing::info!("{name} task finished");
            }
            Err(err) => {
                tracing::error!("{name} task failed: {}", detailed(err));
            }
        },
        Err(err) => {
            tracing::error!("{name} task panicked: {}", detailed(err))
        }
    }
    result
}

pub fn spawn_critical_task(name: &'static str, task: impl Future<Output = ()> + Send + 'static) {
    monitor_critical_task(name, tokio::spawn(task));
}

pub fn monitor_critical_task(name: &'static str, task: JoinHandle<()>) {
    tokio::spawn(async move {
        match &task.await {
            Ok(_) => tracing::info!("{name} task finished"),
            Err(err) => {
                tracing::error!("Critical: {name} task panicked: {}", detailed(err))
            }
        }
    });
}

pub fn start_critical_task_ex<Key: Send + 'static>(
    name: &'static str,
    key: Key,
    stopped_tx: mpsc::UnboundedSender<Key>,
    task: impl Future<Output = anyhow::Result<()>> + Send + 'static,
) {
    monitor_critical_task_ex(name, key, stopped_tx, tokio::spawn(task))
}

pub fn monitor_critical_task_ex<Key: Send + 'static>(
    name: &'static str,
    key: Key,
    stopped_tx: mpsc::UnboundedSender<Key>,
    task: JoinHandle<anyhow::Result<()>>,
) {
    tokio::spawn(async move {
        match &task.await {
            Ok(result) => match result {
                Ok(_) => tracing::info!("{name} task finished"),
                Err(err) => {
                    tracing::error!("Critical: {name} task failed: {}", detailed(err))
                }
            },
            Err(err) => {
                tracing::error!("Critical: {name} task panicked: {}", detailed(err))
            }
        }
        let _ = stopped_tx.send(key);
    });
}

fn diff<Key: Hash + Eq + Clone, Value: Clone>(
    original: impl Iterator<Item = (Key, Value)>,
    target: &Vec<Vec<Key>>,
) -> (Vec<Vec<Key>>, Vec<Value>) {
    let mut preserve_original = HashMap::new();
    for (key, value) in original {
        preserve_original.insert(key, (value, false));
    }
    let mut included_keys = HashSet::new();
    let mut should_be_included = Vec::<Vec<Key>>::new();
    for keys in target {
        let mut all_keys_are_new = true;
        for key in keys {
            if let Some((_, preserve)) = preserve_original.get_mut(key) {
                all_keys_are_new = false;
                *preserve = true;
            } else if included_keys.contains(key) {
                all_keys_are_new = false;
            }
        }
        if all_keys_are_new {
            for key in keys {
                included_keys.insert(key.clone());
            }
            should_be_included.push(keys.to_vec());
        }
    }
    let should_be_excluded = preserve_original
        .values()
        .filter_map(|(v, preserve)| (!preserve).then_some(v.clone()))
        .collect::<Vec<_>>();
    (should_be_included, should_be_excluded)
}

#[test]
fn test_diff() {
    let (include, exclude) =
        diff(vec![("url1", 1), ("url2", 2)].into_iter(), &vec![vec!["url1"], vec!["url2"]]);
    assert_eq!(include, Vec::<Vec<&str>>::new());
    assert_eq!(exclude, Vec::<usize>::new());

    let (included, exclude) = diff(
        vec![("url1", 1), ("url2", 2), ("url4", 4)].into_iter(),
        &vec![vec!["url1", "url2"], vec!["url3", "url5"], vec!["url2", "url3"]],
    );
    assert_eq!(included, vec![vec!["url3", "url5"]]);
    assert_eq!(exclude, vec![4]);

    let (included, exclude) = diff(
        vec![("url1", 1)].into_iter(),
        &vec![vec!["url2", "url3"], vec!["url3", "url4"], vec!["url4", "url5"]],
    );
    assert_eq!(included, vec![vec!["url2", "url3"], vec!["url4", "url5"]]);
    assert_eq!(exclude, vec![1]);
}
