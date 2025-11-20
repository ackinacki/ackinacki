pub mod config;
pub mod connection;
mod executor;
mod receiver;
mod sender;
mod server;
mod subscribe;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

pub use config::CertFile;
pub use config::CertStore;
pub use config::PrivateKeyFile;
use connection::ConnectionWrapper;
use connection::OutgoingMessage;
pub use executor::run;
pub use executor::IncomingSender;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use transport_layer::get_pubkeys_from_cert_der;
use transport_layer::pubkeys_info;
use transport_layer::NetConnection;
use transport_layer::NetCredential;
use transport_layer::NetTransport;

use crate::detailed;
use crate::metrics::to_label_kind;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionInfo;
use crate::pub_sub::connection::ConnectionRole;
use crate::topology::NetEndpoint;
use crate::topology::NetTopology;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL;

#[derive(Clone)]
pub struct PubSub<PeerId: Debug + Clone + Display + Send + Sync, Transport: NetTransport + 'static>
{
    pub transport: Transport,
    pub is_proxy: bool,
    pub metrics: Option<Arc<NetMetrics>>,
    inner: Arc<parking_lot::RwLock<PubSubInner<PeerId, Transport::Connection>>>,
}

pub struct PubSubInner<PeerId: Debug + Clone + Display + Send + Sync, Connection: NetConnection> {
    next_connection_id: u64,
    connections: HashMap<u64, Arc<ConnectionWrapper<PeerId, Connection>>>,
    connections_by_remote_addr: HashMap<SocketAddr, Arc<ConnectionWrapper<PeerId, Connection>>>,
    tasks: HashMap<u64, Arc<JoinHandle<anyhow::Result<()>>>>,
}

impl<PeerId: Debug + Clone + Display + Send + Sync, Connection: NetConnection>
    PubSubInner<PeerId, Connection>
{
    fn generate_connection_id(&mut self) -> u64 {
        let id = self.next_connection_id;
        self.next_connection_id = self.next_connection_id.wrapping_add(1).max(1);
        id
    }
}

impl<
        PeerId: Clone
            + PartialEq
            + Eq
            + Display
            + Debug
            + Send
            + Sync
            + Hash
            + FromStr<Err: Display>
            + 'static,
        Transport: NetTransport,
    > PubSub<PeerId, Transport>
{
    pub fn new(transport: Transport, is_proxy: bool, metrics: Option<NetMetrics>) -> Self {
        PubSub {
            transport,
            is_proxy,
            metrics: metrics.map(Arc::new),
            inner: Arc::new(parking_lot::RwLock::new(
                PubSubInner::<PeerId, Transport::Connection> {
                    next_connection_id: 1,
                    connections: HashMap::new(),
                    connections_by_remote_addr: HashMap::new(),
                    tasks: HashMap::new(),
                },
            )),
        }
    }

    pub fn open_connections(&self) -> usize {
        self.inner.read().connections.len()
    }

    pub fn schedule_subscriptions(
        &self,
        subscribe: &Vec<NetEndpoint<PeerId>>,
    ) -> (Vec<NetEndpoint<PeerId>>, Vec<Arc<ConnectionWrapper<PeerId, Transport::Connection>>>)
    {
        let inner = self.inner.read();
        let subscribed = inner.connections.values().filter_map(|connection| {
            if connection.info.local_role.is_subscriber() {
                Some((connection.info.remote_addr, connection.clone()))
            } else {
                None
            }
        });
        diff(subscribed, subscribe)
    }

    pub async fn disconnect_untrusted(&self, credential: &NetCredential) {
        self.disconnect_matching("untrusted", |info| {
            credential
                .verify_cert_hash_and_pubkeys(&info.remote_cert_hash, &info.remote_cert_pubkeys)
                .is_err()
        })
        .await;
    }

    pub async fn disconnect_incoming(&self) {
        self.disconnect_matching("incoming", |info| info.is_incoming()).await;
    }

    async fn disconnect_matching<F>(&self, hint: &str, filter: F)
    where
        F: Fn(&ConnectionInfo<PeerId>) -> bool,
    {
        let to_disconnect = {
            let inner = self.inner.read();
            inner
                .connections
                .values()
                .filter(|conn| filter(&conn.info))
                .cloned()
                .collect::<Vec<_>>()
        };

        tracing::info!("Disconnect {hint} {}", to_disconnect.len());
        for connection in to_disconnect {
            tracing::info!(peer = connection.info.remote_info(), "Disconnect {hint}");
            connection.connection.close(0).await;
            self.remove_connection(&connection.info);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe_to_publisher(
        self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
        metrics: Option<NetMetrics>,
        incoming_messages: IncomingSender<PeerId>,
        outgoing_messages: broadcast::Sender<OutgoingMessage<PeerId>>,
        connection_closed: mpsc::Sender<Arc<ConnectionInfo<PeerId>>>,
        credential: NetCredential,
        publisher: NetEndpoint<PeerId>,
    ) -> anyhow::Result<()> {
        let alpn = [if self.is_proxy {
            ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL
        } else {
            ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL
        }];
        let mut shuffled_addrs = Vec::from(publisher.subscribe_addrs());
        shuffled_addrs.shuffle(&mut thread_rng());
        let (connection, peer_addr) = 'connect: {
            for publisher_addr in shuffled_addrs {
                tracing::debug!(
                    publisher_addr = publisher_addr.to_string(),
                    "Connecting to publisher"
                );
                match self.transport.connect(publisher_addr, &alpn, credential.clone()).await {
                    Ok(connection) => {
                        break 'connect (connection, publisher_addr);
                    }
                    Err(err) => {
                        if let Some(metrics) = metrics.as_ref() {
                            metrics.report_error("pub_sub_fail_con_peer");
                        }
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
            topology_rx,
            metrics.clone(),
            &incoming_messages,
            &outgoing_messages,
            &connection_closed,
            connection,
            publisher.is_proxy(),
            Some(peer_addr),
            ConnectionRole::Subscriber,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_connection_handler(
        &self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
        metrics: Option<NetMetrics>,
        incoming_messages_tx: &IncomingSender<PeerId>,
        outgoing_messages_tx: &broadcast::Sender<OutgoingMessage<PeerId>>,
        connection_closed_tx: &mpsc::Sender<Arc<ConnectionInfo<PeerId>>>,
        connection: Transport::Connection,
        remote_is_proxy: bool,
        override_remote_addr: Option<SocketAddr>,
        role: ConnectionRole,
    ) -> anyhow::Result<()> {
        let cert = match connection.remote_certificate() {
            Some(cert) => cert,
            None => anyhow::bail!("Missing remote certificate"),
        };
        let cert_pubkeys = match get_pubkeys_from_cert_der(&cert) {
            Ok(pubkeys) => pubkeys,
            Err(err) => anyhow::bail!("{err}"),
        };
        let Some(remote_endpoint) =
            topology_rx.borrow().try_resolve_endpoint(remote_is_proxy, &cert_pubkeys)
        else {
            anyhow::bail!(
                "can not resolve {} endpoint with provided certificate with pubkeys {}.",
                if remote_is_proxy { "proxy" } else { "node" },
                pubkeys_info(&cert_pubkeys, 6),
            );
        };

        let id = { self.inner.write().generate_connection_id() };
        let connection = Arc::new(ConnectionWrapper::new(
            id,
            self.is_proxy,
            override_remote_addr,
            remote_endpoint,
            connection,
            role,
        )?);

        let (outgoing_messages_rx, incoming_messages_tx) = match role {
            ConnectionRole::Publisher => (Some(outgoing_messages_tx.subscribe()), None),
            ConnectionRole::Subscriber | ConnectionRole::DirectReceiver => {
                (Some(outgoing_messages_tx.subscribe()), Some(incoming_messages_tx.clone()))
            }
        };
        let task = tokio::spawn(connection::connection_supervisor(
            shutdown_rx,
            topology_rx,
            self.clone(),
            metrics.clone(),
            connection.clone(),
            incoming_messages_tx,
            outgoing_messages_rx,
            connection_closed_tx.clone(),
        ));

        let metrics = self.metrics.clone();
        let mut inner = self.inner.write();
        inner.tasks.insert(connection.info.id, Arc::new(task));
        inner.connections.insert(connection.info.id, connection.clone());
        inner.connections_by_remote_addr.insert(connection.info.remote_addr, connection.clone());

        tracing::info!(
            connection_count = inner.connections.len(),
            peer = connection.info.remote_info(),
            host_id = connection.info.remote_cert_hash_prefix,
            addr = connection.info.remote_addr.to_string(),
            "Added new connection"
        );
        metrics.inspect(|m| {
            report_connections_metrics::<PeerId, Transport>(m, &inner.connections);
        });
        Ok(())
    }

    pub fn remove_connection(&self, conn: &ConnectionInfo<PeerId>) {
        let metrics = self.metrics.clone();
        let mut inner = self.inner.write();
        inner.tasks.remove(&conn.id);
        inner.connections.remove(&conn.id);
        inner.connections_by_remote_addr.remove(&conn.remote_addr);
        tracing::info!(
            connection_count = inner.connections.len(),
            peer = conn.remote_info(),
            host_id = conn.remote_cert_hash_prefix,
            addr = conn.remote_addr.to_string(),
            "Removed connection"
        );
        match conn.local_role {
            ConnectionRole::Subscriber => {
                tracing::debug!(
                    publisher = conn.remote_addr.to_string(),
                    "Disconnected from publisher"
                );
            }
            ConnectionRole::Publisher => {
                tracing::debug!(
                    subscriber = conn.remote_addr.to_string(),
                    "Subscriber disconnected"
                );
            }
            ConnectionRole::DirectReceiver => {
                tracing::debug!(
                    subscriber = conn.remote_addr.to_string(),
                    "Disconnected from direct sender"
                );
            }
        }
        metrics.inspect(|m| {
            report_connections_metrics::<PeerId, Transport>(m, &inner.connections);
        });
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

pub fn spawn_critical_task(
    name: &'static str,
    task: impl Future<Output = ()> + Send + 'static,
    metrics: Option<NetMetrics>,
) {
    monitor_critical_task(name, tokio::spawn(task), metrics);
}

pub fn monitor_critical_task(
    name: &'static str,
    task: JoinHandle<()>,
    metrics: Option<NetMetrics>,
) {
    tokio::spawn(async move {
        match &task.await {
            Ok(_) => tracing::info!("{name} task finished"),
            Err(err) => {
                if let Some(metrics) = metrics.as_ref() {
                    let kind = to_label_kind(format!("monit_crit_panic_{name}"));
                    metrics.report_error(kind);
                }

                tracing::error!("Critical: {name} task panicked: {}", detailed(err))
            }
        }
    });
}

fn report_connections_metrics<PeerId: Debug + Clone + Display, Transport: NetTransport>(
    metrics: &NetMetrics,
    connections: &HashMap<u64, Arc<ConnectionWrapper<PeerId, Transport::Connection>>>,
) {
    let mut subscriber_count = 0usize;
    let mut publisher_count = 0usize;
    for connection in connections.values() {
        match connection.info.local_role {
            ConnectionRole::Subscriber => {
                publisher_count += 1;
            }
            ConnectionRole::Publisher => {
                subscriber_count += 1;
            }
            ConnectionRole::DirectReceiver => {}
        }
    }
    metrics.report_connections(subscriber_count, publisher_count);
}

// This code is unused
//
// pub fn start_critical_task_ex<Key: Send + 'static>(
//     name: &'static str,
//     key: Key,
//     stopped_tx: mpsc::UnboundedSender<Key>,
//     task: impl Future<Output = anyhow::Result<()>> + Send + 'static,
// ) {
//     monitor_critical_task_ex(name, key, stopped_tx, tokio::spawn(task))
// }

// pub fn monitor_critical_task_ex<Key: Send + 'static>(
//     name: &'static str,
//     key: Key,
//     stopped_tx: mpsc::UnboundedSender<Key>,
//     task: JoinHandle<anyhow::Result<()>>,
// ) {
//     tokio::spawn(async move {
//         match &task.await {
//             Ok(result) => match result {
//                 Ok(_) => tracing::info!("{name} task finished"),
//                 Err(err) => {
//                     tracing::error!("Critical: {name} task failed: {}", detailed(err))
//                 }
//             },
//             Err(err) => {
//                 tracing::error!("Critical: {name} task panicked: {}", detailed(err))
//             }
//         }
//         let _ = stopped_tx.send(key);
//     });
// }

fn diff<PeerId: Clone, Conn: Clone>(
    original: impl Iterator<Item = (SocketAddr, Conn)>,
    target: &Vec<NetEndpoint<PeerId>>,
) -> (Vec<NetEndpoint<PeerId>>, Vec<Conn>) {
    let mut preserve_original = HashMap::new();
    for (addr, conn) in original {
        preserve_original.insert(addr, (conn, false));
    }
    let mut included_addrs = HashSet::new();
    let mut should_be_included = Vec::<NetEndpoint<PeerId>>::new();
    for publisher in target {
        let mut all_addrs_are_new = true;
        let subscribe_addrs = publisher.subscribe_addrs();
        for addr in &subscribe_addrs {
            if let Some((_, preserve)) = preserve_original.get_mut(addr) {
                all_addrs_are_new = false;
                *preserve = true;
            } else if included_addrs.contains(addr) {
                all_addrs_are_new = false;
            }
        }
        if all_addrs_are_new {
            for addr in &subscribe_addrs {
                included_addrs.insert(*addr);
            }
            should_be_included.push(publisher.clone());
        }
    }
    let should_be_excluded = preserve_original
        .values()
        .filter_map(|(conn, preserve)| (!preserve).then_some(conn.clone()))
        .collect::<Vec<_>>();
    (should_be_included, should_be_excluded)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use crate::config::SocketAddrSet;
    use crate::pub_sub::diff;
    use crate::topology::NetEndpoint;

    fn addr(i: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, i], 0))
    }

    fn proxy<const N: usize>(i: [u8; N]) -> NetEndpoint<usize> {
        NetEndpoint::Proxy(SocketAddrSet::from_iter(i.into_iter().map(addr)))
    }

    #[test]
    fn test_diff() {
        let (include, exclude) =
            diff(vec![(addr(1), 1), (addr(2), 2)].into_iter(), &vec![proxy([1]), proxy([2])]);
        assert_eq!(include, Vec::<NetEndpoint<usize>>::new());
        assert_eq!(exclude, Vec::<usize>::new());

        let (included, exclude) = diff(
            vec![(addr(1), 1), (addr(2), 2), (addr(4), 4)].into_iter(),
            &vec![proxy([1, 2]), proxy([3, 5]), proxy([2, 3])],
        );
        assert_eq!(included, vec![proxy([3, 5])]);
        assert_eq!(exclude, vec![4]);

        let (included, exclude) = diff(
            vec![(addr(1), 1)].into_iter(),
            &vec![proxy([2, 3]), proxy([3, 4]), proxy([4, 5])],
        );
        assert_eq!(included, vec![proxy([2, 3]), proxy([4, 5])]);
        assert_eq!(exclude, vec![1]);
    }
}
