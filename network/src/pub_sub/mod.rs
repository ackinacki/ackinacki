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
use url::Url;
use wtransport::endpoint::ConnectOptions;
use wtransport::error::ConnectionError;
use wtransport::Connection;
use wtransport::Endpoint;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::tls::create_client_config;
use crate::tls::TlsConfig;

pub const ACKI_NACKI_SUBSCRIBE_HEADER: &str = "AckiNacki-Subscribe";

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
        subscribe: &Vec<Vec<Url>>,
    ) -> (Vec<Vec<Url>>, Vec<Arc<ConnectionWrapper>>) {
        let inner = self.0.read();
        let subscribed = inner.connections.values().filter_map(|connection| {
            if let (Some(url), true) = (&connection.url, connection.self_is_subscriber) {
                Some((url.clone(), connection.clone()))
            } else {
                None
            }
        });
        diff(subscribed, subscribe)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn connect_to_peer(
        &self,
        metrics: Option<NetMetrics>,
        incoming_messages: &IncomingSender,
        outgoing_messages: &broadcast::Sender<OutgoingMessage>,
        connection_closed: &mpsc::Sender<Arc<ConnectionWrapper>>,
        config: &TlsConfig,
        urls: &[Url],
        subscribe: bool,
    ) -> anyhow::Result<()> {
        let client_config = create_client_config(config)?;
        let endpoint = Endpoint::client(client_config)?;
        let (connection, url) = 'connect: {
            for url in urls {
                let mut connect_options = ConnectOptions::builder(urls[0].as_ref());
                if subscribe {
                    connect_options =
                        connect_options.add_header(ACKI_NACKI_SUBSCRIBE_HEADER, "true");
                }
                match endpoint.connect(connect_options.build()).await {
                    Ok(connection) => {
                        break 'connect (connection, url);
                    }
                    Err(err) => {
                        tracing::error!(
                            url = url.as_ref().to_string(),
                            "Failed to connect to peer: {err}"
                        )
                    }
                };
            }
            return Err(anyhow::anyhow!("Failed to connect to peer"));
        };

        self.add_connection_handler(
            metrics.clone(),
            config.my_cert.is_debug(),
            incoming_messages,
            outgoing_messages,
            connection_closed,
            connection,
            Some(url.clone()),
            subscribe,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_connection_handler(
        &self,
        metrics: Option<NetMetrics>,
        is_debug: bool,
        incoming_messages_tx: &IncomingSender,
        outgoing_messages_tx: &broadcast::Sender<OutgoingMessage>,
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
            first_cert.hash().to_string(),
            connection,
            self_is_subscriber,
            peer_is_subscriber,
        ));

        let (outgoing_messages_tx, incoming_messages_tx) = if peer_is_subscriber {
            (Some(outgoing_messages_tx.subscribe()), None)
        } else {
            (None, Some(incoming_messages_tx.clone()))
        };
        let task = tokio::spawn(connection::connection_supervisor(
            self.clone(),
            metrics.clone(),
            connection.clone(),
            incoming_messages_tx,
            outgoing_messages_tx,
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
        .filter_map(|x| (!x.1).then_some(x.0.clone()))
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
