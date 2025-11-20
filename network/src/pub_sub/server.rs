use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use transport_layer::NetConnection;
use transport_layer::NetIncomingRequest;
use transport_layer::NetListener;
use transport_layer::NetTransport;

use crate::config::NetworkConfig;
use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionInfo;
use crate::pub_sub::connection::ConnectionRole;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::executor::IncomingSender;
use crate::pub_sub::PubSub;
use crate::topology::NetTopology;
use crate::ACKI_NACKI_DIRECT_PROTOCOL;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL;

#[allow(clippy::too_many_arguments)]
pub async fn listen_incoming_connections<
    PeerId: Clone + PartialEq + Eq + Display + Hash + Debug + Send + Sync + FromStr<Err: Display> + 'static,
    Transport,
>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut network_config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    net_topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    pub_sub: PubSub<PeerId, Transport>,
    metrics: Option<NetMetrics>,
    max_connections: usize,
    incoming_tx: IncomingSender<PeerId>,
    outgoing_messages: broadcast::Sender<OutgoingMessage<PeerId>>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionInfo<PeerId>>>,
) -> anyhow::Result<()>
where
    Transport: NetTransport + 'static,
{
    let mut bind = network_config_rx.borrow().bind;
    let mut credential = network_config_rx.borrow().credential.clone();
    loop {
        let listener = pub_sub
            .transport
            .create_listener(
                bind,
                &[
                    ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL,
                    ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL,
                    ACKI_NACKI_DIRECT_PROTOCOL,
                ],
                credential.clone(),
            )
            .await?;
        tracing::info!("Start listening for incoming connections on {}", bind.to_string());
        tracing::info!("Pub sub started with host id {}", credential.identity_prefix());
        loop {
            let request = tokio::select! {
                request = listener.accept() => request?,
                config_changed = network_config_rx.changed() => if config_changed.is_ok() {
                    let (bind_changed, credential_changed) = {
                        let new_config = network_config_rx.borrow();
                        let bind_changed = new_config.bind != bind;
                        let credential_changed = new_config.credential != credential;
                        if bind_changed || credential_changed {
                            bind = new_config.bind;
                            credential = new_config.credential.clone();
                        }
                        (bind_changed, credential_changed)
                    };
                    if bind_changed || credential_changed {
                        drop(listener);
                        if credential_changed {
                            tracing::info!("Listener credential changed. Disconnect untrusted.");
                            pub_sub.disconnect_untrusted(&credential).await;
                        }
                        if bind_changed {
                            pub_sub.disconnect_incoming().await;
                            tracing::info!("Listener bind addr changed. Disconnect incoming.");
                        }
                        tracing::info!("Listener config changed. Restarting listener.");
                        break;
                    }
                    continue;
                } else {
                    tracing::info!("PubSub finished: network config provider closed.");
                    return Ok(());
                },
                shutdown_changed = shutdown_rx.changed() => if shutdown_changed.is_err() || *shutdown_rx.borrow() {
                    tracing::info!("PubSub finished: shutdown signal received.");
                    return Ok(());
                } else {
                    continue;
                }
            };
            tracing::info!(
                addr = request.remote_addr().map(|x| x.to_string()).unwrap_or_default(),
                "New incoming connection request"
            );
            if pub_sub.open_connections() < max_connections {
                // It is not critical task because it serves single incoming connection request
                tokio::spawn(handle_incoming_connection(
                    shutdown_rx.clone(),
                    net_topology_rx.clone(),
                    pub_sub.clone(),
                    metrics.clone(),
                    incoming_tx.clone(),
                    outgoing_messages.clone(),
                    connection_closed_tx.clone(),
                    request,
                ));
            } else {
                if let Some(metrics) = metrics.as_ref() {
                    metrics.report_error("max_conn_reached");
                }
                tracing::error!(
                    "Max connections reached {} of {}",
                    pub_sub.open_connections(),
                    max_connections
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_incoming_connection<
    PeerId: Clone + PartialEq + Eq + Display + Hash + Debug + Send + Sync + FromStr<Err: Display> + 'static,
    Transport: NetTransport,
>(
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    pub_sub: PubSub<PeerId, Transport>,
    metrics: Option<NetMetrics>,
    incoming_tx: IncomingSender<PeerId>,
    outgoing_messages: broadcast::Sender<OutgoingMessage<PeerId>>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionInfo<PeerId>>>,
    incoming_request: impl NetIncomingRequest<Connection = Transport::Connection>,
) {
    tracing::trace!("Incoming request received");

    let connection = match incoming_request.accept().await {
        Ok(connection) => connection,
        Err(err) => {
            if let Some(metrics) = metrics.as_ref() {
                metrics.report_error("fail_accept_in_con");
            }
            tracing::error!("Failed to accept incoming connection: {}", detailed(&err));
            return;
        }
    };

    tracing::trace!(
        alpn = connection.alpn_negotiated().unwrap_or_else(|| "-".to_string()),
        remote_addr = %connection.remote_addr(),
        "Incoming request accepted",
    );

    let (role, remote_is_proxy) =
        if connection.alpn_negotiated_is(ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL) {
            (ConnectionRole::Publisher, true)
        } else if connection.alpn_negotiated_is(ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL) {
            (ConnectionRole::Publisher, false)
        } else {
            (ConnectionRole::DirectReceiver, false)
        };
    let topology = topology_rx.borrow().clone();
    let addr = connection.remote_addr();
    if let Err(err) = pub_sub.add_connection_handler(
        shutdown_rx,
        topology_rx,
        metrics.clone(),
        &incoming_tx,
        &outgoing_messages,
        &connection_closed_tx,
        connection,
        remote_is_proxy,
        None,
        role,
    ) {
        tracing::error!(
            addr = addr.to_string(),
            is_proxy = remote_is_proxy,
            topology = format!("{topology:?}"),
            "Failed to accept incoming connection: {err}"
        );
        if let Some(metrics) = metrics.as_ref() {
            metrics.report_error("err_add_con");
        }
    }
}
