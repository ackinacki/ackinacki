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
use crate::pub_sub::connection::connection_remote_host_id;
use crate::pub_sub::connection::ConnectionInfo;
use crate::pub_sub::connection::ConnectionRoles;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::executor::IncomingSender;
use crate::pub_sub::PubSub;
use crate::ACKI_NACKI_DIRECT_PROTOCOL;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL;

#[allow(clippy::too_many_arguments)]
pub async fn listen_incoming_connections<Transport>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut network_config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    pub_sub: PubSub<Transport>,
    metrics: Option<NetMetrics>,
    max_connections: usize,
    incoming_tx: IncomingSender,
    outgoing_messages: broadcast::Sender<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionInfo>>,
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
                sender = network_config_rx.changed() => if sender.is_err() {
                    return Ok(());
                } else {
                    let config_changed = {
                        let new_config = network_config_rx.borrow();
                        if new_config.bind != bind || new_config.credential != credential {
                            bind = new_config.bind;
                            credential = new_config.credential.clone();
                            true
                        } else {
                            false
                        }
                    };
                    if config_changed {
                        pub_sub.disconnect_untrusted(&credential).await;
                        tracing::info!("Listener config changed. Restarting listener.");
                        break;
                    } else {
                        continue;
                    }
                },
                sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                } else {
                    continue;
                }
            };
            tracing::info!("New session incoming");
            if pub_sub.open_connections() < max_connections {
                // It is not critical task because it serves single incoming connection request
                tokio::spawn(handle_incoming_connection(
                    shutdown_rx.clone(),
                    pub_sub.clone(),
                    metrics.clone(),
                    incoming_tx.clone(),
                    outgoing_messages.clone(),
                    connection_closed_tx.clone(),
                    request,
                ));
            } else {
                tracing::error!(
                    "Max connections reached {} of {}",
                    pub_sub.open_connections(),
                    max_connections
                );
            }
        }
    }
}

pub async fn handle_incoming_connection<Transport: NetTransport>(
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    pub_sub: PubSub<Transport>,
    metrics: Option<NetMetrics>,
    incoming_tx: IncomingSender,
    outgoing_messages: broadcast::Sender<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionInfo>>,
    incoming_request: impl NetIncomingRequest<Connection = Transport::Connection>,
) {
    tracing::trace!("Incoming request received");

    let connection = match incoming_request.accept().await {
        Ok(connection) => connection,
        Err(err) => {
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
            (ConnectionRoles::publisher(), true)
        } else if connection.alpn_negotiated_is(ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL) {
            (ConnectionRoles::publisher(), false)
        } else {
            (ConnectionRoles::direct_sender(), false)
        };
    let host_id = connection_remote_host_id(&connection);

    if let Err(err) = pub_sub.add_connection_handler(
        shutdown_rx,
        metrics.clone(),
        &incoming_tx,
        &outgoing_messages,
        &connection_closed_tx,
        connection,
        host_id,
        None,
        remote_is_proxy,
        role,
    ) {
        tracing::error!("Error adding connection: {}", detailed(&err));
    }
}
