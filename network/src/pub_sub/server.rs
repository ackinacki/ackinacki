use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use transport_layer::NetConnection;
use transport_layer::NetIncomingRequest;
use transport_layer::NetListener;
use transport_layer::NetTransport;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::connection_remote_host_id;
use crate::pub_sub::connection::ConnectionInfo;
use crate::pub_sub::connection::ConnectionRoles;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::executor::IncomingSender;
use crate::pub_sub::PubSub;
use crate::tls::TlsConfig;
use crate::ACKI_NACKI_DIRECT_PROTOCOL;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL;
use crate::ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL;

#[allow(clippy::too_many_arguments)]
pub async fn listen_incoming_connections<Transport>(
    pub_sub: PubSub<Transport>,
    metrics: Option<NetMetrics>,
    max_connections: usize,
    bind: SocketAddr,
    incoming_tx: IncomingSender,
    outgoing_messages: broadcast::Sender<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionInfo>>,
    tls_config: TlsConfig,
) -> anyhow::Result<()>
where
    Transport: NetTransport + 'static,
{
    tracing::info!("Start listening for incoming connections on {}", bind.to_string());
    tracing::info!("Pub sub started with host id {}", tls_config.my_host_id_prefix());
    let listener = pub_sub
        .transport
        .create_listener(
            bind,
            &[
                ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL,
                ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL,
                ACKI_NACKI_DIRECT_PROTOCOL,
            ],
            tls_config.credential(),
        )
        .await?;
    loop {
        let request = listener.accept().await?;
        tracing::info!("New session incoming");
        if pub_sub.open_connections() < max_connections {
            // It is not critical task because it serves single incoming connection request
            tokio::spawn(handle_incoming_connection(
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

pub async fn handle_incoming_connection<Transport: NetTransport>(
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
