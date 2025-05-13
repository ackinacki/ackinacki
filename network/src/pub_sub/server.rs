use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use wtransport::endpoint::IncomingSession;
use wtransport::Endpoint;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::connection_host_id;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::executor::IncomingSender;
use crate::pub_sub::PubSub;
use crate::pub_sub::ACKI_NACKI_SUBSCRIBE_HEADER;
use crate::tls::generate_server_config;
use crate::tls::TlsConfig;

#[allow(clippy::too_many_arguments)]
pub async fn listen_incoming_connections(
    pub_sub: PubSub,
    metrics: Option<NetMetrics>,
    max_connections: usize,
    bind: SocketAddr,
    incoming_tx: IncomingSender,
    outgoing_messages: broadcast::Sender<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionWrapper>>,
    tls_config: TlsConfig,
) -> anyhow::Result<()> {
    tracing::info!("Start listening for incoming connections on {}", bind.to_string());
    let config = generate_server_config(bind, &tls_config)?;
    tracing::info!("Pub sub started with host id {}", tls_config.my_host_id_prefix());
    let server = Endpoint::server(config)?;
    tracing::info!("Proxy subscribers started on port {}", bind.port());
    loop {
        let session = server.accept().await;
        tracing::info!("New session incoming");
        if server.open_connections() < max_connections {
            // It is not critical task because it serves single incoming connection request
            tokio::spawn(handle_incoming_connection(
                pub_sub.clone(),
                metrics.clone(),
                incoming_tx.clone(),
                outgoing_messages.clone(),
                connection_closed_tx.clone(),
                session,
            ));
        } else {
            tracing::error!(
                "Max connections reached {} of {}",
                server.open_connections(),
                max_connections
            );
        }
    }
}

pub async fn handle_incoming_connection(
    pub_sub: PubSub,
    metrics: Option<NetMetrics>,
    incoming_tx: IncomingSender,
    outgoing_messages: broadcast::Sender<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionWrapper>>,
    incoming_session: IncomingSession,
) {
    tracing::trace!("Incoming session received");

    let session_request = match incoming_session.await {
        Ok(request) => request,
        Err(err) => {
            tracing::error!("Incoming session request failed: {}", detailed(&err));
            return;
        }
    };

    tracing::trace!("Incoming session accepted");

    let peer_is_subscriber = session_request
        .headers()
        .get(ACKI_NACKI_SUBSCRIBE_HEADER)
        .map(|x| x == "true")
        .unwrap_or_default();

    let connection = match session_request.accept().await {
        Ok(connection) => connection,
        Err(err) => {
            tracing::error!("Failed to accept incoming connection: {}", detailed(&err));
            return;
        }
    };

    let host_id = match connection_host_id(&connection) {
        Ok(host_id) => host_id,
        Err(err) => {
            tracing::error!("Failed to identify incoming connection: {}", detailed(&err));
            return;
        }
    };

    tracing::trace!("Incoming request accepted");

    if let Err(err) = pub_sub.add_connection_handler(
        metrics.clone(),
        &incoming_tx,
        &outgoing_messages,
        &connection_closed_tx,
        connection,
        host_id,
        None,
        false,
        peer_is_subscriber,
    ) {
        tracing::error!("Error adding connection: {}", detailed(&err));
    }
}
