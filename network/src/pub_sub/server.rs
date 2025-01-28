use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Error;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;
use wtransport::endpoint::IncomingSession;
use wtransport::Endpoint;

use crate::detailed;
use crate::pub_sub::Config;
use crate::pub_sub::ConnectionWrapper;
use crate::pub_sub::IncomingMessage;
use crate::pub_sub::OutgoingMessage;
use crate::pub_sub::PubSub;
use crate::pub_sub::ACKI_NACKI_SUBSCRIBE_HEADER;
use crate::tls::generate_server_config;

pub async fn listen_incoming_connections(
    max_connections: usize,
    bind: SocketAddr,
    session_pub: mpsc::Sender<IncomingSession>,
    mut config_sub: watch::Receiver<Config>,
) -> anyhow::Result<()> {
    let config_store = config_sub.borrow_and_update().clone();
    let config = generate_server_config(bind, &config_store)?;
    let server = Endpoint::server(config)?;
    tracing::info!("Proxy subscribers started on port {}", bind.port());

    loop {
        tokio::select! {
            v = config_sub.changed() => {
                match v {
                    Ok(()) => {
                        tracing::info!("Config changed");
                        let config = generate_server_config(bind, &config_sub.borrow_and_update())?;
                        server.reload_config(config, false)?;
                        tracing::info!("Config reloaded");
                    }
                    Err(err) => {
                        tracing::error!("Config watcher error: {}", err);
                    }
                }
            }
            v = server.accept() => {
                tracing::info!("New session incoming");
                if server.open_connections() >= max_connections {
                    tracing::error!("Max connections reached {} of {}", server.open_connections(), max_connections);
                } else {
                    session_pub.send(v).await?;
                }
            }
        }
    }
}

pub async fn handle_incoming_connections(
    pub_sub: PubSub,
    mut config_updates: watch::Receiver<Config>,
    incoming_messages: mpsc::Sender<IncomingMessage>,
    outgoing_messages: broadcast::Sender<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionWrapper>>,
    mut accepted_connections: mpsc::Receiver<IncomingSession>,
) -> Result<(), Error> {
    let is_debug = config_updates.borrow_and_update().my_cert.is_debug();
    loop {
        tracing::trace!("Waiting for incoming session");
        let incoming_session = match accepted_connections.recv().await {
            Some(session) => session,
            None => {
                tracing::info!("Finish receiving new session");
                break;
            }
        };

        tracing::trace!("Incoming session received");

        let Ok(session_request) = incoming_session.await else {
            tracing::error!("Error incoming session");
            continue;
        };

        tracing::trace!("Incoming session accepted");

        let peer_is_subscriber = session_request
            .headers()
            .get(ACKI_NACKI_SUBSCRIBE_HEADER)
            .map(|x| x == "true")
            .unwrap_or_default();

        let Ok(connection) = session_request.accept().await else {
            tracing::error!("Error accepting connection");
            continue;
        };

        tracing::trace!("Incoming request accepted");

        if let Err(err) = pub_sub.add_connection_handler(
            is_debug,
            &incoming_messages,
            &outgoing_messages,
            &connection_closed_tx,
            connection,
            None,
            false,
            peer_is_subscriber,
        ) {
            tracing::error!("Error adding connection: {}", detailed(&err));
        }
    }
    Ok(())
}
