use anyhow::Error;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;

use super::IncomingMessage;
use super::OutgoingMessage;
use super::PubSub;
use crate::pub_sub::server::handle_incoming_connections;
use crate::pub_sub::server::listen_incoming_connections;
use crate::pub_sub::subscribe::handle_subscriptions;
use crate::pub_sub::Config;

pub async fn run(
    max_connections: usize,
    mut config_updates: watch::Receiver<Config>,
    outgoing: broadcast::Sender<OutgoingMessage>,
    incoming: mpsc::Sender<IncomingMessage>,
) -> anyhow::Result<()> {
    tracing::info!("Starting server");

    let config = config_updates.borrow_and_update().clone();
    let bind = config.bind;
    let pub_sub = PubSub::new();

    let log_task = tokio::spawn(log_broadcasting(outgoing.subscribe()));

    let (accepted_connections_tx, accepted_connections_rx) = mpsc::channel(100);
    let listen_incoming_connections_task = tokio::spawn(listen_incoming_connections(
        max_connections,
        bind,
        accepted_connections_tx,
        config_updates.clone(),
    ));

    let (connection_closed_tx, connection_closed_rx) = mpsc::channel(100);
    let handle_incoming_connections_task = tokio::spawn(handle_incoming_connections(
        pub_sub.clone(),
        config_updates.clone(),
        incoming.clone(),
        outgoing.clone(),
        connection_closed_tx.clone(),
        accepted_connections_rx,
    ));

    let subscriptions_task = tokio::spawn(handle_subscriptions(
        pub_sub.clone(),
        config_updates.clone(),
        incoming.clone(),
        outgoing.clone(),
        connection_closed_tx,
        connection_closed_rx,
    ));

    tokio::select! {
        v = log_task => {
            if let Err(err) = v {
                tracing::error!("Logger stopped with error: {}", err);
            }
            anyhow::bail!("Logger stopped");
        }
        v = listen_incoming_connections_task => {
            if let Err(err) = v {
                tracing::error!("Accept incoming connections task stopped with error: {}", err);
            }
            anyhow::bail!("Accept incoming connections stopped");
        }
        v = handle_incoming_connections_task => {
            tracing::error!("Incoming connections handler stopped: {:?}", v);
            if let Err(err) = v {
                tracing::error!("Incoming connections handler stopped with error: {}", err);
            }
            anyhow::bail!("Incoming connections handler stopped");
        }
        v = subscriptions_task => {
            tracing::error!("Subscribe client stopped: {:?}", v);
            if let Err(err) = v {
                tracing::error!("Subscribe client stopped with error: {}", err);
            }
            anyhow::bail!("Subscribe client stopped");
        }
    }
}

async fn log_broadcasting(
    outgoing_messages: broadcast::Receiver<OutgoingMessage>,
) -> Result<(), Error> {
    tracing::info!("Starting broadcaster logger");
    let mut outgoing_messages = outgoing_messages;
    loop {
        match outgoing_messages.recv().await {
            Ok(msg) => {
                let msg_log = if msg.data.len() > 10 { &msg.data[..10] } else { &msg.data[..] };
                tracing::info!("Received message from broadcast: {:?}", msg_log);
                tracing::info!("brx len {:?}", outgoing_messages.len());
            }
            Err(err) => {
                tracing::error!("Error receiving from broadcast: {}", err);
                anyhow::bail!(err);
            }
        }
    }
}
