use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::detailed;
use crate::pub_sub::Config;
use crate::pub_sub::ConnectionWrapper;
use crate::pub_sub::IncomingMessage;
use crate::pub_sub::OutgoingMessage;
use crate::pub_sub::PubSub;

pub async fn handle_subscriptions(
    pub_sub: PubSub,
    mut config_updates: watch::Receiver<Config>,
    incoming_messages: mpsc::Sender<IncomingMessage>,
    outgoing_messages: broadcast::Sender<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionWrapper>>,
    mut connection_closed_rx: mpsc::Receiver<Arc<ConnectionWrapper>>,
) -> anyhow::Result<()> {
    let mut reason = "starting".to_string();
    'handler: loop {
        let config = config_updates.borrow_and_update().clone();

        let (should_be_subscribed, should_be_unsubscribed) =
            pub_sub.schedule_subscriptions(&config);

        tracing::trace!(
            should_be_subscribed = should_be_subscribed.len(),
            should_be_unsubscribed = should_be_unsubscribed.len(),
            "Update subscriptions because of {reason}"
        );
        for connection in should_be_unsubscribed {
            connection.connection.close(0u32.into(), b"Unsubscribed");
        }

        let mut successfully_subscribed = 0;
        for url in &should_be_subscribed {
            match pub_sub
                .connect_to_peer(
                    &incoming_messages,
                    &outgoing_messages,
                    &connection_closed_tx,
                    &config,
                    url,
                    true,
                )
                .await
            {
                Ok(_) => successfully_subscribed += 1,
                Err(err) => {
                    tracing::error!(
                        url = url.to_string(),
                        "Subscribe to peer failed: {}",
                        detailed(&err)
                    );
                    continue;
                }
            }
        }
        let sleep_duration = if successfully_subscribed < should_be_subscribed.len() {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(60 * 60)
        };
        loop {
            tokio::select! {
                result = config_updates.changed() => {
                    if result.is_err() {
                        break 'handler;
                    }
                    reason = "config changed".to_string();
                }
                _ = tokio::time::sleep(sleep_duration) => {
                    reason = format!("{} failed to subscribe", should_be_subscribed.len() - successfully_subscribed);
                }
                connection = connection_closed_rx.recv() => {
                    if let Some(connection) = connection {
                        if !connection.self_is_subscriber {
                            continue;
                        }
                    }
                    reason = "connection closed".to_string();
                }
            }
            break;
        }
    }
    Ok(())
}
