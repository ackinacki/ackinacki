use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use url::Url;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PubSub;
use crate::tls::TlsConfig;

#[allow(clippy::too_many_arguments)]
pub async fn handle_subscriptions(
    pub_sub: PubSub,
    metrics: Option<NetMetrics>,
    tls_config: TlsConfig,
    mut subscribe_rx: tokio::sync::watch::Receiver<Vec<Vec<Url>>>,
    incoming_messages: IncomingSender,
    outgoing_messages: broadcast::Sender<OutgoingMessage>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionWrapper>>,
    mut connection_closed_rx: mpsc::Receiver<Arc<ConnectionWrapper>>,
) -> anyhow::Result<()> {
    let mut reason = "starting".to_string();
    'handler: loop {
        let urls = subscribe_rx.borrow_and_update().clone();
        let count = urls.iter().flatten().count();
        metrics.as_ref().inspect(|m| m.report_subscribers_count(count));

        let (should_be_subscribed, should_be_unsubscribed) =
            { pub_sub.schedule_subscriptions(&urls) };

        tracing::trace!(
            "Update subscriptions{} because of {reason}",
            diff_info(should_be_subscribed.len(), should_be_unsubscribed.len()),
        );
        for connection in should_be_unsubscribed {
            connection.connection.close(0u32.into(), b"Unsubscribed");
        }

        let mut successfully_subscribed = 0;
        let should_be_subscribed_len = should_be_subscribed.len();
        for urls in should_be_subscribed {
            match pub_sub
                .connect_to_peer(
                    metrics.clone(),
                    &incoming_messages,
                    &outgoing_messages,
                    &connection_closed_tx,
                    &tls_config,
                    &urls,
                    true,
                )
                .await
            {
                Ok(_) => successfully_subscribed += 1,
                Err(err) => {
                    tracing::error!(
                        urls = urls.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","),
                        "Subscribe to peer failed: {}",
                        detailed(&err)
                    );
                    continue;
                }
            }
        }
        let sleep_duration = if successfully_subscribed < should_be_subscribed_len {
            // Retry sleep
            Duration::from_millis(100)
        } else {
            // Infinite sleep
            Duration::from_secs(60 * 60)
        };
        loop {
            tokio::select! {
                result = subscribe_rx.changed() => {
                    if result.is_err() {
                        break 'handler;
                    }
                    reason = "subscribe changed".to_string();
                }
                _ = tokio::time::sleep(sleep_duration) => {
                    reason = format!("{} failed to subscribe", should_be_subscribed_len - successfully_subscribed);
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

fn diff_info(include: usize, exclude: usize) -> String {
    match (include > 0, exclude > 0) {
        (true, true) => format!(" (+{include} -{exclude})"),
        (true, false) => format!(" (+{include})"),
        (false, true) => format!(" (-{exclude})"),
        (false, false) => "".to_string(),
    }
}
