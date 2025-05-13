use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use url::Url;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PubSub;
use crate::tls::TlsConfig;

fn join_urls(urls: &[Url]) -> String {
    urls.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")
}

fn task_urls(urls: &HashMap<tokio::task::Id, Vec<Url>>, id: tokio::task::Id) -> String {
    urls.get(&id).map(|x| join_urls(x)).unwrap_or_default()
}

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
        let mut connect_tasks = JoinSet::new();
        let mut urls_by_task_id = HashMap::<tokio::task::Id, Vec<Url>>::new();
        for urls in should_be_subscribed.clone() {
            let pub_sub = pub_sub.clone();
            let metrics = metrics.clone();
            let incoming_messages = incoming_messages.clone();
            let outgoing_messages = outgoing_messages.clone();
            let connection_closed_tx = connection_closed_tx.clone();
            let tls_config = tls_config.clone();
            let urls_clone = urls.clone();
            let abort_handle = connect_tasks.spawn(async move {
                pub_sub
                    .connect_to_peer(
                        metrics,
                        &incoming_messages,
                        &outgoing_messages,
                        &connection_closed_tx,
                        &tls_config,
                        &urls_clone,
                        true,
                    )
                    .await
            });
            urls_by_task_id.insert(abort_handle.id(), urls);
        }
        while let Some(task) = connect_tasks.join_next_with_id().await {
            match task {
                Ok((_task_id, Ok(()))) => successfully_subscribed += 1,
                Ok((task_id, Err(err))) => {
                    tracing::error!(
                        urls = task_urls(&urls_by_task_id, task_id),
                        "Subscribe to peer failed: {}",
                        detailed(&err)
                    );
                }
                Err(err) => {
                    tracing::error!(
                        urls = task_urls(&urls_by_task_id, err.id()),
                        "Subscribe to peer panic: {}",
                        detailed(&err)
                    );
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

        // Waiting for one of:
        // - subscribe list was changed
        // - 100 ms timeout after failed subscriptions
        // - our subscription connection was closed
        loop {
            tokio::select! {
                result = subscribe_rx.changed() => {
                    if result.is_err() {
                        // if subscribe sender was dropped, exit subscriptions loop
                        break 'handler;
                    }
                    reason = "subscribe changed".to_string();
                }
                _ = tokio::time::sleep(sleep_duration) => {
                    reason = format!("{} failed to subscribe", should_be_subscribed_len - successfully_subscribed);
                }
                connection = connection_closed_rx.recv() => {
                    if let Some(connection) = connection {
                        // if closed connection is not our subscription, continue waiting
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
