use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use transport_layer::NetConnection;
use transport_layer::NetTransport;

use crate::config::NetworkConfig;
use crate::config::SocketAddrSet;
use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionInfo;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PubSub;
use crate::topology::NetEndpoint;
use crate::topology::NetTopology;

fn join_addrs(addrs: &SocketAddrSet) -> String {
    addrs.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")
}

fn task_addrs<PeerId>(
    publishers: &HashMap<tokio::task::Id, NetEndpoint<PeerId>>,
    id: tokio::task::Id,
) -> String {
    publishers.get(&id).map(|x| join_addrs(&x.subscribe_addrs())).unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_subscriptions<
    PeerId: Clone + PartialEq + Eq + Display + Hash + Debug + Send + Sync + FromStr<Err: Display> + 'static,
    Transport: NetTransport + 'static,
>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    network_config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    pub_sub: PubSub<PeerId, Transport>,
    metrics: Option<NetMetrics>,
    mut net_topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    incoming_messages: IncomingSender<PeerId>,
    outgoing_messages: broadcast::Sender<OutgoingMessage<PeerId>>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionInfo<PeerId>>>,
    mut connection_closed_rx: mpsc::Receiver<Arc<ConnectionInfo<PeerId>>>,
) -> anyhow::Result<()> {
    tracing::trace!("Subscription loop started");
    let mut reason = "starting".to_string();
    let mut my_subscribe_plan = net_topology_rx.borrow().my_subscribe_plan().clone();
    let mut last_closed_addrs = HashMap::<SocketAddr, Instant>::new();
    loop {
        metrics.as_ref().inspect(|m| {
            m.report_planned_publisher_count(my_subscribe_plan.len());
        });
        last_closed_addrs.retain(|_, close_time| close_time.elapsed().as_millis() < 200);
        let subscriptions = my_subscribe_plan
            .iter()
            .filter_map(|publisher| match publisher {
                NetEndpoint::Peer(peer) => {
                    (!last_closed_addrs.contains_key(&peer.addr)).then_some(publisher.clone())
                }
                NetEndpoint::Proxy(addrs) => {
                    let mut addrs = addrs.clone();
                    addrs.retain(|addr| !last_closed_addrs.contains_key(addr));
                    (!addrs.is_empty()).then_some(NetEndpoint::Proxy(addrs))
                }
            })
            .collect::<Vec<_>>();

        let (should_be_subscribed, should_be_unsubscribed) =
            pub_sub.schedule_subscriptions(&subscriptions);

        tracing::info!(
            target: "monit",
            added = ?should_be_subscribed,
            "Update subscriptions{} because of {reason}",
            diff_info(
                subscriptions.len(),
                should_be_subscribed.len(),
                should_be_unsubscribed.len()
            ),
        );
        for connection in should_be_unsubscribed {
            connection.connection.close(0).await;
        }

        let credential = network_config_rx.borrow().credential.clone();
        let mut successfully_subscribed = 0;
        let should_be_subscribed_len = should_be_subscribed.len();
        let mut connect_tasks = JoinSet::new();
        let mut publisher_by_task_id = HashMap::<tokio::task::Id, NetEndpoint<PeerId>>::new();
        for publisher in should_be_subscribed.clone() {
            let abort_handle = connect_tasks.spawn(pub_sub.clone().subscribe_to_publisher(
                shutdown_rx.clone(),
                net_topology_rx.clone(),
                metrics.clone(),
                incoming_messages.clone(),
                outgoing_messages.clone(),
                connection_closed_tx.clone(),
                credential.clone(),
                publisher.clone(),
            ));
            publisher_by_task_id.insert(abort_handle.id(), publisher);
        }
        loop {
            tokio::select! {
                sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                    tracing::trace!("Subscription loop finished: shutdown signal received");
                    return Ok(());
                },
                task = connect_tasks.join_next_with_id() => if let Some(task) = task {
                    match task {
                        Ok((_task_id, Ok(()))) => successfully_subscribed += 1,
                        Ok((task_id, Err(err))) => {
                            if let Some(metrics) = metrics.as_ref() {
                                metrics.report_error("sub_to_peer_failed");
                            }
                            tracing::error!(
                                addrs = task_addrs(&publisher_by_task_id, task_id),
                                "Subscribe to peer failed: {}",
                                detailed(&err)
                            );
                        }
                        Err(err) => {
                            if let Some(metrics) = metrics.as_ref() {
                                metrics.report_error("sub_to_peer_panic");
                            }
                            tracing::error!(
                                addrs = task_addrs(&publisher_by_task_id, err.id()),
                                "Subscribe to peer panic: {}",
                                detailed(&err)
                            );
                        }
                    }
                } else {
                    break;
                }
            }
        }

        let sleep_duration = if successfully_subscribed < should_be_subscribed_len {
            // Retry sleep
            Duration::from_millis(100)
        } else if !last_closed_addrs.is_empty() {
            Duration::from_millis(200)
        } else {
            // Infinite sleep
            Duration::from_secs(1000000)
        };

        // Waiting for one of:
        // - a subscribe list was changed
        // - 100 ms timeout after failed subscriptions
        // - our subscription connection was closed
        loop {
            tokio::select! {
                sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                    tracing::trace!("Subscription loop finished: shutdown signal received");
                    return Ok(());
                } else {
                    continue;
                },
                net_topology_changed = net_topology_rx.changed() => if net_topology_changed.is_err() {
                    tracing::trace!("Subscription loop finished: subscribe plan provider closed");
                    return Ok(());
                } else {
                    let new_my_subscribe_plan = net_topology_rx.borrow().my_subscribe_plan().clone();
                    if new_my_subscribe_plan != my_subscribe_plan {
                        my_subscribe_plan = new_my_subscribe_plan;
                        reason = "subscribe plan changed".to_string();
                    } else {
                        continue;
                    }
                },
                _ = tokio::time::sleep(sleep_duration) => {
                    let failed = should_be_subscribed_len - successfully_subscribed + last_closed_addrs.len();
                    reason = format!("{failed} failed to subscribe");
                }
                connection = connection_closed_rx.recv() => {
                    if let Some(connection) = connection {
                        // if the closed connection is not our subscription, continue waiting
                        if !connection.local_is_subscriber() {
                            continue;
                        }
                        last_closed_addrs.insert(connection.remote_addr, Instant::now());
                    }
                    reason = "connection closed".to_string();
                }
            }
            break;
        }
    }
}

fn diff_info(total: usize, include: usize, exclude: usize) -> String {
    match (include > 0, exclude > 0) {
        (true, true) => format!(" {total} (+{include} -{exclude})"),
        (true, false) => format!(" {total} (+{include})"),
        (false, true) => format!(" {total} (-{exclude})"),
        (false, false) => format!(" {total}"),
    }
}
