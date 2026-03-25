// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::application::metrics::Metrics;
use crate::application::services::block_subscriber;
use crate::domain::models::WorkerCommand;

const MAX_ACTIVE: usize = 2;
const COOLDOWN: std::time::Duration = std::time::Duration::from_secs(10);

struct ActiveSubscriber {
    addr: SocketAddr,
    handle: JoinHandle<anyhow::Result<()>>,
}

pub async fn run(
    initial_nodes: Vec<SocketAddr>,
    cmd_tx: mpsc::Sender<WorkerCommand>,
    metrics: Option<Metrics>,
    mut nodes_rx: watch::Receiver<Vec<SocketAddr>>,
) -> anyhow::Result<()> {
    let mut active: Vec<ActiveSubscriber> = Vec::new();
    let mut nodes = initial_nodes;
    let mut cooldowns: HashMap<SocketAddr, Instant> = HashMap::new();

    fill_connections(&mut active, &nodes, &cmd_tx, &metrics, &cooldowns);

    let mut health_interval = tokio::time::interval(std::time::Duration::from_secs(1));

    loop {
        tokio::select! {
            result = nodes_rx.changed() => {
                if result.is_err() {
                    tracing::info!("Config watch channel closed, stopping connection pool");
                    break;
                }
                let new_nodes = nodes_rx.borrow_and_update().clone();
                tracing::info!(
                    "BK nodes config updated: {} -> {} nodes",
                    nodes.len(),
                    new_nodes.len()
                );
                // Abort connections to nodes no longer in the list
                active.retain(|sub| {
                    if new_nodes.contains(&sub.addr) {
                        true
                    } else {
                        tracing::info!("Aborting connection to removed node {}", sub.addr);
                        sub.handle.abort();
                        false
                    }
                });
                // Clear cooldowns for removed nodes
                cooldowns.retain(|addr, _| new_nodes.contains(addr));
                nodes = new_nodes;
                fill_connections(&mut active, &nodes, &cmd_tx, &metrics, &cooldowns);
            }
            _ = health_interval.tick() => {
                reap_dead(&mut active, &mut cooldowns);
                if active.is_empty() && nodes.is_empty() {
                    anyhow::bail!("All BK nodes exhausted and no active connections");
                }
                fill_connections(&mut active, &nodes, &cmd_tx, &metrics, &cooldowns);
            }
        }
    }

    // Cleanup
    for sub in &active {
        sub.handle.abort();
    }
    Ok(())
}

fn reap_dead(active: &mut Vec<ActiveSubscriber>, cooldowns: &mut HashMap<SocketAddr, Instant>) {
    active.retain(|sub| {
        if sub.handle.is_finished() {
            tracing::warn!("Connection to {} died, cooldown {}s", sub.addr, COOLDOWN.as_secs());
            cooldowns.insert(sub.addr, Instant::now());
            false
        } else {
            true
        }
    });
}

fn fill_connections(
    active: &mut Vec<ActiveSubscriber>,
    nodes: &[SocketAddr],
    cmd_tx: &mpsc::Sender<WorkerCommand>,
    metrics: &Option<Metrics>,
    cooldowns: &HashMap<SocketAddr, Instant>,
) {
    let target = MAX_ACTIVE.min(nodes.len());
    let now = Instant::now();
    while active.len() < target {
        let active_addrs: Vec<SocketAddr> = active.iter().map(|s| s.addr).collect();
        let candidate = nodes.iter().find(|n| {
            !active_addrs.contains(n)
                && cooldowns.get(n).is_none_or(|since| now.duration_since(*since) >= COOLDOWN)
        });
        let Some(&addr) = candidate else {
            break;
        };

        tracing::info!("Spawning subscriber connection to {addr}");
        let tx = cmd_tx.clone();
        let m = metrics.clone();
        let handle = tokio::spawn(async move { block_subscriber::run_once(&tx, addr, &m).await });

        active.push(ActiveSubscriber { addr, handle });
    }
}
