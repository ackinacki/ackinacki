// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use chitchat::Chitchat;
use tokio::sync::Mutex;

use crate::socket_addr::StringSocketAddr;
use crate::socket_addr::ToOneSocketAddr;

pub type NodeIdentifier = i32;

#[derive(Clone)]
pub struct NetworkConfig {
    pub bind: SocketAddr,
    pub gossip: Arc<Mutex<Chitchat>>,
    pub nodes: HashMap<NodeIdentifier, SocketAddr>,
}

impl Debug for NetworkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkConfig").field("bind", &self.bind).finish()
    }
}

impl NetworkConfig {
    pub fn new(bind: SocketAddr, gossip: Arc<Mutex<Chitchat>>) -> Self {
        tracing::info!("Creating new network configuration with bind: {}", bind);
        Self { bind, gossip, nodes: HashMap::new() }
    }

    pub async fn alive_nodes(&mut self, filter_self: bool) -> anyhow::Result<Vec<SocketAddr>> {
        tracing::info!("Getting nodes from network configuration");
        tracing::info!(target: "network", "Initial node list: {:?}", self.nodes);
        let chitchat_guard = self.gossip.lock().await;

        let live_nodes_without_self = chitchat_guard
            .live_nodes()
            .map(|n| n.node_id.clone())
            .filter(|node_id| {
                if filter_self {
                    *node_id != chitchat_guard.self_chitchat_id().node_id
                } else {
                    true
                }
            })
            .collect::<HashSet<_>>();

        let node_addresses = chitchat_guard
            .state_snapshot()
            .node_state_snapshots
            .iter()
            .filter(|snapshot| live_nodes_without_self.contains(&snapshot.chitchat_id.node_id))
            .filter_map(|snapshot| {
                let address = snapshot.node_state.get("node_advertise_addr");
                let node_id = snapshot.node_state.get("node_id");
                address.map(|addr| (addr, node_id.unwrap()))
            })
            .map(|(node_advertise_addr, node_id)| {
                tracing::trace!("node_id: {node_id}, node_advertise_addr: {node_advertise_addr}");
                let id = NodeIdentifier::from_str(node_id).expect("Failed to convert node id");
                (id, StringSocketAddr::from(node_advertise_addr.to_string()).to_socket_addr())
            })
            .collect::<Vec<_>>();

        tracing::info!(target: "network", "Alive nodes: {node_addresses:?}");
        for (node_id, node_address) in node_addresses {
            self.nodes.insert(node_id, node_address);
        }
        tracing::info!(target: "network", "Final node list: {:?}", self.nodes);

        Ok(Vec::from_iter(self.nodes.values().copied()))
    }
}
