// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use network::try_socket_addr_from_url;
use parking_lot::Mutex;
use url::Url;

use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;

pub struct BPResolverImpl {
    peers_rx: tokio::sync::watch::Receiver<HashMap<NodeIdentifier, Url>>,
    repository: Arc<Mutex<RepositoryImpl>>,
}

impl BPResolverImpl {
    pub fn new(
        peers_rx: tokio::sync::watch::Receiver<HashMap<NodeIdentifier, Url>>,
        repository: Arc<Mutex<RepositoryImpl>>,
    ) -> Self {
        Self { peers_rx, repository }
    }
}

impl BPResolver for BPResolverImpl {
    fn resolve(&mut self, thread_id: Option<String>) -> Vec<SocketAddr> {
        let repository = self.repository.lock();
        let bp_id_for_thread_map = repository.get_nodes_by_threads();
        drop(repository);

        let target_thread = thread_id.and_then(|id| id.try_into().ok());

        tracing::debug!(target: "message_router", "bp_id_for_thread_map: {:?}", bp_id_for_thread_map);

        // TODO: this list of threads can change in runtime need to take smth like shared services
        let mut nodes_vec: Vec<SocketAddr> = bp_id_for_thread_map
            .into_iter()
            .filter_map(|(thread, bp_id)| {
                if target_thread.as_ref().is_none_or(|t| &thread == t) {
                    bp_id.and_then(|bp_node_id| {
                        self.peers_rx
                            .borrow_and_update()
                            .get(&bp_node_id)
                            .and_then(try_socket_addr_from_url)
                    })
                } else {
                    None
                }
            })
            .collect();

        nodes_vec.dedup();
        nodes_vec
    }
}
