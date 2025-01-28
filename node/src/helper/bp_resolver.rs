// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use network::config::NetworkConfig;
use parking_lot::Mutex;

use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;

pub struct BPResolverImpl {
    network: NetworkConfig<NodeIdentifier>,
    repository: Arc<Mutex<RepositoryImpl>>,
}

impl BPResolver for BPResolverImpl {
    fn resolve(&mut self) -> Vec<SocketAddr> {
        futures::executor::block_on(async { self.network.refresh_alive_nodes(false).await });
        let repository = self.repository.lock();
        let bp_id_for_thread_map = repository.get_nodes_by_threads();
        drop(repository);
        let mut nodes_vec = vec![];
        // TODO: this list of threads can change in runtime need to take smth like shared services
        for (thread_id, bp_id) in bp_id_for_thread_map.into_iter() {
            tracing::trace!("BP resolver: thread: {:?} bp: {:?}", thread_id, bp_id);
            if let Some(bp_node_id) = bp_id {
                tracing::trace!("BP resolver: producer id={bp_node_id}");
                if let Some(socker_addr) =
                    self.network.nodes.get(&bp_node_id).map(|addr| addr.to_owned())
                {
                    nodes_vec.push(socker_addr)
                }
            }
        }
        nodes_vec.dedup();
        nodes_vec
    }
}

impl BPResolverImpl {
    pub fn new(network_config: NetworkConfig<NodeIdentifier>, repository: RepositoryImpl) -> Self {
        Self { network: network_config, repository: Arc::new(Mutex::new(repository)) }
    }
}
