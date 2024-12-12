// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use network::config::NetworkConfig;
use parking_lot::Mutex;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;

pub struct BPResolverImpl {
    network: NetworkConfig,
    repository: Arc<Mutex<RepositoryImpl>>,
}

impl BPResolver for BPResolverImpl {
    fn resolve(&mut self) -> Vec<SocketAddr> {
        let fut_nodes = async { self.network.alive_nodes(false).await };
        let _alive_nodes =
            futures::executor::block_on(fut_nodes).expect("Failed to update nodes addresses");
        let repository = self.repository.lock();
        let mut nodes_vec = vec![];
        // TODO: this list of threads can change in runtime need to take smth like shared services
        for (thread_id, _state) in repository.list_finalized_states() {
            tracing::trace!("BP resolver: thread: {:?}", thread_id);
            if let Ok(Some(block)) = repository.load_archived_block(&None, Some(*thread_id)) {
                let bp_node_id = block.data().get_common_section().producer_id;
                tracing::trace!("BP resolver: producer id={bp_node_id}");
                if let Some(socker_addr) =
                    self.network.nodes.get(&bp_node_id).map(|addr| addr.to_owned())
                {
                    nodes_vec.push(socker_addr)
                }
            }
        }
        nodes_vec
    }
}

impl BPResolverImpl {
    pub fn new(network_config: NetworkConfig, repository: RepositoryImpl) -> Self {
        Self { network: network_config, repository: Arc::new(Mutex::new(repository)) }
    }
}
