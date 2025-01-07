// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use network::config::NetworkConfig;
use parking_lot::Mutex;

use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::ThreadIdentifier;

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
        let metadatas = repository.get_all_metadata();
        drop(repository);
        let metadatas_guarded = metadatas.lock();
        let bp_id_for_thread_map: HashMap<ThreadIdentifier, Option<NodeIdentifier>> =
            metadatas_guarded
                .iter()
                .map(|(k, v)| {
                    let thread_metadata = v.lock();
                    (*k, thread_metadata.last_finalized_producer_id)
                })
                .collect();
        drop(metadatas_guarded);
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
    pub fn new(network_config: NetworkConfig, repository: RepositoryImpl) -> Self {
        Self { network: network_config, repository: Arc::new(Mutex::new(repository)) }
    }
}
