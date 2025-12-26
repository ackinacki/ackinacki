// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use parking_lot::RwLock;
use transport_layer::HostPort;

use crate::domain::models::UpdatableBPResolver;
use crate::domain::models::DEFAULT_BP_PORT;

pub struct BPResolverImpl {
    default_bp: HostPort,
    map_thread_addr: Arc<RwLock<HashMap<String, Vec<HostPort>>>>,
}

impl BPResolverImpl {
    pub fn new(default_bp: HostPort) -> Self {
        let map_thread_addr = Arc::new(RwLock::new(HashMap::new()));

        Self { default_bp, map_thread_addr }
    }
}

impl BPResolver for BPResolverImpl {
    fn resolve(&mut self, thread_id: Option<String>) -> Vec<HostPort> {
        thread_id
            .and_then(|id| self.map_thread_addr.read().get(&id).cloned())
            .unwrap_or_else(|| vec![self.default_bp.clone()])
    }
}
impl UpdatableBPResolver for BPResolverImpl {
    fn upsert(&mut self, thread_id: String, bp_list: Vec<HostPort>) -> anyhow::Result<()> {
        let new_addrs: Vec<HostPort> =
            bp_list.into_iter().map(|addr| addr.with_default_port(DEFAULT_BP_PORT)).collect();

        let mut map = self.map_thread_addr.write();
        if map.get(&thread_id) != Some(&new_addrs) {
            map.insert(thread_id, new_addrs);
        }
        Ok(())
    }
}
