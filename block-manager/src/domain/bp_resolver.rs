// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use message_router::bp_resolver::BPResolver;
use parking_lot::RwLock;
use transport_layer::HostPort;

use crate::domain::models::UpdatableBPResolver;

pub struct BPResolverImpl {
    default_bp: HostPort,
    map_thread_addr: RwLock<HashMap<String, Vec<HostPort>>>,
}

impl BPResolverImpl {
    pub fn new(default_bp: HostPort) -> Self {
        Self { default_bp, map_thread_addr: RwLock::new(HashMap::new()) }
    }

    // pub fn start_listener(
    //     resolver: Arc<Mutex<BPResolverImpl>>,
    //     bp_data_rx: mpsc::Receiver<(String, Vec<HostPort>)>,
    // ) -> anyhow::Result<()> {
    //     std::thread::Builder::new().name("BP update handler".to_string()).spawn(move || {
    //         for (thread_id, bp_list) in bp_data_rx {
    //             if let Err(e) = resolver.lock().upsert(thread_id, bp_list) {
    //                 tracing::error!("Failed to update `map_thread_addr`: {e}");
    //             }
    //         }
    //     })?;

    //     Ok(())
    // }
}

impl BPResolver for BPResolverImpl {
    fn resolve(&mut self, thread_id: Option<String>) -> Vec<HostPort> {
        let resolved = thread_id
            .clone()
            .and_then(|id| self.map_thread_addr.read().get(&id).cloned())
            .unwrap_or_else(|| vec![self.default_bp.clone()]);

        tracing::debug!("resolved for thread={:?}, bp={:?}", thread_id, resolved);
        resolved
    }
}

impl UpdatableBPResolver for BPResolverImpl {
    fn upsert(&self, thread_id: String, bp_list: Vec<HostPort>) -> anyhow::Result<()> {
        tracing::debug!(bp_list = ?bp_list, thread_id = thread_id, "upsert data");
        let mut new_addrs = Vec::with_capacity(bp_list.len());

        for addr in bp_list {
            // default port forced for backward compatibility
            new_addrs.push(addr.with_default_port(8600));
        }

        use std::collections::hash_map::Entry;

        let mut map = self.map_thread_addr.write();

        match map.entry(thread_id) {
            Entry::Vacant(e) => {
                e.insert(new_addrs);
            }
            Entry::Occupied(mut e) => {
                if e.get() != &new_addrs {
                    e.insert(new_addrs);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn upsert_and_resolve() {
        let default_bp = HostPort::from_str("127.0.0.1:8600").unwrap();
        let mut resolver = BPResolverImpl::new(default_bp);

        let bp1 = HostPort::from_str("127.0.0.1:8001").unwrap();
        let bp2 = HostPort::from_str("127.0.0.1:8002").unwrap();
        resolver.upsert("thread1".into(), vec![bp1.clone(), bp2.clone()]).unwrap();

        let result = resolver.resolve(Some("thread1".into()));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&bp1));
        assert!(result.contains(&bp2));

        let bp3 = HostPort::from_str("127.0.0.7:8007").unwrap();
        let bp4 = HostPort::from_str("127.0.0.8:8008").unwrap();
        resolver.upsert("thread1".into(), vec![bp3.clone(), bp4.clone()]).unwrap();

        let result = resolver.resolve(Some("thread1".into()));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&bp3));
        assert!(result.contains(&bp4));
    }

    #[test]
    fn resolve_returns_default_when_missing() {
        let default_bp = HostPort::from_str("127.0.0.1:8600").unwrap();
        let mut resolver = BPResolverImpl::new(default_bp.clone());

        let result = resolver.resolve(Some("unknown".into()));
        assert_eq!(result, vec![default_bp.clone()]);

        let result = resolver.resolve(None);
        assert_eq!(result, vec![default_bp]);
    }
}
