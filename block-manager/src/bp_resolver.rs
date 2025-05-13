// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use network::socket_addr::ToOneSocketAddr;
use parking_lot::Mutex;
use parking_lot::RwLock;

pub struct BPResolverImpl {
    default_bp: SocketAddr,
    map_thread_addr: Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>,
}

impl BPResolverImpl {
    pub fn new() -> Self {
        let default_bp = std::env::var("DEFAULT_BP")
            .expect("DEFAULT_BP environment variable must be set")
            .to_socket_addr();
        let map_thread_addr = Arc::new(RwLock::new(HashMap::new()));

        Self { default_bp, map_thread_addr }
    }

    pub fn upsert(&mut self, thread_id: String, bp_list: Vec<String>) -> anyhow::Result<()> {
        let new_addrs: Vec<SocketAddr> =
            bp_list.into_iter().map(|addr_str| addr_str.to_socket_addr()).collect();

        let mut map = self.map_thread_addr.write();
        if map.get(&thread_id) != Some(&new_addrs) {
            map.insert(thread_id, new_addrs);
        }
        Ok(())
    }

    pub fn start_listener(
        resolver: Arc<Mutex<BPResolverImpl>>,
        bp_data_rx: mpsc::Receiver<(String, Vec<String>)>,
    ) -> anyhow::Result<()> {
        let _ =
            std::thread::Builder::new().name("BP update handler".to_string()).spawn(move || {
                for (thread_id, bp_list) in bp_data_rx {
                    tracing::debug!("Received BP data update: {thread_id}, {bp_list:?}");

                    let result = resolver.lock().upsert(thread_id, bp_list);

                    if let Err(e) = result {
                        tracing::error!("Failed to update `map_thread_addr`: {e}");
                    }
                }
            });

        Ok(())
    }
}

impl BPResolver for BPResolverImpl {
    fn resolve(&mut self, thread_id: Option<String>) -> Vec<SocketAddr> {
        thread_id
            .and_then(|id| self.map_thread_addr.read().get(&id).cloned())
            .unwrap_or_else(|| vec![self.default_bp])
    }
}
