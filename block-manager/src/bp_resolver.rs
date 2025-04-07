// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;

use message_router::bp_resolver::BPResolver;
use network::socket_addr::ToOneSocketAddr;

pub struct BPResolverImpl {
    default_bp: SocketAddr,
}

impl BPResolverImpl {
    pub fn new() -> Self {
        let default_bp = std::env::var("DEFAULT_BP")
            .expect("DEFAULT_BP environment variable must be set")
            .to_socket_addr();

        Self { default_bp }
    }
}

impl BPResolver for BPResolverImpl {
    fn resolve(&mut self, _thread_id: Option<String>) -> Vec<SocketAddr> {
        vec![self.default_bp]
    }
}
