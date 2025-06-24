// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;

use mockall::automock;

#[automock]
pub trait BPResolver: Send + Sync {
    fn resolve(&mut self, thread_id: Option<String>) -> Vec<SocketAddr>;
}
