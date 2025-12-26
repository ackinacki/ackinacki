// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use mockall::automock;
use transport_layer::HostPort;

#[automock]
pub trait BPResolver: Send + Sync {
    fn resolve(&mut self, thread_id: Option<String>) -> Vec<HostPort>;
}
