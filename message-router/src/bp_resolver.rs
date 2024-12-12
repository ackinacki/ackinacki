// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;

pub trait BPResolver: Send + Sync {
    fn resolve(&mut self) -> Vec<SocketAddr>;
}
