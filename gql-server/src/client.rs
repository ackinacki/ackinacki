// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use tvm_client::ClientConfig;
use tvm_client::ClientContext;

pub type BlockchainClient = Arc<ClientContext>;

pub fn create_client() -> anyhow::Result<BlockchainClient> {
    let config = ClientConfig { ..Default::default() };
    let an_client = ClientContext::new(config)
        .map_err(|e| anyhow::anyhow!("Failed to create blockchain client: {}", e))?;

    Ok(Arc::new(an_client))
}
