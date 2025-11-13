use std::sync::Arc;

use tvm_client::net::NetworkConfig;
use tvm_client::ClientConfig;
use tvm_client::ClientContext;

// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub mod web;

pub mod defaults;
pub mod helpers;
pub mod schema;

pub fn init_sdk(api_url: Option<String>) -> anyhow::Result<Arc<ClientContext>> {
    let endpoints = api_url.map(|url| vec![url]);

    let config = ClientConfig {
        network: NetworkConfig { endpoints, ..Default::default() },
        ..Default::default()
    };

    Ok(Arc::new(
        ClientContext::new(config)
            .map_err(|e| anyhow::anyhow!("failed to create SDK client: {e}"))?,
    ))
}
