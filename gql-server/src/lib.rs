// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#![allow(unused)]

use std::sync::Arc;

use tvm_client::net::NetworkConfig;
use tvm_client::ClientConfig;
use tvm_client::ClientContext;

// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub mod web;

pub mod defaults;
pub mod helpers;
pub mod metrics;
pub mod schema;

pub fn init_sdk(api_url: Option<String>) -> anyhow::Result<Arc<ClientContext>> {
    let endpoints = api_url.map(|url| {
        let endpoint = normalize_sdk_endpoint(url);
        tracing::info!("Using Block Manager API endpoint: {endpoint}");
        vec![endpoint]
    });

    let config = ClientConfig {
        network: NetworkConfig { endpoints, ..Default::default() },
        ..Default::default()
    };

    Ok(Arc::new(
        ClientContext::new(config)
            .map_err(|e| anyhow::anyhow!("failed to create SDK client: {e}"))?,
    ))
}

fn normalize_sdk_endpoint(endpoint: String) -> String {
    if endpoint.contains("://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_sdk_endpoint;

    #[test]
    fn keeps_url_with_scheme() {
        assert_eq!(
            normalize_sdk_endpoint("https://block-manager.example:8600".to_string()),
            "https://block-manager.example:8600"
        );
    }

    #[test]
    fn adds_http_scheme_for_host_port() {
        assert_eq!(
            normalize_sdk_endpoint("block_manager:8600".to_string()),
            "http://block_manager:8600"
        );
    }
}
