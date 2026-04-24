// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;

use clap::Parser;
use message_router::read_keys_from_file;
use serde::Deserialize;
use transport_layer::HostPort;
use url::Url;

use crate::domain::models::AppConfig;
use crate::domain::models::DEFAULT_BP_PORT;

pub static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
{}
BUILD_GIT_BRANCH={}
BUILD_GIT_COMMIT={}
BUILD_GIT_DATE={}
BUILD_TIME={}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_GIT_BRANCH"),
        env!("BUILD_GIT_COMMIT"),
        env!("BUILD_GIT_DATE"),
        env!("BUILD_TIME"),
    )
});

/// Acki-Nacki Block Manager
#[derive(Parser, Debug)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
pub struct Args {
    /// Subscription to blocks (TODO change type to SocketAddr)
    #[arg(long, env)]
    pub stream_src_url: Option<Url>,

    /// Path to YAML config file with BK endpoints list
    #[arg(long, env = "CONFIG_FILE")]
    pub config: Option<PathBuf>,

    /// REST API endpoint
    #[arg(long, env, default_value = "0.0.0.0:8001")]
    pub rest_api: std::net::SocketAddr,

    /// File path for sqlite
    #[arg(long, env, default_value_t = database::sqlite::sqlite_helper::SQLITE_DATA_DIR.to_string())]
    pub sqlite_path: String,

    /// ClickHouse URL for wallet activity export (e.g. http://localhost:8123)
    /// If not set, ClickHouse export is disabled
    #[arg(long, env = "CLICKHOUSE_URL")]
    pub clickhouse_url: Option<String>,

    /// ClickHouse user (required when clickhouse-url is set)
    #[arg(long, env = "CLICKHOUSE_USER")]
    pub clickhouse_user: Option<String>,

    /// ClickHouse password (required when clickhouse-url is set)
    #[arg(long, env = "CLICKHOUSE_PASSWORD")]
    pub clickhouse_password: Option<String>,
}

#[derive(Deserialize)]
struct ConfigFile {
    bk_stream_blocks_endpoints: Vec<String>,
    #[serde(default)]
    bk_api_endpoints: Vec<String>,
}

pub struct ParsedConfig {
    pub stream_sockets: Vec<SocketAddr>,
    pub api_endpoints: Vec<HostPort>,
}

pub fn parse_config_file(path: &Path) -> anyhow::Result<ParsedConfig> {
    let contents = std::fs::read_to_string(path)?;
    let config: ConfigFile = serde_yaml::from_str(&contents)?;
    let mut stream_sockets = Vec::new();
    for ep in &config.bk_stream_blocks_endpoints {
        let addr = parse_endpoint(ep)?;
        stream_sockets.push(addr);
    }
    if stream_sockets.is_empty() {
        anyhow::bail!("bk_stream_blocks_endpoints list is empty in {}", path.display());
    }
    let mut api_endpoints = Vec::new();
    for ep in &config.bk_api_endpoints {
        let hp = HostPort::from_str(ep)?;
        api_endpoints.push(hp);
    }
    Ok(ParsedConfig { stream_sockets, api_endpoints })
}

fn parse_endpoint(ep: &str) -> anyhow::Result<SocketAddr> {
    // Try parsing as URL only if it looks like one (contains "://")
    if ep.contains("://") {
        if let Ok(url) = Url::parse(ep) {
            let host =
                url.host_str().ok_or_else(|| anyhow::anyhow!("No host in endpoint: {ep}"))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| anyhow::anyhow!("No port in endpoint: {ep}"))?;
            return Ok(parse_socket_address(host, port)?);
        }
    }
    // Try parsing as IP "addr:port"
    if let Ok(addr) = ep.parse::<SocketAddr>() {
        return Ok(addr);
    }
    // Try DNS resolution for "hostname:port" (e.g. docker compose service names)
    let mut addrs = ep.to_socket_addrs()?;
    addrs.next().ok_or_else(|| anyhow::anyhow!("Cannot resolve endpoint: {ep}"))
}

pub fn config_from_args(args: Args) -> anyhow::Result<AppConfig> {
    let (subscribe_sockets, config_path, config_api_endpoints) =
        if let Some(ref config_path) = args.config {
            let parsed = parse_config_file(config_path)?;
            (parsed.stream_sockets, Some(config_path.clone()), parsed.api_endpoints)
        } else if let Some(ref stream_src_url) = args.stream_src_url {
            let socket = parse_socket_address(
                stream_src_url.host_str().expect("Host required"),
                stream_src_url.port_or_known_default().expect("Port required"),
            )?;
            (vec![socket], None, vec![])
        } else {
            anyhow::bail!("Either --config or --stream-src-url must be provided");
        };

    let default_bp = HostPort::from_str(
        &std::env::var("DEFAULT_BP").expect("DEFAULT_BP environment variable must be set"),
    )?
    .with_default_port(DEFAULT_BP_PORT);
    let bk_api_token =
        std::env::var("BK_API_TOKEN").expect("BK Account API token (BK_API_TOKEN) not found");

    // Use bk_api_endpoints from config if available, otherwise fall back to default_bp
    let bk_api_endpoints = if config_api_endpoints.is_empty() {
        vec![default_bp.clone()]
    } else {
        config_api_endpoints
    };

    Ok(AppConfig {
        default_bp,
        rest_api: args.rest_api,
        subscribe_sockets,
        sqlite_path: args.sqlite_path,
        bk_api_token,
        owner_wallet_pubkey: std::env::var("BM_OWNER_WALLET_PUBKEY").ok(),
        signing_keys: std::env::var("BM_ISSUER_KEYS_FILE")
            .ok()
            .and_then(|path| read_keys_from_file(&path).ok()),
        config_path,
        bk_api_endpoints,
        clickhouse_url: args.clickhouse_url,
        clickhouse_user: args.clickhouse_user,
        clickhouse_password: args.clickhouse_password,
    })
}

fn parse_socket_address(hostname: &str, port: u16) -> std::io::Result<SocketAddr> {
    let address = (hostname, port);
    // Try to resolve the hostname
    let mut addrs_iter = address.to_socket_addrs()?;
    addrs_iter.next().ok_or_else(|| std::io::Error::other("No address found for hostname"))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_parse_config_file() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "bk_stream_blocks_endpoints:\n  - 127.0.0.1:12000\n  - 127.0.0.1:12001")
            .unwrap();
        let parsed = parse_config_file(tmp.path()).unwrap();
        assert_eq!(parsed.stream_sockets.len(), 2);
        assert_eq!(parsed.stream_sockets[0].port(), 12000);
        assert_eq!(parsed.stream_sockets[1].port(), 12001);
        assert!(parsed.api_endpoints.is_empty());
    }

    #[test]
    fn test_parse_config_file_with_api_endpoints() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            tmp,
            "bk_stream_blocks_endpoints:\n  - 127.0.0.1:12000\nbk_api_endpoints:\n  - node0:8600\n  - node1:8600"
        )
        .unwrap();
        let parsed = parse_config_file(tmp.path()).unwrap();
        assert_eq!(parsed.stream_sockets.len(), 1);
        assert_eq!(parsed.api_endpoints.len(), 2);
        assert_eq!(parsed.api_endpoints[0].to_string(), "node0:8600");
        assert_eq!(parsed.api_endpoints[1].to_string(), "node1:8600");
    }

    #[test]
    fn test_parse_config_file_empty_list() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "bk_stream_blocks_endpoints: []").unwrap();
        assert!(parse_config_file(tmp.path()).is_err());
    }
}
