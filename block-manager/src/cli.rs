// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::str::FromStr;
use std::sync::LazyLock;

use clap::Parser;
use message_router::read_keys_from_file;
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
    // Subscription to blocks (TODO change type to SocketAddr)
    #[arg(long, env)]
    pub stream_src_url: Url,

    /// REST API endpoint
    #[arg(long, env, default_value = "0.0.0.0:8001")]
    pub rest_api: std::net::SocketAddr,

    /// File path for sqlite
    #[arg(long, env, default_value_t = database::sqlite::sqlite_helper::SQLITE_DATA_DIR.to_string())]
    pub sqlite_path: String,
}

pub fn config_from_args(args: Args) -> anyhow::Result<AppConfig> {
    let subscribe_socket = parse_socket_address(
        // TODO replace with try_parse
        args.stream_src_url.host_str().expect("Host required"),
        args.stream_src_url.port_or_known_default().expect("Port required"),
    )?;

    let default_bp = HostPort::from_str(
        &std::env::var("DEFAULT_BP").expect("DEFAULT_BP environment variable must be set"),
    )?
    .with_default_port(DEFAULT_BP_PORT);
    let bk_api_token =
        std::env::var("BK_API_TOKEN").expect("BK Account API token (BK_API_TOKEN) not found");

    Ok(AppConfig {
        default_bp,
        rest_api: args.rest_api,
        subscribe_socket,
        sqlite_path: args.sqlite_path,
        bk_api_token,
        owner_wallet_pubkey: std::env::var("BM_OWNER_WALLET_PUBKEY").ok(),
        signing_keys: std::env::var("BM_ISSUER_KEYS_FILE")
            .ok()
            .and_then(|path| read_keys_from_file(&path).ok()),
    })
}

fn parse_socket_address(hostname: &str, port: u16) -> std::io::Result<SocketAddr> {
    let address = (hostname, port);
    // Try to resolve the hostname
    let mut addrs_iter = address.to_socket_addrs()?;
    addrs_iter.next().ok_or_else(|| std::io::Error::other("No address found for hostname"))
}
