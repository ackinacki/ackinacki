// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;
pub mod gossip_peer;
use std::fmt::Display;

use anyhow::Context;
use base64::Engine;
use chitchat::spawn_chitchat;
use chitchat::ChitchatConfig;
use chitchat::ChitchatHandle;
use chitchat::ChitchatId;
use chitchat::ChitchatRef;
use chitchat::ClusterStateSnapshot;
use chitchat::FailureDetectorConfig;
use chitchat::NodeState;
use ed25519_dalek::Signer;
use itertools::Itertools;
use poem::listener::TcpListener;
use poem::Route;
use poem::Server;
use poem_openapi::payload::Json;
use poem_openapi::OpenApi;
use poem_openapi::OpenApiService;
use rand::Rng;
use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinHandle;
use url::Url;

use crate::gossip_peer::GossipPeer;

pub static DEFAULT_GOSSIP_INTERVAL: Duration = Duration::from_millis(500);
pub const GOSSIP_API_ADVERTISE_ADDR_KEY: &str = "api_advertise_addr";
pub const ADVERTISE_ADDR_KEY: &str = "node_advertise_addr";
pub const BK_API_SOCKET_KEY: &str = "bk_api_socket";
pub const BM_API_SOCKET_KEY: &str = "bm_api_socket";
pub const ID_KEY: &str = "node_id";
pub const PROXIES_KEY: &str = "node_proxies";
// pubkey_signature is base64 buf with (VerifyingKey([u8; 32]), Signature([u8; 64]))
pub const PUBKEY_SIGNATURE_KEY: &str = "pubkey_signature";

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse {
    pub cluster_id: String,
    pub cluster_state: ClusterStateSnapshot,
    pub live_nodes: Vec<ChitchatId>,
    pub dead_nodes: Vec<ChitchatId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetKeyValueResponse {
    pub status: bool,
}

struct Api {
    chitchat: ChitchatRef,
}

#[OpenApi]
impl Api {
    /// Chitchat state
    #[oai(path = "/", method = "get")]
    async fn index(&self) -> Json<serde_json::Value> {
        let chitchat_guard = self.chitchat.lock();
        let response = ApiResponse {
            cluster_id: chitchat_guard.cluster_id().to_string(),
            cluster_state: chitchat_guard.state_snapshot(),
            live_nodes: chitchat_guard.live_nodes().cloned().collect::<Vec<_>>(),
            dead_nodes: chitchat_guard.dead_nodes().cloned().collect::<Vec<_>>(),
        };
        Json(serde_json::to_value(&response).unwrap())
    }
}

fn generate_chitchat_node_id(prefix: &str) -> String {
    let x: u64 = rand::thread_rng().gen();
    format!("{prefix}-{x:x}")
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct GossipConfig {
    /// UDP socket address to listen gossip.
    /// Defaults to "127.0.0.1:10000"
    #[serde(default = "default_gossip_listen_addr")]
    pub listen_addr: SocketAddr,

    /// Gossip advertise socket address.
    /// Defaults to `listen_addr` address
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise_addr: Option<SocketAddr>,

    /// Gossip seed nodes socket addresses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seeds: Vec<SocketAddr>,

    /// Chitchat cluster id for gossip
    #[serde(default = "default_chitchat_cluster_id")]
    pub cluster_id: String,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_gossip_listen_addr(),
            advertise_addr: None,
            seeds: Vec::new(),
            cluster_id: default_chitchat_cluster_id(),
        }
    }
}

impl GossipConfig {
    pub fn advertise_addr(&self) -> SocketAddr {
        self.advertise_addr.unwrap_or(self.listen_addr)
    }

    fn chitchat_config(&self, chitchat_node_id: String) -> ChitchatConfig {
        let generation = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|err| {
                tracing::warn!("System time error: {err:?}");
                Duration::from_secs(0)
            })
            .as_secs();

        let chitchat_id = ChitchatId::new(chitchat_node_id, generation, self.advertise_addr());

        ChitchatConfig {
            cluster_id: self.cluster_id.clone(),
            chitchat_id,
            gossip_interval: DEFAULT_GOSSIP_INTERVAL,
            listen_addr: self.listen_addr,
            seed_nodes: self.seeds.iter().map(|x| x.to_string()).collect(),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(600),
            catchup_callback: None,
            extra_liveness_predicate: None,
        }
    }
}

fn default_gossip_listen_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 10000))
}

fn default_chitchat_cluster_id() -> String {
    "acki_nacki".to_string()
}

#[derive(Debug, Clone, PartialEq)]
pub struct GossipReloadConfig<PeerId: FromStr<Err: Display> + Display> {
    pub gossip_config: GossipConfig,
    pub api_advertise_addr: Url,
    pub my_ed_key_secret: Vec<String>,
    pub my_ed_key_path: Vec<String>,
    pub peer_config: Option<GossipPeer<PeerId>>,
}

pub async fn run_gossip_no_reload(
    chitchat_id_prefix: &str,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    config_rx: tokio::sync::watch::Receiver<GossipConfig>,
    transport: impl chitchat::transport::Transport,
) -> anyhow::Result<(ChitchatHandle, JoinHandle<anyhow::Result<()>>)> {
    let config = config_rx.borrow().clone();
    let chitchat_config = config.chitchat_config(generate_chitchat_node_id(chitchat_id_prefix));

    // tracing::info!("Starting UDP gossip server on {gossip_advertise_addr}");
    // let transport = UdpTransport;

    tracing::info!("Starting gossip server on {}", config.advertise_addr());

    let chitchat_handle = spawn_chitchat(chitchat_config, Vec::new(), &transport).await?;
    let chitchat = chitchat_handle.chitchat();
    let api = Api { chitchat: chitchat.clone() };
    let api_service = OpenApiService::new(api, "Acki Nacki", "1.0")
        .server(format!("http://{}/", config.advertise_addr()));
    let docs = api_service.swagger_ui();
    let app = Route::new().nest("/", api_service).nest("/docs", docs);

    tracing::info!("Starting REST server on advertise addr {}", config.advertise_addr());
    tracing::info!("Starting REST server on listen addr {}", config.listen_addr);

    let rest_server_handle = tokio::spawn(async move {
        let shutdown_signal = async {
            while !*shutdown_rx.borrow() {
                if shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
        };

        Server::new(TcpListener::bind(config.listen_addr))
            .run_with_graceful_shutdown(app, shutdown_signal, None)
            .await
            .map_err(|err| err.into())
    });

    Ok((chitchat_handle, rest_server_handle))
}

pub async fn run_gossip_with_reload<PeerId>(
    chitchat_id_prefix: &str,
    mut graceful_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut config_rx: tokio::sync::watch::Receiver<GossipReloadConfig<PeerId>>,
    transport: impl chitchat::transport::Transport,
) -> anyhow::Result<(
    tokio::sync::watch::Receiver<Option<ChitchatRef>>,
    JoinHandle<Result<(), anyhow::Error>>,
)>
where
    PeerId: Clone + Display + Send + Sync + Eq + FromStr<Err: Display> + 'static,
{
    let (first_tx, first_rx) = tokio::sync::oneshot::channel::<bool>();
    let mut first_tx = Some(first_tx);

    let (chitchat_tx, chitchat_rx) = tokio::sync::watch::channel::<Option<ChitchatRef>>(None);

    let chitchat_node_id = generate_chitchat_node_id(chitchat_id_prefix);
    let run_gossip_handle = tokio::spawn(async move {
        loop {
            let update_config = config_rx.borrow().clone();
            tracing::debug!("Config updated");
            let gossip_config = update_config.gossip_config;
            let chitchat_config = gossip_config.chitchat_config(chitchat_node_id.clone());

            tracing::info!("Starting ChitChat server on {}", gossip_config.advertise_addr());

            let chitchat_handle = spawn_chitchat(chitchat_config, Vec::new(), &transport).await?;

            chitchat_handle
                .with_chitchat(|c| {
                    if let Some(gossip_node) = update_config.peer_config.clone() {
                        gossip_node.set_to(c.self_node_state());
                    }
                    c.self_node_state().set(
                        GOSSIP_API_ADVERTISE_ADDR_KEY,
                        update_config.api_advertise_addr.to_string(),
                    );
                    if let Ok(keys) = transport_layer::resolve_signing_keys(
                        &update_config.my_ed_key_secret,
                        &update_config.my_ed_key_path,
                    ) {
                        if let Some(key) = keys.first() {
                            sign_gossip_node(c.self_node_state(), key.clone());
                        }
                    }
                })
                .await;

            let chitchat_ref = chitchat_handle.chitchat();
            let api = Api { chitchat: chitchat_ref.clone() };

            let api_service = OpenApiService::new(api, "Acki Nacki", "1.0")
                .server(format!("http://{}/", gossip_config.advertise_addr()));
            let app = Route::new().nest("/", api_service);

            let (rest_shutdown_tx, mut rest_shutdown_rx) = tokio::sync::watch::channel(false);

            tracing::info!(
                listen_addr = %gossip_config.listen_addr,
                advertise_addr = %gossip_config.advertise_addr(),
                "Starting REST server"
            );
            let rest_server_handle = tokio::spawn(async move {
                Server::new(TcpListener::bind(gossip_config.listen_addr))
                    .run_with_graceful_shutdown(
                        app,
                        async move {
                            while !*rest_shutdown_rx.borrow() {
                                if rest_shutdown_rx.changed().await.is_err() {
                                    break;
                                }
                            }
                            tracing::info!("REST server received shutdown signal")
                        },
                        Some(Duration::from_secs(1)),
                    )
                    .await
            });

            if chitchat_tx.send(Some(chitchat_ref.clone())).is_err() {
                tracing::warn!("No receivers for chitchat updates");
            }
            if let Some(tx) = first_tx.take() {
                let _ = tx.send(true);
            }

            let mut reload = true;
            tokio::select! {
                res = config_rx.changed() => {
                    if res.is_err() {
                        tracing::error!("Thread serving ChitChat aborted, watch channel closed");
                        reload = false;
                    }
                }
                res = graceful_shutdown_rx.changed() => {
                    tracing::info!("Received graceful shutdown signal, stopping chitchat");
                    if res.is_err() || *graceful_shutdown_rx.borrow() {
                        tracing::info!("Received shutdown signal, stopping chitchat");
                        reload = false;
                    }
                }
            }

            // REST server graceful shutdown
            if rest_shutdown_tx.send(true).is_err() {
                // If this error occurs we will proceed without REST server, this is better then abort whole application
                tracing::error!("REST server can't receive shutdown signal");
            }
            if let Err(e) = rest_server_handle.await {
                tracing::error!("Stopping REST server: {e:?}");
            }

            // Abort ChitChat server
            chitchat_handle.abort();
            while !chitchat_handle.is_finished() {
                tracing::debug!("Waiting for ChitChat to stop");
                tokio::time::sleep(Duration::from_micros(200)).await
            }
            if reload {
                tracing::trace!("REST and ChitChat stopped and will be restarted");
            } else {
                tracing::trace!("REST and ChitChat stopped.");
                return Ok(());
            }
        }
    });
    first_rx.await.context("Listener task failed before sending value")?;
    Ok((chitchat_rx, run_gossip_handle))
}

pub fn sign_gossip_node(node_state: &mut NodeState, key: transport_layer::SigningKey) {
    let signature = key.sign(&bytes_to_sign(node_state.key_values()));
    node_state
        .set(PUBKEY_SIGNATURE_KEY, pubkey_signature_to_string(&key.verifying_key(), &signature));
}

pub fn pubkey_signature_to_string(
    pubkey: &transport_layer::VerifyingKey,
    signature: &transport_layer::Signature,
) -> String {
    let mut buf = [0u8; 32 + 64];
    buf[..32].copy_from_slice(&pubkey.as_bytes()[..]);
    buf[32..].copy_from_slice(&signature.to_bytes()[..]);
    base64::engine::general_purpose::STANDARD.encode(buf)
}

pub fn bytes_to_sign<'kv>(key_values: impl Iterator<Item = (&'kv str, &'kv str)>) -> Vec<u8> {
    let mut data = Vec::new();
    for (k, v) in key_values.sorted_by_key(|x| x.0) {
        if k != PUBKEY_SIGNATURE_KEY {
            data.extend_from_slice(k.as_bytes());
            data.push(0);
            data.extend_from_slice(v.as_bytes());
            data.push(0);
        }
    }
    data
}
