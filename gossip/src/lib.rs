// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
pub mod gossip_peer;
use std::fmt::Display;

use anyhow::Context;
use chitchat::spawn_chitchat;
use chitchat::ChitchatConfig;
use chitchat::ChitchatHandle;
use chitchat::ChitchatId;
use chitchat::ChitchatRef;
use chitchat::ClusterStateSnapshot;
use chitchat::FailureDetectorConfig;
use poem::listener::TcpListener;
use poem::Route;
use poem::Server;
use poem_openapi::payload::Json;
use poem_openapi::OpenApi;
use poem_openapi::OpenApiService;
use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::gossip_peer::GossipPeer;

pub static DEFAULT_GOSSIP_INTERVAL: Duration = Duration::from_millis(500);

static LAST_GENERATION_ID: AtomicU64 = AtomicU64::new(0);

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

    /// Stable identifier of this gossip process. If omitted, it is derived from
    /// the process role and gossip advertise address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,

    /// Time to retain a dead Chitchat member before removing its state.
    /// Defaults to 7200 seconds (2 hours).
    #[serde(default = "default_dead_node_grace_period_seconds")]
    pub dead_node_grace_period_seconds: u64,

    /// Gossip peer TTL in seconds.
    /// Defaults to 900 (15 minutes).
    /// Value 0 means no TTL.
    #[serde(default = "default_gossip_peer_ttl_seconds")]
    pub peer_ttl_seconds: u64,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_gossip_listen_addr(),
            advertise_addr: None,
            seeds: Vec::new(),
            cluster_id: default_chitchat_cluster_id(),
            instance_id: None,
            dead_node_grace_period_seconds: default_dead_node_grace_period_seconds(),
            peer_ttl_seconds: default_gossip_peer_ttl_seconds(),
        }
    }
}

impl GossipConfig {
    pub fn advertise_addr(&self) -> SocketAddr {
        self.advertise_addr.unwrap_or(self.listen_addr)
    }

    fn chitchat_config(&self, chitchat_id_prefix: &str) -> ChitchatConfig {
        let chitchat_node_id = self
            .instance_id
            .as_ref()
            .filter(|id| !id.is_empty())
            .map(|id| format!("{chitchat_id_prefix}-{id}"))
            .unwrap_or_else(|| format!("{chitchat_id_prefix}-{}", self.advertise_addr()));
        let generation = next_generation_id();

        let chitchat_id = ChitchatId::new(chitchat_node_id, generation, self.advertise_addr());

        ChitchatConfig {
            cluster_id: self.cluster_id.clone(),
            chitchat_id,
            gossip_interval: DEFAULT_GOSSIP_INTERVAL,
            listen_addr: self.listen_addr,
            seed_nodes: self.seeds.iter().map(|x| x.to_string()).collect(),
            failure_detector_config: FailureDetectorConfig {
                dead_node_grace_period: Duration::from_secs(self.dead_node_grace_period_seconds),
                ..Default::default()
            },
            marked_for_deletion_grace_period: Duration::from_secs(600),
            catchup_callback: None,
            extra_liveness_predicate: None,
        }
    }
}

fn default_gossip_listen_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 10000))
}

pub fn default_gossip_peer_ttl_seconds() -> u64 {
    60 * 15
}

pub fn default_dead_node_grace_period_seconds() -> u64 {
    60 * 60 * 2
}

fn default_chitchat_cluster_id() -> String {
    "acki_nacki".to_string()
}

fn next_generation_id() -> u64 {
    let wall_clock_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|err| {
            tracing::warn!("System time error: {err:?}");
            Duration::ZERO
        })
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);

    let mut previous = LAST_GENERATION_ID.load(Ordering::Relaxed);
    loop {
        let generation = wall_clock_millis.max(previous.saturating_add(1));
        match LAST_GENERATION_ID.compare_exchange_weak(
            previous,
            generation,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return generation,
            Err(actual) => previous = actual,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GossipReloadConfig<PeerId: FromStr<Err: Display> + Display> {
    pub gossip_config: GossipConfig,
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
    let chitchat_config = config.chitchat_config(chitchat_id_prefix);

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
    chitchat_id_prefix: &'static str,
    mut graceful_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut gossip_reload_config_rx: tokio::sync::watch::Receiver<GossipReloadConfig<PeerId>>,
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

    let run_gossip_handle = tokio::spawn(async move {
        loop {
            let gossip_reload_config = gossip_reload_config_rx.borrow().clone();
            tracing::debug!("Config updated");
            let gossip_config = gossip_reload_config.gossip_config;
            let chitchat_config = gossip_config.chitchat_config(chitchat_id_prefix);

            tracing::info!("Starting ChitChat server on {}", gossip_config.advertise_addr());

            let chitchat_handle = spawn_chitchat(chitchat_config, Vec::new(), &transport).await?;

            chitchat_handle
                .with_chitchat(|c| {
                    if let Some(self_peer) = gossip_reload_config.peer_config.clone() {
                        self_peer.update_node_state(
                            c.self_node_state(),
                            &gossip_reload_config.my_ed_key_secret,
                            &gossip_reload_config.my_ed_key_path,
                        );
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
                res = gossip_reload_config_rx.changed() => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gossip_config_uses_stable_instance_id_and_increasing_generation() {
        let config = GossipConfig::default();
        let first = config.chitchat_config("node");
        let second = config.chitchat_config("node");

        assert_eq!(first.chitchat_id.node_id, second.chitchat_id.node_id);
        assert_eq!(first.chitchat_id.node_id, "node-127.0.0.1:10000");
        assert!(second.chitchat_id.generation_id > first.chitchat_id.generation_id);
    }

    #[test]
    fn gossip_config_honors_instance_id_and_dead_node_grace_period() {
        let config = GossipConfig {
            instance_id: Some("bk-1".to_string()),
            dead_node_grace_period_seconds: 42,
            ..Default::default()
        };
        let chitchat_config = config.chitchat_config("node");

        assert_eq!(chitchat_config.chitchat_id.node_id, "node-bk-1");
        assert_eq!(
            chitchat_config.failure_detector_config.dead_node_grace_period,
            Duration::from_secs(42)
        );
    }

    #[test]
    fn gossip_config_deserialization_uses_new_defaults() {
        let config: GossipConfig = serde_json::from_str("{}").unwrap();

        assert_eq!(config.instance_id, None);
        assert_eq!(config.dead_node_grace_period_seconds, default_dead_node_grace_period_seconds());
        assert_eq!(config.dead_node_grace_period_seconds, 2 * 60 * 60);
    }
}
