// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::time::Duration;
use std::time::SystemTime;

use chitchat::spawn_chitchat;
use chitchat::ChitchatConfig;
use chitchat::ChitchatHandle;
use chitchat::ChitchatId;
use chitchat::ChitchatRef;
use chitchat::ClusterStateSnapshot;
use chitchat::FailureDetectorConfig;
use cool_id_generator::Size;
use poem::listener::TcpListener;
use poem::Route;
use poem::Server;
use poem_openapi::param::Query;
use poem_openapi::payload::Json;
use poem_openapi::OpenApi;
use poem_openapi::OpenApiService;
use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinHandle;

static DEFAULT_GOSSIP_INTERVAL: Duration = Duration::from_millis(500);

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

    /// Sets a key-value pair on this node (without validation).
    #[oai(path = "/set_kv/", method = "get")]
    async fn set_kv(&self, key: Query<String>, value: Query<String>) -> Json<serde_json::Value> {
        let mut chitchat_guard = self.chitchat.lock();

        let cc_state = chitchat_guard.self_node_state();
        cc_state.set(key.as_str(), value.as_str());

        Json(serde_json::to_value(&SetKeyValueResponse { status: true }).unwrap())
    }
}

fn generate_server_id(public_addr: SocketAddr) -> String {
    let cool_id = cool_id_generator::get_id(Size::Medium);
    format!("server:{public_addr}-{cool_id}")
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
}

fn default_gossip_listen_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 10000))
}

fn default_chitchat_cluster_id() -> String {
    "acki_nacki".to_string()
}

pub async fn run(
    _shutdown_rx: tokio::sync::watch::Receiver<bool>,
    config_rx: tokio::sync::watch::Receiver<GossipConfig>,
    transport: impl chitchat::transport::Transport,
) -> anyhow::Result<(ChitchatHandle, JoinHandle<anyhow::Result<()>>)> {
    let config = config_rx.borrow().clone();
    let node_id = generate_server_id(config.advertise_addr());
    let generation = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
    let chitchat_id = ChitchatId::new(node_id, generation, config.advertise_addr());
    let chitchat_config = ChitchatConfig {
        cluster_id: config.cluster_id.clone(),
        chitchat_id,
        gossip_interval: DEFAULT_GOSSIP_INTERVAL,
        listen_addr: config.listen_addr,
        seed_nodes: config.seeds.iter().map(|x| x.to_string()).collect(),
        failure_detector_config: FailureDetectorConfig::default(),
        marked_for_deletion_grace_period: Duration::from_secs(600),
        catchup_callback: None,
        extra_liveness_predicate: None,
    };

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
        Server::new(TcpListener::bind(config.listen_addr)).run(app).await.map_err(|err| err.into())
    });

    Ok((chitchat_handle, rest_server_handle))
}
