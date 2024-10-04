// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use chitchat::spawn_chitchat;
use chitchat::transport::UdpTransport;
use chitchat::Chitchat;
use chitchat::ChitchatConfig;
use chitchat::ChitchatId;
use chitchat::FailureDetectorConfig;
use cool_id_generator::Size;
use gossip::ApiResponse;
use poem::listener::TcpListener;
use poem::Route;
use poem::Server;
use poem_openapi::param::Query;
use poem_openapi::payload::Json;
use poem_openapi::OpenApi;
use poem_openapi::OpenApiService;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tokio::time::sleep;

struct Api {
    chitchat: Arc<Mutex<Chitchat>>,
}

#[OpenApi]
impl Api {
    /// Chitchat state
    #[oai(path = "/", method = "get")]
    async fn index(&self) -> Json<serde_json::Value> {
        let chitchat_guard = self.chitchat.lock().await;
        let response = ApiResponse {
            cluster_id: chitchat_guard.cluster_id().to_string(),
            cluster_state: chitchat_guard.state_snapshot(),
            live_nodes: chitchat_guard.live_nodes().cloned().collect::<Vec<_>>(),
            dead_nodes: chitchat_guard.dead_nodes().cloned().map(|node| node.0).collect::<Vec<_>>(),
        };
        Json(serde_json::to_value(&response).unwrap())
    }

    /// Sets a key-value pair on this node (without validation).
    #[oai(path = "/set_kv/", method = "get")]
    async fn set_kv(&self, key: Query<String>, value: Query<String>) -> Json<serde_json::Value> {
        let mut chitchat_guard = self.chitchat.lock().await;

        let cc_state = chitchat_guard.self_node_state();
        cc_state.set(key.as_str(), value.as_str());

        Json(serde_json::to_value(&gossip::SetKeyValueResponse { status: true }).unwrap())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "chitchat", about = "Chitchat test server.")]
struct Opt {
    /// Defines the socket addr on which we should listen to.
    #[structopt(long = "listen_addr", default_value = "127.0.0.1:10000")]
    listen_addr: SocketAddr,
    /// Defines the socket address (host:port) other servers should use to
    /// reach this server.
    ///
    /// It defaults to the listen address, but this is only valid
    /// when all server are running on the same server.
    #[structopt(long = "public_addr")]
    public_addr: Option<SocketAddr>,

    /// Node ID. Must be unique. If None, the node ID will be generated from
    /// the public_addr and a random suffix.
    #[structopt(long = "node_id")]
    node_id: Option<String>,

    #[structopt(long = "seed")]
    seeds: Vec<String>,

    #[structopt(long = "interval_ms", default_value = "500")]
    interval: u64,
}

fn generate_server_id(public_addr: SocketAddr) -> String {
    let cool_id = cool_id_generator::get_id(Size::Medium);
    format!("server:{public_addr}-{cool_id}")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();
    let public_addr = opt.public_addr.unwrap_or(opt.listen_addr);
    let node_id = opt.node_id.unwrap_or_else(|| generate_server_id(public_addr));
    let generation = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
    let chitchat_id = ChitchatId::new(node_id, generation, public_addr);
    let config = ChitchatConfig {
        cluster_id: "testing".to_string(),
        chitchat_id,
        gossip_interval: Duration::from_millis(opt.interval),
        listen_addr: opt.listen_addr,
        seed_nodes: opt.seeds.clone(),
        failure_detector_config: FailureDetectorConfig::default(),
        marked_for_deletion_grace_period: 10_000,
    };
    let chitchat_handler = spawn_chitchat(config, Vec::new(), &UdpTransport).await?;
    let chitchat = chitchat_handler.chitchat();
    let api = Api { chitchat: chitchat.clone() };

    {
        let mut chitchat_guard = chitchat.lock().await;
        let cc_state = chitchat_guard.self_node_state();
        cc_state.set("bind", public_addr);
    }

    let api_service = OpenApiService::new(api, "Hello World", "1.0")
        .server(format!("http://{}/", opt.listen_addr));
    let docs = api_service.swagger_ui();
    let app = Route::new().nest("/", api_service).nest("/docs", docs);
    let _thread = tokio::spawn(async move {
        loop {
            let chitchat_guard = chitchat.lock().await;
            let live_nodes = chitchat_guard
                .live_nodes()
                .map(|n| n.node_id.clone())
                .filter(|node_id| *node_id != chitchat_guard.self_chitchat_id().node_id)
                .collect::<HashSet<_>>();
            tracing::info!("Live nodes: {live_nodes:?}");
            let binds = chitchat_guard
                .state_snapshot()
                .node_state_snapshots
                .iter()
                .filter(|snapshot| live_nodes.contains(&snapshot.chitchat_id.node_id))
                .filter_map(|snapshot| snapshot.node_state.get("bind"))
                .filter_map(|bind| SocketAddr::from_str(bind).ok())
                .collect::<Vec<_>>();
            tracing::info!("Binds: {binds:?}");
            sleep(Duration::from_secs(1)).await;
        }
    });
    Server::new(TcpListener::bind(&opt.listen_addr)).run(app).await?;
    Ok(())
}
