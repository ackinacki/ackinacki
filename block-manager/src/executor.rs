// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::Arc;

use message_router::message_router::MessageRouter;
use message_router::message_router::MessageRouterConfig;
use message_router::read_keys_from_file;
use network::try_parse_socket_addr;
use parking_lot::Mutex;
use salvo::conn::TcpListener;
use salvo::Listener;
use salvo::Server;
use telemetry_utils::get_metrics_endpoint;
use telemetry_utils::init_meter_provider;

use crate::block_subscriber;
use crate::block_subscriber::WorkerCommand;
use crate::bp_resolver::BPResolverImpl;
use crate::cli::Args;
use crate::events;
use crate::metrics::Metrics;
use crate::rest_api_routes::rest_api_router;

pub struct AppState {
    pub default_bp: SocketAddr,
    pub message_router: Arc<MessageRouter>,
    pub last_block_gen_utime: AtomicU64,
}

pub async fn execute(
    args: Args,
    cmd_tx: mpsc::Sender<WorkerCommand>,
    cmd_rx: mpsc::Receiver<WorkerCommand>,
) -> anyhow::Result<()> {
    // Init metrics
    let metrics = if let Some(endpoint) = get_metrics_endpoint() {
        tracing::info!("Using OTLP metrics endpoint: {endpoint}");
        opentelemetry::global::set_meter_provider(init_meter_provider());
        Some(Metrics::new(&opentelemetry::global::meter("bm")))
    } else {
        tracing::info!("No OTEL exporter endpoint found, metrics not collected.");
        None
    };

    // event bus
    let (event_pub, _event_sub) = channel::<events::Event>();
    // pass BP data (thread => IP addresses)
    let (bp_data_tx, bp_data_rx) = channel::<(String, Vec<String>)>();

    // message router
    let Ok(bind) = std::env::var("BLOCK_MANAGER_API") else {
        anyhow::bail!("BLOCK_MANAGER_API environment variable must be set");
    };

    let socket_addr = parse_socket_address(
        args.stream_src_url.host_str().expect("Host required"),
        args.stream_src_url.port_or_known_default().expect("Port required"),
    )?;

    let default_bp = try_parse_socket_addr(
        std::env::var("DEFAULT_BP").expect("DEFAULT_BP environment variable must be set"),
        crate::DEFAULT_BP_PORT,
    )
    .unwrap();

    let bp_resolver = Arc::new(Mutex::new(BPResolverImpl::new(default_bp)));
    BPResolverImpl::start_listener(Arc::clone(&bp_resolver), bp_data_rx)?;

    let owner_wallet_pubkey = std::env::var("BM_OWNER_WALLET_PUBKEY").ok();

    let config = MessageRouterConfig {
        bp_resolver: bp_resolver.clone(),
        owner_wallet_pubkey,
        signing_keys: std::env::var("BM_ISSUER_KEYS_FILE")
            .ok()
            .and_then(|path| read_keys_from_file(&path).ok()),
    };
    // Create an instance of MessageRouter. It won't start
    let message_router = MessageRouter::new(bind, config);

    // REST API server
    let tcp_listener = TcpListener::new(args.rest_api);
    let acceptor = tcp_listener.try_bind().await?;

    let app_state = Arc::new(AppState {
        default_bp,
        message_router: Arc::new(message_router),
        last_block_gen_utime: AtomicU64::new(0),
    });

    let app_state_clone = app_state.clone();
    let rest_api_server_handler = tokio::spawn(async move {
        Server::new(acceptor).serve(rest_api_router(app_state_clone)).await;
    });

    // block subscriber
    let block_subscriber = block_subscriber::BlockSubscriber::new(
        args.sqlite_path,
        socket_addr,
        event_pub.clone(),
        bp_data_tx,
    );

    let block_subscriber_handler = block_subscriber.run(app_state, metrics, cmd_tx, cmd_rx);

    tokio::select! {
        _ = block_subscriber_handler => {
            anyhow::bail!("block_subscriber_handler exited")
        },
        _= rest_api_server_handler => {
             anyhow::bail!("rest_api_server_handler exited")
        },

    }
}

fn parse_socket_address(hostname: &str, port: u16) -> std::io::Result<SocketAddr> {
    let address = (hostname, port);
    // Try to resolve the hostname
    let mut addrs_iter = address.to_socket_addrs()?;
    addrs_iter.next().ok_or_else(|| std::io::Error::other("No address found for hostname"))
}
