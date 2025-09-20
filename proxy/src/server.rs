use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use chitchat::transport::UdpTransport;
use clap::Parser;
use ed25519_dalek::VerifyingKey;
use gossip::GossipConfig;
use network::config::NetworkConfig;
use network::metrics::NetMetrics;
use network::pub_sub::connection::IncomingMessage;
use network::pub_sub::connection::MessageDelivery;
use network::pub_sub::connection::OutgoingMessage;
use network::pub_sub::spawn_critical_task;
use network::pub_sub::IncomingSender;
use network::resolver::watch_gossip;
use network::resolver::SubscribeStrategy;
use network::resolver::WatchGossipConfig;
use network::DeliveryPhase;
use network::SendMode;
use opentelemetry::global;
use telemetry_utils::TokioMetrics;
use tokio::task::JoinHandle;
use transport_layer::msquic::MsQuicTransport;
use transport_layer::TlsCertCache;

use crate::bk_set_watcher;
use crate::config::config_reload_handler;
use crate::config::ProxyConfig;

const BK_SET_WATCH_INTERVAL_SECS: u64 = 5;
const BK_SET_REQUEST_TIMEOUT_SECS: u64 = 1;

pub static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{}
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

// Acki Nacki Proxy CLI
#[derive(Parser, Debug)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
pub struct CliArgs {
    #[arg(short, long, default_value = "config.yaml")]
    pub config: PathBuf,

    #[arg(long, env, default_value_t = 1000)]
    pub max_connections: usize,
}

pub fn run() -> anyhow::Result<()> {
    println!("{}", LONG_VERSION.as_str());
    eprintln!("Starting server...");
    dotenvy::dotenv().ok(); // ignore all errors and load what we can

    tracing::info!("Starting...");

    tracing::debug!("Installing default crypto provider...");
    if let Err(err) = rustls::crypto::ring::default_provider().install_default() {
        anyhow::bail!("Failed to install default crypto provider: {:?}", err);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")?;

    runtime.block_on(tokio_main())?;

    Ok(())
}

async fn tokio_main() -> anyhow::Result<()> {
    let args = CliArgs::parse();
    tracing::info!("Config path: {}", args.config.as_path().display());

    // Initialize the meter provider for OpenTelemetry
    let meter_provider = telemetry_utils::init_meter_provider();
    global::set_meter_provider(meter_provider.clone());

    // Create a NetMetrics instance using the meter provider
    let net_metrics = Some(NetMetrics::new(&global::meter("node")));
    let _tokio_metrics = TokioMetrics::new(&global::meter("node"));

    let result = args.run(net_metrics).await;

    // Shutdown the meter provider gracefully
    meter_provider.shutdown().ok();
    result
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NodeId(String);

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for NodeId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl CliArgs {
    async fn run(self, net_metrics: Option<NetMetrics>) -> anyhow::Result<()> {
        let tls_cert_cache = TlsCertCache::new()?;
        let config = ProxyConfig::from_file(&self.config)?;
        tracing::info!("Loaded configuration: {}", serde_json::to_string_pretty(&config)?);
        let shutdown_tx = tokio::sync::watch::channel(false).0;
        let transport = MsQuicTransport::new();

        let (config_tx, config_rx) = tokio::sync::watch::channel(config.clone());
        let (network_config_tx, network_config_rx) =
            tokio::sync::watch::channel(config.network_config(Some(tls_cert_cache.clone()))?);
        let (gossip_config_tx, gossip_config_rx) =
            tokio::sync::watch::channel(config.gossip.clone());

        let (watch_gossip_config_tx, watch_gossip_config_rx) =
            tokio::sync::watch::channel(WatchGossipConfig { trusted_pubkeys: HashSet::new() });
        let (subscribe_tx, subscribe_rx) = tokio::sync::watch::channel(Vec::new());
        let (peers_tx, _) = tokio::sync::watch::channel(HashMap::new());

        let (gossip_handle, gossip_rest_handle) =
            gossip::run(shutdown_tx.subscribe(), gossip_config_rx, UdpTransport).await?;
        spawn_critical_task(
            "Gossip",
            watch_gossip(
                shutdown_tx.subscribe(),
                watch_gossip_config_rx,
                SubscribeStrategy::<NodeId>::Proxy(
                    config.my_addr.unwrap_or(vec![config.gossip.advertise_addr()]),
                ),
                gossip_handle.chitchat(),
                subscribe_tx.clone(),
                peers_tx,
                None,
            ),
        );

        let (outgoing_messages_tx, _ /* we will subscribe() later */) =
            tokio::sync::broadcast::channel(100);
        let (incoming_messages_tx, incoming_messages_rx) = tokio::sync::mpsc::unbounded_channel();

        let config_reload_handle: JoinHandle<anyhow::Result<()>> =
            tokio::spawn(config_reload_handler(config_tx, self.config.clone()));

        let multiplexer_handle = tokio::spawn(message_multiplexor(
            net_metrics.clone(),
            incoming_messages_rx,
            outgoing_messages_tx.clone(),
        ));

        let pub_sub_task = tokio::spawn(network::pub_sub::run(
            shutdown_tx.subscribe(),
            network_config_rx,
            transport,
            true,
            net_metrics,
            self.max_connections,
            subscribe_rx,
            outgoing_messages_tx,
            IncomingSender::AsyncUnbounded(incoming_messages_tx),
        ));

        let client: reqwest::Client = reqwest::Client::builder()
            .pool_max_idle_per_host(1000)
            .timeout(Duration::from_secs(BK_SET_REQUEST_TIMEOUT_SECS))
            .build()
            .expect("Reqwest client can be built");

        let (bk_set_update_tx, bk_set_update_rx) =
            tokio::sync::watch::channel(HashSet::<VerifyingKey>::new());

        let bk_set_watcher_handle = tokio::spawn(bk_set_watcher::run(
            config_rx.clone(),
            bk_set_update_tx,
            client,
            BK_SET_WATCH_INTERVAL_SECS,
        ));

        tokio::spawn(dispatch_hot_reload(
            Some(tls_cert_cache.clone()),
            shutdown_tx.subscribe(),
            config_rx,
            bk_set_update_rx,
            network_config_tx,
            gossip_config_tx,
            watch_gossip_config_tx,
        ));

        tokio::select! {
            v = pub_sub_task => {
                if let Err(err) = v {
                    tracing::error!("Critical: PubSub task stopped with error: {}", err);
                }
                anyhow::bail!("PubSub task stopped");
            }
            v = multiplexer_handle => {
                if let Err(err) = v {
                    tracing::error!("Critical: Multiplexer task stopped with error: {}", err);
                }
                anyhow::bail!("Multiplexer task stopped");
            }
            v = config_reload_handle => {
                if let Err(err) = v {
                    tracing::error!("Critical: Config reload task stopped with error: {}", err);
                }
                anyhow::bail!("Config reload task stopped");
            }
            v = gossip_rest_handle => {
                if let Err(err) = v {
                    tracing::error!("Critical: Gossip REST task stopped with error: {}", err);
                }
                anyhow::bail!("Gossip REST task stopped");
            }
            v = bk_set_watcher_handle => {
                if let Err(err) = v {
                    tracing::error!("Critical: bk_set_watcher task stopped with error: {}", err);
                }
                anyhow::bail!("bk_set_watcher task stopped");
            }
        }
    }
}

async fn dispatch_hot_reload(
    tls_cert_cache: Option<TlsCertCache>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut proxy_config_rx: tokio::sync::watch::Receiver<ProxyConfig>,
    mut bk_set_rx: tokio::sync::watch::Receiver<HashSet<VerifyingKey>>,
    network_config_tx: tokio::sync::watch::Sender<NetworkConfig>,
    gossip_config_tx: tokio::sync::watch::Sender<GossipConfig>,
    watch_gossip_config_tx: tokio::sync::watch::Sender<WatchGossipConfig>,
) {
    let Some((mut network_config, mut gossip_config, mut watch_gossip_config)) =
        dispatch_configs(&proxy_config_rx, &bk_set_rx, &tls_cert_cache)
    else {
        return;
    };
    loop {
        let Some((new_network_config, new_gossip_config, new_watch_gossip_config)) =
            dispatch_configs(&proxy_config_rx, &bk_set_rx, &tls_cert_cache)
        else {
            return;
        };
        if new_network_config != network_config {
            network_config = new_network_config;
            network_config_tx.send_replace(network_config.clone());
        }
        if new_gossip_config != gossip_config {
            gossip_config = new_gossip_config;
            gossip_config_tx.send_replace(gossip_config.clone());
        }
        if new_watch_gossip_config != watch_gossip_config {
            watch_gossip_config = new_watch_gossip_config;
            watch_gossip_config_tx.send_replace(watch_gossip_config.clone());
        }
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                return;
            },
            sender = proxy_config_rx.changed() => if sender.is_err() {
                return;
            },
            sender = bk_set_rx.changed() => if sender.is_err() {
                return;
            },
        }
    }
}

fn dispatch_configs(
    proxy_config_rx: &tokio::sync::watch::Receiver<ProxyConfig>,
    bk_set_rx: &tokio::sync::watch::Receiver<HashSet<VerifyingKey>>,
    tls_cert_cache: &Option<TlsCertCache>,
) -> Option<(NetworkConfig, GossipConfig, WatchGossipConfig)> {
    let config = proxy_config_rx.borrow();
    let mut network_config = match config.network_config(tls_cert_cache.clone()) {
        Ok(config) => config,
        Err(err) => {
            tracing::error!("Failed to load network config: {}", err);
            return None;
        }
    };
    let mut bk_set = bk_set_rx.borrow().clone();
    bk_set.extend(network_config.credential.trusted_pubkeys.iter().cloned());
    bk_set.extend(network_config.credential.my_cert_pubkeys().unwrap_or_default());
    let trusted_pubkeys = bk_set.clone();
    network_config.credential.trusted_pubkeys = trusted_pubkeys.clone();
    Some((network_config, config.gossip.clone(), WatchGossipConfig { trusted_pubkeys }))
}

async fn message_multiplexor(
    metrics: Option<NetMetrics>,
    mut incoming_messages: tokio::sync::mpsc::UnboundedReceiver<IncomingMessage>,
    outgoing_messages: tokio::sync::broadcast::Sender<OutgoingMessage>,
) -> anyhow::Result<()> {
    tracing::info!("Proxy multiplexor bridge started");
    loop {
        match incoming_messages.recv().await {
            Some(incoming) => {
                let label = incoming.message.label.clone();
                tracing::debug!(
                    msg_type = label,
                    msg_id = incoming.message.id,
                    "Proxy multiplexor forwarded incoming"
                );
                metrics.as_ref().inspect(|x| {
                    x.finish_delivery_phase(
                        DeliveryPhase::IncomingBuffer,
                        1,
                        &label,
                        SendMode::Broadcast,
                        incoming.duration_after_transfer.elapsed(),
                    )
                });
                if let Ok(sent_count) = outgoing_messages.send(OutgoingMessage {
                    delivery: MessageDelivery::BroadcastExcluding(incoming.connection_info),
                    message: incoming.message,
                    duration_before_transfer: Instant::now(),
                }) {
                    metrics.as_ref().inspect(|x| {
                        x.start_delivery_phase(
                            DeliveryPhase::OutgoingBuffer,
                            sent_count,
                            &label,
                            SendMode::Broadcast,
                        )
                    });
                }
            }
            None => {
                tracing::info!("Proxy multiplexor bridge stopped");
                break;
            }
        }
    }
    Ok(())
}
