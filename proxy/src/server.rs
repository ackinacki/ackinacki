use std::fmt::Display;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use network::metrics::NetMetrics;
use network::pub_sub::connection::IncomingMessage;
use network::pub_sub::connection::MessageDelivery;
use network::pub_sub::connection::OutgoingMessage;
use network::pub_sub::spawn_critical_task;
use network::pub_sub::IncomingSender;
use network::resolver::watch_gossip;
use network::resolver::SubscribeStrategy;
use network::socket_addr::ToOneSocketAddr;
use network::DeliveryPhase;
use network::SendMode;
use opentelemetry::global;
use tokio::task::JoinHandle;

use crate::config::config_reload_handler;
use crate::config::ProxyConfig;

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

// Acki Nacki Proxy CLI
#[derive(Parser, Debug)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
pub struct CliArgs {
    #[arg(short, long, default_value = "config.yaml")]
    pub config: PathBuf,

    #[arg(long, env, default_value_t = 200)]
    pub max_connections: usize,
}

pub fn run() -> anyhow::Result<()> {
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
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // Create NetMetrics instance using the meter provider
    let net_metrics = NetMetrics::new(&global::meter("proxy"));

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
    async fn run(self, net_metrics: NetMetrics) -> anyhow::Result<()> {
        let config = ProxyConfig::from_file(&self.config)?;
        tracing::info!("Loaded configuration: {}", serde_json::to_string_pretty(&config)?);

        let (subscribe_tx, _) = tokio::sync::watch::channel(Vec::new());

        let (gossip_handle, gossip_rest_handle) = if !config.subscribe.is_empty() {
            let _ = subscribe_tx.send_replace(config.subscribe.clone());
            (None, tokio::spawn(std::future::pending::<anyhow::Result<()>>()))
        } else if let (Some(gossip_config), Some(my_url)) = (&config.gossip, &config.my_url) {
            let seeds = gossip_config.get_seeds();
            tracing::info!("Gossip seeds expanded: {:?}", seeds);

            let listen_addr = gossip_config.listen_addr.clone();
            let advertise_addr = gossip_config.advertise_addr.clone().unwrap_or(listen_addr);
            let (gossip_handle, gossip_rest_handle) = gossip::run(
                gossip_config.listen_addr.try_to_socket_addr()?,
                advertise_addr.try_to_socket_addr()?,
                seeds,
                gossip_config.cluster_id.clone(),
            )
            .await?;

            spawn_critical_task(
                "Gossip",
                watch_gossip(
                    SubscribeStrategy::<NodeId>::Proxy(my_url.clone()),
                    gossip_handle.chitchat(),
                    Some(subscribe_tx.clone()),
                    None,
                ),
            );
            (Some(gossip_handle), gossip_rest_handle)
        } else {
            (None, tokio::spawn(std::future::pending::<anyhow::Result<()>>()))
        };

        let (outgoing_messages_tx, _ /* we will subscribe() later */) =
            tokio::sync::broadcast::channel(100);
        let (incoming_messages_tx, incoming_messages_rx) = tokio::sync::mpsc::unbounded_channel();

        let (config_tx, _config_rx) = tokio::sync::watch::channel(config.clone());
        let config_reload_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn({
            let config_path = self.config.clone();
            async move { config_reload_handler(config_tx, config_path).await }
        });

        let multiplexer_handle = tokio::spawn({
            let outgoing_messages_sender = outgoing_messages_tx.clone();
            let net_metrics = net_metrics.clone();
            async move {
                message_multiplexor(
                    Some(net_metrics),
                    incoming_messages_rx,
                    outgoing_messages_sender,
                )
                .await
            }
        });

        let pub_sub_task = tokio::spawn(async move {
            network::pub_sub::run(
                Some(net_metrics),
                self.max_connections,
                config.bind,
                config.tls_config(),
                subscribe_tx,
                outgoing_messages_tx,
                IncomingSender::AsyncUnbounded(incoming_messages_tx),
            )
            .await
        });

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
                drop(gossip_handle);
                anyhow::bail!("Gossip REST task stopped");
            }
        }
    }
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
                tracing::debug!("Proxy multiplexor forwarded incoming {}", label);
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
                    delivery: MessageDelivery::BroadcastExcluding(incoming.peer),
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
