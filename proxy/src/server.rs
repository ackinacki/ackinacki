use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::exit;
use std::sync::LazyLock;
use std::thread;

use anyhow::Context;
use clap::Parser;
use network::pub_sub::Config;
use network::pub_sub::IncomingMessage;
use network::pub_sub::MessageDelivery;
use network::pub_sub::OutgoingMessage;
use tokio::signal::unix::signal;
use tokio::signal::unix::SignalKind;
use tokio::task::JoinHandle;

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

pub fn run() -> Result<(), std::io::Error> {
    eprintln!("Starting server...");
    dotenvy::dotenv().ok(); // ignore all errors and load what we can
    crate::tracing::init_tracing();
    tracing::info!("Starting...");

    tracing::debug!("Installing default crypto provider...");
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    if let Err(err) = thread::Builder::new().name("tokio_main".into()).spawn(tokio_main)?.join() {
        tracing::error!("tokio main thread panicked: {:#?}", err);
        exit(1);
    }

    exit(0);
}

#[tokio::main]
async fn tokio_main() -> anyhow::Result<()> {
    let args = CliArgs::parse();

    tracing::info!("Config path: {}", args.config.as_path().display());

    if let Err(err) = args.run().await {
        tracing::error!("{:#?}", err);
        exit(1);
    }

    exit(0);
}

impl CliArgs {
    async fn run(self) -> anyhow::Result<()> {
        let config = load_config(&self.config)?;
        let (config_updates_sender, config_updates_receiver) = tokio::sync::watch::channel(config);

        let (outgoing_messages_sender, _ /* we will subscribe() later */) =
            tokio::sync::broadcast::channel(100);
        let (incoming_messages_sender, incoming_messages_receiver) =
            tokio::sync::mpsc::channel(100);

        let config_reload_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn({
            let config_path = self.config.clone();
            async move { config_reload_handler(config_updates_sender, config_path).await }
        });

        let multiplexer_handle = tokio::spawn({
            let outgoing_messages_sender = outgoing_messages_sender.clone();
            async move {
                message_multiplexor(incoming_messages_receiver, outgoing_messages_sender).await
            }
        });

        let pub_sub_task = tokio::spawn(async move {
            network::pub_sub::run(
                self.max_connections,
                config_updates_receiver,
                outgoing_messages_sender,
                incoming_messages_sender,
            )
            .await
        });

        tokio::select! {
            v = pub_sub_task => {
                if let Err(err) = v {
                    tracing::error!("PubSub task stopped with error: {}", err);
                }
                anyhow::bail!("PubSub task stopped");
            }
            v = multiplexer_handle => {
                if let Err(err) = v {
                    tracing::error!("Multiplexer task stopped with error: {}", err);
                }
                anyhow::bail!("Multiplexer task stopped");
            }
            v = config_reload_handle => {
                if let Err(err) = v {
                    tracing::error!("Config reload task stopped with error: {}", err);
                }
                anyhow::bail!("Config reload task stopped");
            }
        }
    }
}

async fn message_multiplexor(
    mut incoming_messages: tokio::sync::mpsc::Receiver<IncomingMessage>,
    outgoing_messages: tokio::sync::broadcast::Sender<OutgoingMessage>,
) -> anyhow::Result<()> {
    tracing::info!("Message multiplexor started");
    loop {
        match incoming_messages.recv().await {
            Some(message) => {
                tracing::debug!("Message received: {:?}", message);
                let outgoing_message = OutgoingMessage {
                    delivery: MessageDelivery::BroadcastExcluding(message.peer),
                    data: message.data,
                };
                let _ = outgoing_messages.send(outgoing_message);
            }
            None => {
                tracing::info!("Message multiplexor stopped");
                break;
            }
        }
    }
    Ok(())
}

async fn config_reload_handler(
    config_updates: tokio::sync::watch::Sender<Config>,
    config_path: PathBuf,
) -> anyhow::Result<()> {
    // 1. config error shouldn't be an error
    // 2. every other error should panic
    let mut sig_hup = signal(SignalKind::hangup())?;
    loop {
        sig_hup.recv().await;
        tracing::info!("Received SIGHUP, reloading configuration...");

        match load_config(&config_path) {
            Ok(new_config_state) => {
                config_updates.send(new_config_state).expect("failed to send config")
            }
            Err(err) => {
                tracing::error!(
                    "Failed to load new configuration at {}: {:?}",
                    config_path.display(),
                    err
                );
            }
        };
    }
}

pub fn load_config(path: impl AsRef<Path>) -> anyhow::Result<Config> {
    let file = std::fs::File::open(path.as_ref())?;
    serde_yaml::from_reader(file).context("Failed to load config")
}

pub fn save_config(config: &Config, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let file = std::fs::File::create(path.as_ref())?;
    let mut writer = std::io::BufWriter::new(file);
    serde_yaml::to_writer(&mut writer, config).context("Failed to save config")?;
    writer.flush()?;
    Ok(())
}
