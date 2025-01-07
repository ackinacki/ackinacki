use std::process::exit;
use std::thread;

use clap::Parser;

pub mod cli;
pub mod connection;
pub mod executor;
pub mod wtransport_server;

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
    let args = cli::CliArgs::parse();

    tracing::info!("Config path: {}", args.config_path.as_path().display());

    // NOTE: doesn't catch panic!
    if let Err(err) = executor::execute(args).await {
        tracing::error!("{err}");
        exit(1);
    }

    exit(0);
}
