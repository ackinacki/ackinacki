// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::process::exit;
use std::thread;

use block_manager::cli::Args;
use block_manager::signals::init_signals;
use clap::Parser;

fn main() -> Result<(), std::io::Error> {
    dotenvy::dotenv().ok(); // ignore errors
    block_manager::tracing::init_tracing();
    tracing::info!("Starting...");

    let signals = init_signals()?;

    if let Err(err) = thread::Builder::new().name("tokio_main".into()).spawn(tokio_main)?.join() {
        tracing::error!("tokio main thread panicked: {:#?}", err);
        exit(1);
    }

    if let Err(err) = signals.join() {
        tracing::error!("signal handler thread panicked: {:?}", err);
        exit(2);
    }

    exit(0);
}

#[tokio::main]
async fn tokio_main() -> anyhow::Result<()> {
    let args = Args::parse();

    if let Err(err) = block_manager::executor::execute(args).await {
        tracing::error!("{err}");
        exit(1);
    }

    exit(0);
}
