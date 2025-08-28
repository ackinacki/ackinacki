// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io::Write;
use std::process::exit;
use std::sync::mpsc;
use std::thread;

use block_manager::block_subscriber::WorkerCommand;
use block_manager::cli::Args;
use block_manager::signals::init_signals;
use clap::Parser;

fn main() -> Result<(), std::io::Error> {
    dotenvy::dotenv().ok(); // ignore errors
    block_manager::tracing::init_tracing();
    tracing::info!("Starting...");

    std::panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::force_capture();

        let crash_log = format!("panicked: {panic_info}\nBacktrace:\n{backtrace}");
        eprintln!("{crash_log}");

        if let Ok(mut file) =
            std::fs::OpenOptions::new().create(true).append(true).open("crash.log")
        {
            let _ = writeln!(file, "{crash_log}");
        }

        std::process::exit(100);
    }));

    let (cmd_tx, cmd_rx) = mpsc::channel::<WorkerCommand>();

    let signals = init_signals(cmd_tx.clone())?;

    if let Err(err) = thread::Builder::new()
        .name("tokio_main".into())
        .spawn(move || tokio_main(cmd_tx, cmd_rx))?
        .join()
    {
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
async fn tokio_main(
    cmd_tx: mpsc::Sender<WorkerCommand>,
    cmd_rx: mpsc::Receiver<WorkerCommand>,
) -> anyhow::Result<()> {
    let args = Args::parse();

    if let Err(err) = block_manager::executor::execute(args, cmd_tx, cmd_rx).await {
        tracing::error!("{err}");
        exit(1);
    }

    exit(0);
}
