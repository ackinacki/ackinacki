// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io;
use std::process::exit;
use std::sync::mpsc;
use std::thread::JoinHandle;

use signal_hook::consts::SIGHUP;
use signal_hook::consts::SIGINT;
use signal_hook::consts::SIGTERM;

use crate::domain::models::WorkerCommand;

pub fn dispatch_signals(
    tx: mpsc::Sender<WorkerCommand>,
) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    tokio::task::spawn_blocking(move || {
        run(tx)
            .expect("spawn block-subscriber worker")
            .join()
            .map_err(|err| anyhow::anyhow!("signal handler thread: {err:?}"))
    })
}

fn run(tx: mpsc::Sender<WorkerCommand>) -> io::Result<JoinHandle<()>> {
    let mut signals = signal_hook::iterator::Signals::new([SIGHUP, SIGINT, SIGTERM])?;
    std::thread::Builder::new().name("signal_handler".into()).spawn(move || {
        for signal in &mut signals {
            match signal {
                signal_hook::consts::SIGTERM => {
                    tracing::warn!("Received SIGTERM signal");
                    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel::<()>();
                    let mut exit_code = 1;
                    if tx.send(WorkerCommand::Shutdown(shutdown_complete_tx)).is_err() {
                        tracing::error!("Failed to send shutdown signal to worker");
                    } else if let Err(err) = shutdown_complete_rx.recv() {
                        tracing::error!("Worker did not confirm shutdown: {err:?})");
                    } else {
                        tracing::info!(target: "monit", "Database shutdown completed gracefully");
                        exit_code = 0;
                    }
                    exit(exit_code)
                }
                signal_hook::consts::SIGINT => {
                    tracing::warn!("Received SIGINT signal");
                    exit(1);
                }
                signal_hook::consts::SIGHUP => {
                    tracing::warn!("Received SIGHUP signal");
                    if tx.send(WorkerCommand::RotateDb).is_err() {
                        tracing::error!("Failed to send RotateDb command to worker");
                    }
                }
                _ => {
                    tracing::warn!("Received unhandled signal: {}", signal);
                }
            }
        }
    })
}
