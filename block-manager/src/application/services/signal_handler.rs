// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io;
use std::process::exit;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use signal_hook::consts::SIGHUP;
use signal_hook::consts::SIGINT;
use signal_hook::consts::SIGTERM;

use crate::domain::models::WorkerCommand;

pub fn dispatch_signals(
    tx: mpsc::Sender<WorkerCommand>,
) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    tokio::task::spawn_blocking(move || {
        let handle = run(tx).expect("spawn block-subscriber worker");
        let inner_res =
            handle.join().map_err(|err| anyhow::anyhow!("signal handler thread: {err:?}"))?;
        inner_res
    })
}

fn run(tx: mpsc::Sender<WorkerCommand>) -> io::Result<JoinHandle<Result<(), anyhow::Error>>> {
    let mut signals = signal_hook::iterator::Signals::new([SIGHUP, SIGINT, SIGTERM])?;
    std::thread::Builder::new()
        .name("signal_handler".into())
        .spawn(move || -> Result<(), anyhow::Error> {
            for signal in &mut signals {
                match signal {
                    signal_hook::consts::SIGTERM => {
                        tracing::warn!("Received SIGTERM signal");
                        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel::<()>();
                        if tx.send(WorkerCommand::Shutdown(shutdown_complete_tx)).is_err() {
                            return Err(anyhow::anyhow!("Failed to send shutdown signal to worker"));
                        } else if let Err(err) = shutdown_complete_rx.recv() {
                            return Err(anyhow::anyhow!("Worker did not confirm shutdown: {err:?}"));
                        } else {
                            tracing::info!(target: "monit", "Database shutdown completed gracefully");
                            std::thread::sleep(Duration::from_secs(5));
                            break;
                        }
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
            Ok(())
        })
}
