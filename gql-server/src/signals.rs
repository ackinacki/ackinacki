// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io;
use std::sync::mpsc;
use std::thread::JoinHandle;

use signal_hook::consts::SIGHUP;

pub enum WorkerCommand {
    ResolveArchives,
}

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
    let mut signals = signal_hook::iterator::Signals::new([SIGHUP])?;
    std::thread::Builder::new().name("signal_handler".into()).spawn(move || {
        for signal in &mut signals {
            match signal {
                signal_hook::consts::SIGHUP => {
                    tracing::warn!("Received SIGHUP signal");
                    if tx.send(WorkerCommand::ResolveArchives).is_err() {
                        tracing::error!("Failed to send ResolveArchives command to worker");
                    }
                }
                _ => {
                    tracing::warn!("Received unhandled signal: {}", signal);
                }
            }
        }
    })
}
