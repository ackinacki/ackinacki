// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io;
use std::process::exit;
use std::sync::mpsc;
use std::thread::JoinHandle;

use signal_hook::consts::SIGHUP;
use signal_hook::consts::SIGINT;
use signal_hook::consts::SIGTERM;

use crate::block_subscriber::WorkerCommand;

pub fn init_signals(tx: mpsc::Sender<WorkerCommand>) -> io::Result<JoinHandle<()>> {
    let mut signals = signal_hook::iterator::Signals::new([SIGHUP, SIGINT, SIGTERM])?;

    std::thread::Builder::new().name("signal_handler".into()).spawn(move || {
        for signal in &mut signals {
            match signal {
                signal_hook::consts::SIGTERM => {
                    tracing::warn!("Received SIGTERM signal");
                    if tx.send(WorkerCommand::Shutdown).is_err() {
                        tracing::error!("Failed to prepare DB for shutdown");
                    }
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    exit(1);
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
