// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io;
use std::process::exit;
use std::thread::JoinHandle;

pub fn init_signals() -> io::Result<JoinHandle<()>> {
    let mut signals = signal_hook::iterator::Signals::new(signal_hook::consts::TERM_SIGNALS)?;

    std::thread::Builder::new().name("signal_handler".into()).spawn(move || {
        for signal in &mut signals {
            match signal {
                signal_hook::consts::SIGTERM => {
                    tracing::warn!("Received SIGTERM signal");
                    exit(1);
                }
                signal_hook::consts::SIGINT => {
                    tracing::warn!("Received SIGINT signal");
                    exit(1);
                }
                _ => {}
            }
        }
    })
}
