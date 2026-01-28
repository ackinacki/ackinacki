use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use signal_hook::consts::SIGHUP;
use signal_hook::consts::SIGINT;
use signal_hook::consts::SIGTERM;
use signal_hook::iterator::Signals;

use crate::bls::gosh_bls;
use crate::bls::GoshBLS;
use crate::config::load_config_from_file;
use crate::config::Config;
use crate::helper::key_handling::key_pairs_from_file;
use crate::types;

/// Initialize and spawn the signals join handle.
pub fn init_signals_join_handle(
    blk_key_path: String,
    config_path: PathBuf,
    config_tx: tokio::sync::watch::Sender<Config>,
    bls_keys_map_clone: Arc<Mutex<HashMap<gosh_bls::PubKey, (gosh_bls::Secret, types::RndSeed)>>>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let blk_key_path_str = blk_key_path;
    let config_path_buf = config_path;
    let config_tx = config_tx.clone();
    let bls_keys_map_clone = bls_keys_map_clone.clone();

    std::thread::Builder::new().name("signal handler".to_string()).spawn(move || {
        let mut signals =
            Signals::new([SIGHUP, SIGINT, SIGTERM]).expect("Failed to create Signals");
        for sig in signals.forever() {
            tracing::info!(target: "monit", "Received signal {:?}", sig);
            match sig {
                SIGHUP => {
                    let new_key_map: HashMap<gosh_bls::PubKey, (gosh_bls::Secret, types::RndSeed)> =
                        key_pairs_from_file::<GoshBLS>(&blk_key_path_str);
                    tracing::trace!(
                        target: "monit",
                        "Insert key pair, pubkeys: {:?}",
                        new_key_map.keys().collect::<Vec<_>>()
                    );
                    let mut keys_map = bls_keys_map_clone.lock();
                    *keys_map = new_key_map;
                    ext_messages_auth::auth::update_ext_message_auth_flag_from_files();
                    match load_config_from_file(&config_path_buf) {
                        Ok(config) => {
                            let _ = config_tx.send_replace(config);
                        }
                        Err(err) => {
                            tracing::error!("Failed to load config from file: {err:?}");
                        }
                    }
                }
                SIGTERM | SIGINT => {
                    break;
                }
                _ => {}
            }
        }
    })
}
