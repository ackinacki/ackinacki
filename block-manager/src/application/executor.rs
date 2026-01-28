// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
use std::sync::Arc;

use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::sqlite_helper::SqliteHelperConfig;
use message_router::message_router::MessageRouter;
use message_router::message_router::MessageRouterConfig;
use parking_lot::Mutex;

use crate::application::metrics::Metrics;
use crate::application::quarantine::Quarantine;
use crate::application::services::block_applier;
use crate::application::services::block_subscriber;
use crate::application::services::http_server;
use crate::application::services::signal_handler::dispatch_signals;
use crate::domain::models::AppConfig;
use crate::domain::models::AppState;
use crate::domain::models::UpdatableBPResolver;
use crate::domain::models::WorkerCommand;

pub async fn run(
    config: AppConfig,
    bp_resolver: Arc<Mutex<dyn UpdatableBPResolver>>,
    metrics: Option<Metrics>,
) -> anyhow::Result<()> {
    let message_router = Arc::new(MessageRouter::new(
        // bind socket can be anything, because message router will not be started
        "0.0.0.0:1111".to_string(),
        MessageRouterConfig {
            bp_resolver: bp_resolver.clone(),
            owner_wallet_pubkey: config.owner_wallet_pubkey,
            signing_keys: config.signing_keys,
        },
    ));
    let app_state = Arc::new(AppState {
        message_router,
        last_block_gen_utime: AtomicU64::new(0),
        bk_api_token: config.bk_api_token,
        default_bp: config.default_bp, // used only to query accounts from node
    });

    let (cmd_tx, cmd_rx) = mpsc::channel::<WorkerCommand>();

    let signals_handle = dispatch_signals(cmd_tx.clone());

    let rest_api_handle = http_server::run(config.rest_api, app_state.clone()).await?;

    let (db_writer, db_helper_handle) = SqliteHelper::from_config(SqliteHelperConfig::new(
        config.sqlite_path.clone().into(),
        Some("bm-archive.db".into()),
    ))?;

    let quarantine = Quarantine::new(config.sqlite_path.into())?;

    let blk_apply_handle =
        block_applier::run(bp_resolver, db_writer, quarantine, app_state, metrics.clone(), cmd_rx);

    let blk_subscribe_handle = block_subscriber::run(cmd_tx, config.subscribe_socket, metrics);
    let db_helper_handle = tokio::task::spawn_blocking(move || db_helper_handle.join());
    tokio::select! {
        result = db_helper_handle => {
            match result {
                Ok(reason) => {
                    anyhow::bail!("db helper thread exited unexpectedly: {reason:?}");
                }
                Err(err) => {
                    anyhow::bail!("db helper thread panicked: {err:?}");
                }
            }
        }
        result = signals_handle => {
            match result {
                Ok(reason) => {
                    anyhow::bail!("signal handler exited unexpectedly: {reason:?}");
                }
                Err(err) => {
                    anyhow::bail!("signal handler panicked: {err:?}");

                }
            }
        }
        result = blk_apply_handle=> {
            match result {
                Ok(reason) => {
                    anyhow::bail!("block_applier exited unexpectedly: {reason:?}");
                }
                Err(err) => {
                    anyhow::bail!("block_applier panicked: {err:?}");

                }
            }
        },
        result = blk_subscribe_handle => {
            match result {
                Ok(()) => {
                    anyhow::bail!("block subscriber exited unexpectedly!");
                }
                Err(err) => {
                    anyhow::bail!("block subscriber panicked: {err:?}");
                }
            }
        },
        result = rest_api_handle => {
            match result {
                Ok(()) => {
                    anyhow::bail!("rest_api server exited unexpectedly");
                }
                Err(err) => {
                    anyhow::bail!("rest_api server panicked: {err:?}");
                }
            }
        }
    }
}
