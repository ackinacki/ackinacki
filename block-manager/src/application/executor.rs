// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
use std::sync::Arc;

use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::sqlite_helper::SqliteHelperConfig;
use message_router::message_router::MessageRouter;
use message_router::message_router::MessageRouterConfig;
use node::repository::accounts::NodeThreadAccountsRepository;
use parking_lot::Mutex;
use tokio::signal::unix::SignalKind;
use tokio::sync::watch;

use crate::application::metrics::Metrics;
use crate::application::quarantine::Quarantine;
use crate::application::services::block_applier;
use crate::application::services::block_subscriber;
use crate::application::services::connection_pool;
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
    thread_accounts_repository: NodeThreadAccountsRepository,
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
    let bk_api_pool = crate::domain::models::BkApiPool::new(config.bk_api_endpoints);
    let app_state = Arc::new(AppState {
        message_router,
        last_block_gen_utime: AtomicU64::new(0),
        bk_api_token: config.bk_api_token,
        bk_api_pool,
    });

    let (cmd_tx, cmd_rx) = mpsc::channel::<WorkerCommand>();

    let signals_handle = dispatch_signals(cmd_tx.clone());

    let rest_api_handle = http_server::run(config.rest_api, app_state.clone()).await?;

    let (db_writer, db_helper_handle) = SqliteHelper::from_config(SqliteHelperConfig::new(
        config.sqlite_path.clone().into(),
        Some("bm-archive.db".into()),
    ))?;

    let quarantine = Quarantine::new(config.sqlite_path.into())?;

    let blk_apply_handle = block_applier::run(
        bp_resolver,
        db_writer,
        quarantine,
        app_state.clone(),
        metrics.clone(),
        cmd_rx,
        thread_accounts_repository,
    );

    let db_helper_handle = tokio::task::spawn_blocking(move || db_helper_handle.join());

    if config.subscribe_sockets.len() > 1 || config.config_path.is_some() {
        // Multi-node mode: connection pool
        let (nodes_tx, nodes_rx) = watch::channel(config.subscribe_sockets.clone());

        let pool_handle =
            tokio::spawn(connection_pool::run(config.subscribe_sockets, cmd_tx, metrics, nodes_rx));

        // Spawn config reload handler if config_path is set, otherwise a no-op future
        let reload_handle: tokio::task::JoinHandle<anyhow::Result<()>> =
            if let Some(path) = config.config_path {
                tokio::spawn(config_reload_handler(nodes_tx, app_state.clone(), path))
            } else {
                tokio::spawn(std::future::pending())
            };

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
            result = blk_apply_handle => {
                match result {
                    Ok(reason) => {
                        anyhow::bail!("block_applier exited unexpectedly: {reason:?}");
                    }
                    Err(err) => {
                        anyhow::bail!("block_applier panicked: {err:?}");
                    }
                }
            }
            result = pool_handle => {
                match result {
                    Ok(reason) => {
                        anyhow::bail!("connection pool exited unexpectedly: {reason:?}");
                    }
                    Err(err) => {
                        anyhow::bail!("connection pool panicked: {err:?}");
                    }
                }
            }
            result = reload_handle => {
                match result {
                    Ok(reason) => {
                        anyhow::bail!("config reload handler exited unexpectedly: {reason:?}");
                    }
                    Err(err) => {
                        anyhow::bail!("config reload handler panicked: {err:?}");
                    }
                }
            }
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
    } else {
        // Legacy single-node mode (unchanged)
        let blk_subscribe_handle =
            block_subscriber::run(cmd_tx, config.subscribe_sockets[0], metrics);

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
            result = blk_apply_handle => {
                match result {
                    Ok(reason) => {
                        anyhow::bail!("block_applier exited unexpectedly: {reason:?}");
                    }
                    Err(err) => {
                        anyhow::bail!("block_applier panicked: {err:?}");
                    }
                }
            }
            result = blk_subscribe_handle => {
                match result {
                    Ok(()) => {
                        anyhow::bail!("block subscriber exited unexpectedly!");
                    }
                    Err(err) => {
                        anyhow::bail!("block subscriber panicked: {err:?}");
                    }
                }
            }
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
}

async fn config_reload_handler(
    nodes_tx: watch::Sender<Vec<std::net::SocketAddr>>,
    app_state: Arc<AppState>,
    config_path: PathBuf,
) -> anyhow::Result<()> {
    let mut sig_usr1 = tokio::signal::unix::signal(SignalKind::user_defined1())?;
    loop {
        sig_usr1.recv().await;
        tracing::info!("SIGUSR1: reloading BK nodes from {}", config_path.display());
        match crate::cli::parse_config_file(&config_path) {
            Ok(parsed) => {
                tracing::info!(
                    "Config reloaded: {} stream nodes, {} API endpoints",
                    parsed.stream_sockets.len(),
                    parsed.api_endpoints.len()
                );
                nodes_tx.send(parsed.stream_sockets).expect("failed to send config update");
                if !parsed.api_endpoints.is_empty() {
                    app_state.bk_api_pool.update_endpoints(parsed.api_endpoints);
                }
            }
            Err(e) => {
                tracing::error!("Failed to reload config: {e}");
            }
        }
    }
}
