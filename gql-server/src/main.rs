// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#![allow(unused)]

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;

use clap::Parser;
use gql_server::init_sdk;
use helpers::init_tracing;

use crate::schema::db::DBConnector;

mod config;
mod defaults;
mod helpers;
mod metrics;
mod schema;
mod signals;
mod web;

/// Acki-Nacki GraphQL server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The path to the DB file (bm-archive.db)
    #[arg(short = 'd', long = "db", env, num_args = 0..=1)]
    db: Option<String>,

    /// The host address and TCP port on which the service will accept
    /// connections (default: 127.0.0.1:3000)
    #[arg(short = 'l', long = "listen", env, num_args = 0..=1)]
    listen: Option<String>,

    #[arg(long = "bm_api_socket", env, num_args = 0..=1)]
    bm_api_socket: Option<String>,

    /// Path to YAML config file for runtime-tunable parameters.
    /// Reload with `kill -SIGUSR1 <pid>`.
    #[arg(long = "config", env = "GQL_CONFIG_FILE")]
    config: Option<PathBuf>,

    /// Enable deprecated API fields (account, accounts, blocks, messages,
    /// transactions at the query root and blockchain.accounts).
    #[arg(long = "deprecated-api", env = "GQL_DEPRECATED_API", default_value_t = false)]
    deprecated_api: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install a default rustls crypto provider before any TLS code runs.
    // Both `ring` and `aws-lc-rs` features are compiled in (via different deps),
    // and rustls 0.23 refuses to pick one automatically in that case.
    let _ = rustls::crypto::ring::default_provider().install_default();

    init_tracing();

    let args = Args::parse();

    // Load initial config (if provided).
    let initial_config = match &args.config {
        Some(path) => {
            let cfg = config::GqlServerConfig::from_file(path)?;
            tracing::info!("Loaded config from {}", path.display());
            Some(cfg)
        }
        None => None,
    };

    let max_pool_connections = initial_config
        .as_ref()
        .map(|c| c.max_pool_connections)
        .unwrap_or(defaults::MAX_POOL_CONNECTIONS);

    let acquire_timeout_secs = initial_config
        .as_ref()
        .map(|c| c.acquire_timeout_secs)
        .unwrap_or(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS);

    let db = PathBuf::from(args.db.unwrap_or(defaults::PATH_TO_DB.to_string()));
    let db_fs_path = normalize_sqlite_path(&db);

    let listen = args.listen.unwrap_or(defaults::LISTEN.to_string());
    let bm_api_socket = args.bm_api_socket.expect("--bm_api_socket must be set");
    let sdk_client = init_sdk(Some(bm_api_socket)).expect("SDK client created");

    let sqlite_mmap_size = initial_config
        .as_ref()
        .map(|c| c.sqlite_mmap_size)
        .unwrap_or(defaults::DEFAULT_SQLITE_MMAP_SIZE);
    let sqlite_cache_size = initial_config
        .as_ref()
        .map(|c| c.sqlite_cache_size)
        .unwrap_or(defaults::DEFAULT_SQLITE_CACHE_SIZE);

    let acquire_timeout = std::time::Duration::from_secs(acquire_timeout_secs);
    let pragmas = schema::db::build_read_pragmas(sqlite_mmap_size, sqlite_cache_size);
    let pool = web::open_db(db, max_pool_connections, acquire_timeout, pragmas).await?;
    let db_connector = DBConnector::new(pool, db_fs_path.clone(), max_pool_connections);

    // Apply initial config values to the connector.
    if let Some(ref cfg) = initial_config {
        apply_config(&db_connector, cfg);
    }

    let query_duration_boundaries =
        initial_config.as_ref().and_then(|c| c.query_duration_boundaries.clone());
    let sqlite_query_boundaries =
        initial_config.as_ref().and_then(|c| c.sqlite_query_boundaries.clone());

    let metrics = metrics::Metrics::new(
        Arc::clone(&db_connector),
        query_duration_boundaries,
        sqlite_query_boundaries,
    );
    if let Some(m) = metrics.as_ref() {
        tracing::info!("Using OTLP metrics endpoint: {}", m.endpoint);
        m.gql.report_build_info();
        db_connector.set_metrics(m.gql.clone());
    } else {
        tracing::info!("No OTEL exporter endpoint found, metrics not collected.");
    }

    let deprecated_api_initial = args.deprecated_api
        || initial_config.as_ref().and_then(|c| c.deprecated_api).unwrap_or(false);
    let deprecated_api = Arc::new(AtomicBool::new(deprecated_api_initial));
    if deprecated_api_initial {
        tracing::info!("Deprecated API is enabled");
    } else {
        tracing::info!("Deprecated API is disabled (use --deprecated-api to enable)");
    }

    resolve_archives(db_connector.as_ref(), &db_fs_path).await?;

    let (cmd_tx, cmd_rx) = mpsc::channel::<signals::WorkerCommand>();
    let signals_handle = signals::dispatch_signals(cmd_tx);
    let config_path = args.config;
    let worker_handle = spawn_command_worker(
        Arc::clone(&db_connector),
        db_fs_path,
        config_path,
        cmd_rx,
        Arc::clone(&deprecated_api),
        args.deprecated_api,
    );

    tokio::select! {
        result = web::start(listen, db_connector, sdk_client, metrics.map(|m| m.gql), deprecated_api) => result,
        result = signals_handle => match result {
            Ok(Ok(())) => anyhow::bail!("signal handler exited unexpectedly"),
            Ok(Err(err)) => anyhow::bail!("signal handler error: {err:?}"),
            Err(err) => anyhow::bail!("signal handler panicked: {err:?}"),
        },
        result = worker_handle => match result {
            Ok(Ok(())) => anyhow::bail!("command worker exited unexpectedly"),
            Ok(Err(err)) => anyhow::bail!("command worker error: {err:?}"),
            Err(err) => anyhow::bail!("command worker panicked: {err:?}"),
        },
    }
}

fn apply_config(db_connector: &DBConnector, cfg: &config::GqlServerConfig) {
    db_connector.set_query_timeout(std::time::Duration::from_secs(cfg.sqlite_query_timeout_secs));
    tracing::info!("sqlite_query_timeout={}s", cfg.sqlite_query_timeout_secs);

    db_connector.set_max_attached_db(cfg.max_attached_db);
    tracing::info!("max_attached_db={}", cfg.max_attached_db);

    if cfg.query_duration_boundaries.is_some() || cfg.sqlite_query_boundaries.is_some() {
        tracing::warn!("Histogram boundary changes require a restart to take effect");
    }
}

fn spawn_command_worker(
    db_connector: Arc<schema::db::DBConnector>,
    db_fs_path: PathBuf,
    config_path: Option<PathBuf>,
    cmd_rx: mpsc::Receiver<signals::WorkerCommand>,
    deprecated_api: Arc<AtomicBool>,
    cli_deprecated_api: bool,
) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        for cmd in cmd_rx {
            match cmd {
                signals::WorkerCommand::ResolveArchives => {
                    if let Err(err) =
                        handle.block_on(resolve_archives(db_connector.as_ref(), &db_fs_path))
                    {
                        tracing::error!("Failed to resolve archives: {err:?}");
                    }
                }
                signals::WorkerCommand::ReloadConfig => {
                    if let Some(ref path) = config_path {
                        match config::GqlServerConfig::from_file(path) {
                            Ok(cfg) => {
                                tracing::info!("Config reloaded from {}", path.display());
                                apply_config(&db_connector, &cfg);
                                let enabled =
                                    cli_deprecated_api || cfg.deprecated_api.unwrap_or(false);
                                let prev = deprecated_api.swap(enabled, Ordering::Relaxed);
                                if prev != enabled {
                                    tracing::info!("deprecated_api changed: {prev} -> {enabled}");
                                }
                                if let Err(err) = handle.block_on(
                                    db_connector.set_max_connections(cfg.max_pool_connections),
                                ) {
                                    tracing::error!("Failed to update max_connections: {err:?}");
                                }
                                if let Err(err) = handle.block_on(db_connector.set_acquire_timeout(
                                    std::time::Duration::from_secs(cfg.acquire_timeout_secs),
                                )) {
                                    tracing::error!("Failed to update acquire_timeout: {err:?}");
                                }
                                if let Err(err) = handle.block_on(db_connector.set_sqlite_pragmas(
                                    cfg.sqlite_mmap_size,
                                    cfg.sqlite_cache_size,
                                )) {
                                    tracing::error!("Failed to update SQLite PRAGMAs: {err:?}");
                                }
                            }
                            Err(err) => {
                                tracing::error!(
                                    "Failed to reload config from {}: {err:?}",
                                    path.display()
                                );
                            }
                        }
                    } else {
                        tracing::warn!(
                            "SIGUSR1 received but no --config path was provided, ignoring"
                        );
                    }
                }
            }
        }
        Err(anyhow::anyhow!("command worker channel closed"))
    })
}

async fn resolve_archives(
    db_connector: &schema::db::DBConnector,
    db_fs_path: &Path,
) -> anyhow::Result<()> {
    let max_attached = db_connector.max_attached_db();
    let attachments = list_archive_dbs(db_fs_path, max_attached)?;
    db_connector.update_attachments(attachments).await?;
    Ok(())
}

fn list_archive_dbs(db_fs_path: &Path, max_attached_db: u16) -> anyhow::Result<Vec<String>> {
    let Some(dir) = db_fs_path.parent() else {
        return Ok(Vec::new());
    };
    let stem = db_fs_path.file_stem().and_then(|name| name.to_str()).unwrap_or("bm-archive");
    let ext = db_fs_path.extension().and_then(|name| name.to_str()).unwrap_or("db");
    let prefix = format!("{stem}-");
    let suffix = format!(".{ext}");
    let mut archives = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                tracing::warn!("Failed to read archive dir entry: {err:?}");
                continue;
            }
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name.starts_with(&prefix) && file_name.ends_with(&suffix) {
            let ts_start = prefix.len();
            let ts_end = file_name.len() - suffix.len();
            let ts = file_name[ts_start..ts_end].parse::<u64>().unwrap_or(0);
            archives.push((ts, path.to_string_lossy().into_owned()));
        }
    }
    archives.sort_unstable_by_key(|(ts, _)| std::cmp::Reverse(*ts));
    archives.truncate(max_attached_db as usize);
    let archives = archives.into_iter().map(|(_, path)| path).collect();
    tracing::debug!(list = ?archives, "discovered archives:");
    Ok(archives)
}

fn normalize_sqlite_path(path: &Path) -> PathBuf {
    let raw = path.to_string_lossy();
    let raw = raw.strip_prefix("sqlite://").or_else(|| raw.strip_prefix("sqlite:")).unwrap_or(&raw);
    let raw = raw.split('?').next().unwrap_or(raw);
    PathBuf::from(raw)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::path::Path;

    use testdir::testdir;

    use super::list_archive_dbs;

    fn create_file(path: &Path) {
        File::create(path).expect("create test file");
    }

    #[test]
    fn list_archive_dbs_returns_9_newest() {
        let root = testdir!();
        let db_path = root.join("bm-archive.db");
        create_file(&db_path);

        for ts in [3_u64, 11, 5, 1, 8, 2, 10, 4, 9, 7, 6, 12] {
            create_file(&root.join(format!("bm-archive-{ts}.db")));
        }

        let archives = list_archive_dbs(&db_path, 9).expect("list archives");
        let names: Vec<String> = archives
            .iter()
            .map(|p| {
                Path::new(p)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .expect("archive file name")
                    .to_string()
            })
            .collect();

        assert_eq!(
            names,
            vec![
                "bm-archive-12.db",
                "bm-archive-11.db",
                "bm-archive-10.db",
                "bm-archive-9.db",
                "bm-archive-8.db",
                "bm-archive-7.db",
                "bm-archive-6.db",
                "bm-archive-5.db",
                "bm-archive-4.db",
            ]
        );
    }

    #[test]
    fn list_archive_dbs_ignores_non_matching_files() {
        let root = testdir!();
        let db_path = root.join("bm-archive.db");
        create_file(&db_path);

        create_file(&root.join("bm-archive-20.db"));
        create_file(&root.join("bm-archive-foo.db"));
        create_file(&root.join("bm-archive-20.txt"));
        create_file(&root.join("other-30.db"));
        fs::create_dir(root.join("bm-archive-99.db")).expect("create test dir");

        let archives = list_archive_dbs(&db_path, 9).expect("list archives");
        let names: Vec<String> = archives
            .iter()
            .map(|p| {
                Path::new(p)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .expect("archive file name")
                    .to_string()
            })
            .collect();

        assert_eq!(names, vec!["bm-archive-20.db", "bm-archive-foo.db"]);
    }
}
