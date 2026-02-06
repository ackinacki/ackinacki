// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;

use clap::Parser;
use gql_server::init_sdk;
use helpers::init_tracing;

use crate::schema::db::DBConnector;

mod defaults;
mod helpers;
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let args = Args::parse();

    let db = PathBuf::from(args.db.unwrap_or(defaults::PATH_TO_DB.to_string()));
    let db_fs_path = normalize_sqlite_path(&db);

    let listen = args.listen.unwrap_or(defaults::LISTEN.to_string());
    let bm_api_socket = args.bm_api_socket.expect("--bm_api_socket must be set");
    let sdk_client = init_sdk(Some(bm_api_socket)).expect("SDK client created");

    let pool = web::open_db(db).await?;
    let db_connector = DBConnector::new(pool, db_fs_path.clone());

    resolve_archives(db_connector.as_ref(), &db_fs_path).await?;

    let (cmd_tx, cmd_rx) = mpsc::channel::<signals::WorkerCommand>();
    let signals_handle = signals::dispatch_signals(cmd_tx);
    let archive_resolver_handle =
        spawn_archive_resolver(Arc::clone(&db_connector), db_fs_path, cmd_rx);

    tokio::select! {
        result = web::start(listen, db_connector, sdk_client) => result,
        result = signals_handle => match result {
            Ok(Ok(())) => anyhow::bail!("signal handler exited unexpectedly"),
            Ok(Err(err)) => anyhow::bail!("signal handler error: {err:?}"),
            Err(err) => anyhow::bail!("signal handler panicked: {err:?}"),
        },
        result = archive_resolver_handle => match result {
            Ok(Ok(())) => anyhow::bail!("archive resolver exited unexpectedly"),
            Ok(Err(err)) => anyhow::bail!("archive resolver error: {err:?}"),
            Err(err) => anyhow::bail!("archive resolver panicked: {err:?}"),
        },
    }
}

fn spawn_archive_resolver(
    db_connector: Arc<schema::db::DBConnector>,
    db_fs_path: PathBuf,
    cmd_rx: mpsc::Receiver<signals::WorkerCommand>,
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
            }
        }
        Err(anyhow::anyhow!("archive resolver channel closed"))
    })
}

async fn resolve_archives(
    db_connector: &schema::db::DBConnector,
    db_fs_path: &Path,
) -> anyhow::Result<()> {
    let attachments = list_archive_dbs(db_fs_path)?;
    db_connector.update_attachments(attachments).await?;
    Ok(())
}

fn list_archive_dbs(db_fs_path: &Path) -> anyhow::Result<Vec<String>> {
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
