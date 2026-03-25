// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
mod app;
mod domain;
use std::fs;
use std::fs::File;

use anyhow::Context;
use clap::Parser;

use crate::app::metrics::Metrics;
use crate::app::pipeline::App;
use crate::cli::Args;
use crate::config::AppConfig;
use crate::infra::factory::create_db_client;
use crate::infra::factory::create_fs_client;
use crate::infra::factory::create_s3_client;
use crate::infra::logging::init_tracing;

mod cli;
mod config;
mod infra;
mod utils;

const TABLES: &[&str] =
    &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let cfg = AppConfig::from_args(Args::parse());
    for path in [&cfg.daily_dir, &cfg.processed_dir] {
        fs::create_dir_all(path).context(format!("Failed to create {path:?} directory"))?;
    }

    let lock_path = cfg.daily_dir.join(".bm-archive-processor.lock");
    let lock_file =
        File::create(&lock_path).context(format!("Failed to create lock file {lock_path:?}"))?;
    if !try_lock_exclusive(&lock_file) {
        anyhow::bail!("Another instance is already running (lock: {})", lock_path.display());
    }

    tracing::info!("Starting BM archives processor with config: {cfg}");
    let metrics = Metrics::new(&opentelemetry::global::meter("bmap_meter"));

    // Initialize dependencies
    let db_client = create_db_client(TABLES, Some(metrics.clone()));
    let fs_client = create_fs_client(cfg.incoming_dir.clone());
    let s3_client = create_s3_client(cfg.skip_upload).await?;

    let app = App::new(cfg, Some(metrics));
    if let Err(err) = app.run(db_client, s3_client, fs_client).await {
        tracing::error!(error = %err, "Application failed");
        anyhow::bail!(err);
    }

    // lock_file is dropped here, releasing the flock automatically
    drop(lock_file);
    Ok(())
}

/// Try to acquire an exclusive flock on the file. Returns `true` on success.
fn try_lock_exclusive(file: &File) -> bool {
    use std::os::unix::io::AsRawFd;
    // LOCK_EX | LOCK_NB: exclusive, non-blocking
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}
