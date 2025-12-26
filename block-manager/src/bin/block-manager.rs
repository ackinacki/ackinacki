// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io::Write;
use std::process::exit;
use std::sync::Arc;
use std::thread;

use block_manager::application::executor;
use block_manager::application::metrics::Metrics;
use block_manager::cli::config_from_args;
use block_manager::cli::Args;
use block_manager::domain::bp_resolver::BPResolverImpl;
use block_manager::domain::models::AppConfig;
use block_manager::infrastructure::tracing::init_tracing;
use clap::Parser;
use parking_lot::lock_api::Mutex;

fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok(); // ignore errors

    std::panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        let crash_log = format!("panicked: {panic_info}\nBacktrace:\n{backtrace}");
        eprintln!("{crash_log}");

        if let Ok(mut file) =
            std::fs::OpenOptions::new().create(true).append(true).open("crash.log")
        {
            let _ = writeln!(file, "{crash_log}");
        }
        std::process::exit(100);
    }));

    let config = config_from_args(Args::parse())?;

    if let Err(err) = thread::Builder::new()
        .name("tokio_main".into())
        .spawn(move || tokio_main(config))
        .expect("Tokio main thread must be started")
        .join()
    {
        eprintln!("tokio main thread panicked: {:#?}", err);
        exit(1);
    }
    Ok(())
}

#[tokio::main]
async fn tokio_main(config: AppConfig) -> anyhow::Result<()> {
    init_tracing();
    let metrics = Metrics::new("bm");
    if let Some(m) = metrics.as_ref() {
        tracing::info!("Using OTLP metrics endpoint: {}", m.endpoint);
        m.bm.report_build_info();
    } else {
        tracing::info!("No OTEL exporter endpoint found, metrics not collected.");
    };

    let bp_resolver = Arc::new(Mutex::new(BPResolverImpl::new(config.default_bp.clone())));
    executor::run(config, bp_resolver, metrics).await
}
