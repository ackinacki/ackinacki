// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use account_state::ThreadAccountsRepository;
use block_manager::application::executor;
use block_manager::application::history_cli;
use block_manager::application::metrics::Metrics;
use block_manager::cli::config_from_args;
use block_manager::cli::event_filters_from_args;
use block_manager::cli::Args;
use block_manager::cli::Command;
use block_manager::cli::LONG_VERSION;
use block_manager::domain::bp_resolver::BPResolverImpl;
use block_manager::domain::models::AppConfig;
use block_manager::infrastructure::tracing::init_tracing;
use clap::Parser;
use parking_lot::Mutex;

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

    let mut args = Args::try_parse().unwrap_or_else(|err| {
        // A wrong argument of the service names the version, as its start
        // does; help, version and history-proofs print only their own lines.
        // args_os: an argument that is not UTF-8 must not panic here.
        let history_proofs = std::env::args_os().any(|arg| arg == "history-proofs");
        if err.use_stderr() && !history_proofs {
            eprintln!("Starting Acki-Nacki Block Manager, version: {}", *LONG_VERSION);
        }
        err.exit()
    });
    if let Some(Command::HistoryProofs(cmd)) = args.command.take() {
        // Maintenance of the history proofs store: no network, no block-manager start.
        let data_dir = std::path::PathBuf::from(&args.sqlite_path);
        let code = match event_filters_from_args(&args) {
            Ok(filter) => history_cli::run(cmd, &data_dir, &filter),
            Err(err) => {
                eprintln!("error: {err:#}");
                history_cli::EXIT_USAGE
            }
        };
        std::process::exit(code);
    }
    // The service only: the stderr of history-proofs holds its own lines.
    eprintln!("Starting Acki-Nacki Block Manager, version: {}", *LONG_VERSION);
    let config = config_from_args(args)?;

    if let Err(err) = tokio_main(config) {
        eprintln!("tokio main thread panicked: {:#?}", err);
        std::process::exit(1);
    };
    Ok(())
}

fn tokio_main(config: AppConfig) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    let res = rt.block_on(async {
        init_tracing();
        let metrics = Metrics::new("bm");

        if let Some(m) = metrics.as_ref() {
            tracing::info!("Using OTLP metrics endpoint: {}", m.endpoint);
            m.bm.report_build_info();
        } else {
            tracing::info!("No OTEL exporter endpoint found, metrics not collected.");
        };

        let bp_resolver = Arc::new(Mutex::new(BPResolverImpl::new(config.default_bp.clone())));
        let thread_accounts_repository =
            ThreadAccountsRepository::builder("./data/thread_state").build()?;
        executor::run(config, bp_resolver, metrics, thread_accounts_repository).await
    });
    tracing::debug!("Shutting down tokio runtime");
    rt.shutdown_timeout(Duration::from_millis(3000));

    res
}
