// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.

use std::path::PathBuf;
use std::sync::LazyLock;

use clap::Parser;
use clap::Subcommand;

pub static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "\n{}\nBUILD_GIT_BRANCH={}\nBUILD_GIT_COMMIT={}\nBUILD_GIT_DATE={}\nBUILD_TIME={}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_GIT_BRANCH"),
        env!("BUILD_GIT_COMMIT"),
        env!("BUILD_GIT_DATE"),
        env!("BUILD_TIME"),
    )
});

/// Acki Nacki Proxy Manager CLI
#[derive(Parser, Debug, Clone)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
pub struct CliArgs {
    /// Path to the proxy configuration file (should already exist)
    #[arg(short = 'c', long, default_value = "./proxy.yaml")]
    pub proxy_config: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Docker
    Docker {
        /// Docker socket path
        #[arg(long, default_value = "unix:///var/run/docker.sock")]
        socket: String,

        /// Proxy container name
        #[arg(long)]
        container: String,
    },

    PidPath {
        /// Path to the proxy PID file
        #[arg(long, default_value = "/var/run/proxy.pid")]
        pid_path: PathBuf,
    },

    Pid {
        /// proxy PID
        #[arg(long)]
        pid: u64,
    },
}
