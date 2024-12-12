// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;
use std::sync::LazyLock;

use clap::Parser;
use url::Url;

pub static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
{}
BUILD_GIT_BRANCH={}
BUILD_GIT_COMMIT={}
BUILD_GIT_DATE={}
BUILD_TIME={}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_GIT_BRANCH"),
        env!("BUILD_GIT_COMMIT"),
        env!("BUILD_GIT_DATE"),
        env!("BUILD_TIME"),
    )
});

/// Acki-Nacki Block Manager
#[derive(Parser, Debug)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
pub struct Args {
    /// A string representing the URL of the HTTP/3 server to connect to. The
    /// scheme must be HTTPS, and the port number needs to be explicitly
    /// specified.
    #[arg(long, env)]
    pub stream_src_url: Url,

    /// URL for node http endpoint
    #[arg(long, env)]
    pub http_src_url: Url,

    /// File path for sqlite
    #[arg(long, env)]
    pub sqlite_path: PathBuf,
}
