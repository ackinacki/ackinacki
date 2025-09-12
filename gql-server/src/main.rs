// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use clap::Parser;
use gql_server::init_sdk;
use helpers::init_tracing;

mod defaults;
mod helpers;
mod schema;
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

    let listen = args.listen.unwrap_or(defaults::LISTEN.to_string());
    let bm_api_socket = args.bm_api_socket.expect("--bm_api_socket must be set");
    let sdk_client = init_sdk(Some(bm_api_socket)).expect("SDK client created");

    web::start(listen, db, sdk_client).await
}
