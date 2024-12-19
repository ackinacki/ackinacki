// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use clap::Parser;
use helpers::init_tracing;

mod client;
mod defaults;
mod helpers;
mod schema;
mod web;

/// Acki-Nacki GraphQL server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The path to the DB file (node-archive.db)
    #[arg(short = 'd', long = "db", env, num_args = 0..=1)]
    db: Option<String>,

    /// The host address and TCP port on which the service will accept
    /// connections (default: 127.0.0.1:3000)
    #[arg(short = 'l', long = "listen", env, num_args = 0..=1)]
    listen: Option<String>,

    /// The node's endpoint for resending incoming external messages (default: http://127.0.0.1/bk/v1/messages)
    #[arg(short = 'n', long = "node_url", env)]
    node_url: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // env_logger::builder().format_timestamp(None).init();
    init_tracing();

    let args = Args::parse();

    let db = PathBuf::from(args.db.unwrap_or(defaults::PATH_TO_DB.to_string()));

    let listen = args.listen.unwrap_or(defaults::LISTEN.to_string());

    web::start(listen, db).await
}
