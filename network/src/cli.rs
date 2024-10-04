// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[arg(short, long, env)]
    pub bind: SocketAddr,
    #[arg(short, long, env, value_delimiter = ',')]
    pub nodes: Vec<SocketAddr>,
}
