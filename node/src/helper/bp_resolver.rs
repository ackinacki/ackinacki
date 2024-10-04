// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use message_router::bp_resolver::BPResolver;
use network::config::NetworkConfig;
use parking_lot::Mutex;

use crate::block::Block;
use crate::database::sqlite_helper::SqliteHelper;
use crate::database::sqlite_helper::SqliteHelperContext;

pub struct BPResolverImpl {
    network: NetworkConfig,
    context: Arc<Mutex<SqliteHelperContext>>,
}

impl BPResolver for BPResolverImpl {
    fn resolve(&mut self, node_id: Option<i32>) -> Option<SocketAddr> {
        let fut_nodes = async { self.network.alive_nodes(false).await };
        let _alive_nodes =
            futures::executor::block_on(fut_nodes).expect("Failed to update nodes addresses");
        let bp_node_id = match node_id {
            Some(id) => id,
            None => {
                let mut context = self.context.lock();
                let latest_block = SqliteHelper::get_latest_block(&mut context)
                    .expect("Failed to load latest block");
                match latest_block {
                    Some(block) => block.get_common_section().producer_id,
                    None => unreachable!(),
                }
            }
        };
        tracing::trace!("BP resolver: producer id={bp_node_id}");
        self.network.nodes.get(&bp_node_id).map(|addr| addr.to_owned())
    }
}

impl BPResolverImpl {
    pub fn new(network_config: NetworkConfig, sqlite_helper: SqliteHelper) -> Self {
        let db_path = PathBuf::from(sqlite_helper.config.data_dir.clone()).join("node-archive.db");
        let sqlite_conn: rusqlite::Connection =
            SqliteHelper::create_connection_ro(db_path).expect("Failed to open {db_path}");
        Self {
            network: network_config,
            context: Arc::new(Mutex::new(SqliteHelperContext {
                config: sqlite_helper.config,
                conn: sqlite_conn,
            })),
        }
    }
}
