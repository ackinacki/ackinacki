// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::sync::mpsc::Sender;
use std::sync::Arc;

// use std::thread::sleep;
use parking_lot::Mutex;
use serde_json::Value;
use tvm_executor::BlockchainConfig;
use tvm_types::UInt256;

use super::feedback::AckiNackiSend;
use super::inner_loop;
use crate::config::Config;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::node::BlockState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::utilities::thread_spawn_critical::SpawnCritical;
use crate::utilities::FixedSizeHashSet;

#[derive(Clone)]
pub struct ValidationServiceInterface {
    send_tx: Sender<BlockState>,
}

impl ValidationServiceInterface {
    pub fn send(&self, state: BlockState) {
        self.send_tx.send(state).expect("Validation service must not ever die");
    }
}

pub struct ValidationService {
    interface: ValidationServiceInterface,
    // TODO: check handler that thread was not stopped
    _handler: std::thread::JoinHandle<()>,
}

impl ValidationService {
    pub fn interface(&self) -> ValidationServiceInterface {
        self.interface.clone()
    }

    pub fn new<P: AsRef<Path>>(
        blockchain_config_path: P,
        repository: RepositoryImpl,
        node_config: Config,
        shared_services: SharedServices,
        block_state_repo: BlockStateRepository,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        send: AckiNackiSend,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let interface = ValidationServiceInterface { send_tx: tx };
        let blockchain_config_path = blockchain_config_path.as_ref();
        let json = std::fs::read_to_string(blockchain_config_path)?;
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(&json)?;
        let config_params = tvm_block_json::parse_config(&map).map_err(|e| {
            anyhow::format_err!(
                "Failed to load config params from file {blockchain_config_path:?}: {e}"
            )
        })?;

        let blockchain_config = BlockchainConfig::with_config(config_params)
            .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))?;

        let handler: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name("Block validation service".to_string())
            .spawn_critical(move || {
                inner_loop::inner_loop(
                    rx,
                    block_state_repo,
                    repository,
                    blockchain_config.into(),
                    node_config,
                    shared_services,
                    nack_set_cache,
                    send,
                );
                Ok(())
            })?;

        Ok(ValidationService { _handler: handler, interface })
    }
}
