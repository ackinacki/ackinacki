// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedSender;

// use std::thread::sleep;
use super::feedback::AckiNackiSend;
use super::inner_loop;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::load_blockchain_config;
use crate::config::Config;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message_storage::MessageDurableStorage;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::node::BlockState;
use crate::protocol::authority_switch::action_lock::Authority;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::AckiNackiBlock;
use crate::utilities::thread_spawn_critical::SpawnCritical;

#[derive(Clone)]
pub struct ValidationServiceInterface {
    send_tx: InstrumentedSender<(BlockState, Envelope<GoshBLS, AckiNackiBlock>)>,
}

impl ValidationServiceInterface {
    pub fn send(&self, state: (BlockState, Envelope<GoshBLS, AckiNackiBlock>)) {
        self.send_tx.send(state).expect("Validation service must not ever die");
    }
}

pub struct ValidationService {
    interface: ValidationServiceInterface,
    _handler: std::thread::JoinHandle<()>,
}

impl ValidationService {
    pub fn interface(&self) -> ValidationServiceInterface {
        self.interface.clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new<P: AsRef<Path>>(
        blockchain_config_path: P,
        repository: RepositoryImpl,
        node_config: Config,
        shared_services: SharedServices,
        block_state_repo: BlockStateRepository,
        send: AckiNackiSend,
        metrics: Option<BlockProductionMetrics>,
        message_db: MessageDurableStorage,
        authority: Arc<Mutex<Authority>>,
    ) -> anyhow::Result<Self> {
        let (tx, rx) =
            instrumented_channel(metrics.clone(), crate::helper::metrics::BLOCK_STATE_CHANNEL);
        let interface = ValidationServiceInterface { send_tx: tx };
        let blockchain_config = load_blockchain_config(&blockchain_config_path.as_ref().into())?;

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
                    send,
                    metrics,
                    message_db,
                    authority,
                );
                Ok(())
            })?;

        Ok(ValidationService { _handler: handler, interface })
    }
}
