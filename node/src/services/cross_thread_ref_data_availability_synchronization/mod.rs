use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedSender;

use crate::helper::metrics::BlockProductionMetrics;
use crate::node::block_state::repository::BlockState;
use crate::utilities::thread_spawn_critical::SpawnCritical;

mod inner_loop;

enum Command {
    NotifyCrossThreadRefDataPrepared(BlockState),
    SetCrossThreadRefDataDependencies(BlockState, Vec<BlockState>),
}

#[derive(Clone)]
pub struct CrossThreadRefDataAvailabilitySynchronizationServiceInterface {
    send_tx: InstrumentedSender<Command>,
}

pub struct CrossThreadRefDataAvailabilitySynchronizationService {
    interface: CrossThreadRefDataAvailabilitySynchronizationServiceInterface,
    _handler: std::thread::JoinHandle<()>,
}

impl CrossThreadRefDataAvailabilitySynchronizationServiceInterface {
    pub fn send_cross_thread_ref_data_prepared(&mut self, block_state: BlockState) {
        let _ = self.send_tx.send(Command::NotifyCrossThreadRefDataPrepared(block_state));
    }

    pub fn send_await_cross_thread_ref_data(
        &mut self,
        block_state: BlockState,
        dependencies: Vec<BlockState>,
    ) {
        let _ = self
            .send_tx
            .send(Command::SetCrossThreadRefDataDependencies(block_state, dependencies));
    }
}

impl CrossThreadRefDataAvailabilitySynchronizationService {
    pub fn new(metrics: Option<BlockProductionMetrics>) -> anyhow::Result<Self> {
        let (tx, rx) = instrumented_channel(
            metrics.clone(),
            crate::helper::metrics::CROSS_THREAD_REF_DATA_AVAILABILITY_SYNCHRONIZATION_SERVICE_CHANNEL
        );
        let handler: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name("Block validation service".to_string())
            .spawn_critical(move || {
                inner_loop::inner_loop(rx);
                Ok(())
            })?;
        Ok(Self {
            interface: CrossThreadRefDataAvailabilitySynchronizationServiceInterface {
                send_tx: tx,
            },
            _handler: handler,
        })
    }

    pub fn interface(&self) -> CrossThreadRefDataAvailabilitySynchronizationServiceInterface {
        self.interface.clone()
    }
}
