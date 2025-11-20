use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateSaveCommand;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;

pub fn start_optimistic_state_save_service(
    repository: RepositoryImpl,
    receiver: InstrumentedReceiver<OptimisticStateSaveCommand>,
) -> anyhow::Result<()> {
    loop {
        match receiver.recv()? {
            OptimisticStateSaveCommand::Save(state) => {
                let block_id = state.get_block_id().clone();
                tracing::trace!("Optimistic State saving service received state: {block_id:?}");
                repository.store_optimistic(state)?;
                tracing::trace!("Optimistic State saving successfully saved state: {block_id:?}");
            }
            OptimisticStateSaveCommand::Shutdown => {
                tracing::trace!("Optimistic State saving service shutting down!!");
                return Ok(());
            }
        }
    }
}
