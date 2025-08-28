use std::sync::Arc;

use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;

pub fn start_optimistic_state_save_service(
    repository: RepositoryImpl,
    receiver: InstrumentedReceiver<Arc<OptimisticStateImpl>>,
) -> anyhow::Result<()> {
    loop {
        match receiver.recv() {
            Ok(state) => {
                let block_id = state.get_block_id().clone();
                tracing::trace!("Optimistic State saving service received state: {:?}", block_id);
                repository.store_optimistic(state)?;
                tracing::trace!("Optimistic State saving successfully saved state: {:?}", block_id);
            }
            Err(e) => anyhow::bail!(e),
        }
    }
}
