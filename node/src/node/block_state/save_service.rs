use std::sync::Arc;

use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::node::block_state::block_state_inner::BlockStateInner;
use crate::utilities::guarded::Guarded;

pub fn start_state_save_service(
    state_receiver: InstrumentedReceiver<Arc<BlockStateInner>>,
) -> anyhow::Result<()> {
    loop {
        match state_receiver.recv() {
            Ok(state) => {
                tracing::trace!(
                    "State saving service received state: {:?} {:?}",
                    state.guarded(|e| *e.block_seq_no()),
                    state.block_identifier
                );
                let mut state = state.shared_access.write();
                if state.last_saved_object_state_version != state.object_state_version {
                    state.save()?;
                }
            }
            Err(e) => anyhow::bail!(e),
        }
    }
}
