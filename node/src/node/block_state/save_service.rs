use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::node::block_state::block_state_inner::StateSaveCommand;
use crate::utilities::guarded::Guarded;

pub fn start_state_save_service(
    state_receiver: InstrumentedReceiver<StateSaveCommand>,
) -> anyhow::Result<()> {
    loop {
        match state_receiver.recv()? {
            StateSaveCommand::Save(state) => {
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
            StateSaveCommand::Shutdown => {
                tracing::trace!("State saving service shutting down!!");
                return Ok(());
            }
        }
    }
}
