use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::node::block_state::block_state_inner::StateSaveCommand;

const BLOCK_STATE_SAVE_TARGET: &str = "block_state_save";

pub fn start_state_save_service(
    state_receiver: InstrumentedReceiver<StateSaveCommand>,
) -> anyhow::Result<()> {
    loop {
        match state_receiver.recv()? {
            StateSaveCommand::Save(state) => {
                let mut state = state.shared_access.write();
                if state.last_saved_object_state_version != state.object_state_version {
                    state.save()?;
                }
            }
            StateSaveCommand::Shutdown => {
                tracing::trace!(
                    target: BLOCK_STATE_SAVE_TARGET,
                    "State saving service shutting down!!"
                );
                return Ok(());
            }
        }
    }
}
