use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::node::block_state::block_state_inner::StateSaveCommand;
use crate::utilities::guarded::Guarded;

const BLOCK_STATE_SAVE_TARGET: &str = "block_state_save";

pub fn start_state_save_service(
    state_receiver: InstrumentedReceiver<StateSaveCommand>,
) -> anyhow::Result<()> {
    loop {
        match state_receiver.recv()? {
            StateSaveCommand::Save(state) => {
                let received_at = std::time::Instant::now();
                let block_identifier = state.block_identifier;
                let block_seq_no = state.guarded(|e| *e.block_seq_no());
                tracing::trace!(
                    target: BLOCK_STATE_SAVE_TARGET,
                    "State saving service received state: {:?} {:?}",
                    block_seq_no,
                    block_identifier
                );
                let lock_started_at = std::time::Instant::now();
                let mut state = state.shared_access.write();
                let lock_wait_ms = lock_started_at.elapsed().as_millis();
                let version_changed =
                    state.last_saved_object_state_version != state.object_state_version;
                let last_saved_version = state.last_saved_object_state_version;
                let object_state_version = state.object_state_version;
                if state.last_saved_object_state_version != state.object_state_version {
                    let save_started_at = std::time::Instant::now();
                    state.save()?;
                    tracing::trace!(
                        target: BLOCK_STATE_SAVE_TARGET,
                        "Block state save completed: seq_no={block_seq_no:?} block_id={block_identifier:?} lock_wait_ms={lock_wait_ms} save_ms={} total_ms={} version_changed={version_changed} last_saved_version={last_saved_version} object_state_version={object_state_version}",
                        save_started_at.elapsed().as_millis(),
                        received_at.elapsed().as_millis(),
                    );
                } else {
                    tracing::trace!(
                        target: BLOCK_STATE_SAVE_TARGET,
                        "Block state save skipped: seq_no={block_seq_no:?} block_id={block_identifier:?} lock_wait_ms={lock_wait_ms} total_ms={} version_changed={version_changed} last_saved_version={last_saved_version} object_state_version={object_state_version}",
                        received_at.elapsed().as_millis(),
                    );
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
