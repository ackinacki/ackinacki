// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::repository::repository_impl::save_to_file;
use crate::types::AckiNackiBlock;

const BK_SET_BLOCK_STORAGE_TARGET: &str = "bk_set_block_storage";

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum BkSetBlockSaveCommand {
    Save(Envelope<AckiNackiBlock>),
    Shutdown,
}

pub fn start_bk_set_block_storage_service(
    base_path: PathBuf,
    receiver: InstrumentedReceiver<BkSetBlockSaveCommand>,
) -> anyhow::Result<()> {
    loop {
        match receiver.recv()? {
            BkSetBlockSaveCommand::Save(block) => {
                let block_id = block.data().identifier();
                let seq_no = block.data().seq_no();
                let thread_id = block.data().common_section().thread_id();
                let mut path = base_path.clone();
                path.push(format!("{thread_id:x}"));
                path.push(format!("{seq_no}_{block_id}"));
                tracing::trace!(
                    target: BK_SET_BLOCK_STORAGE_TARGET,
                    "Saving finalized block with BK set changes: thread_id={thread_id:?} seq_no={seq_no:?} block_id={block_id:?} path={path:?}",
                );
                save_to_file(&path, &block, true)?;
            }
            BkSetBlockSaveCommand::Shutdown => {
                tracing::trace!(
                    target: BK_SET_BLOCK_STORAGE_TARGET,
                    "BK set block storage service shutting down"
                );
                return Ok(());
            }
        }
    }
}
