// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod downloader;

use tvm_block::Deserializable;
use tvm_types::read_single_root_boc;

#[allow(dead_code)]
fn apply_block_on_state() -> anyhow::Result<tvm_types::Cell> {
    let block = tvm_block::Block::construct_from_bytes(&[])
        .map_err(|e| anyhow::anyhow!("Failed to construct block: {e}"))?;

    let shared_state = read_single_root_boc([])
        .map_err(|e| anyhow::anyhow!("Failed to read shared state: {e}"))?;

    let state_update = block
        .read_state_update()
        .map_err(|e| anyhow::anyhow!("Failed to read state update: {e}"))?;

    let new_state = state_update
        .apply_for(&shared_state)
        .map_err(|e| anyhow::anyhow!("Failed to apply state update: {e}"))?;
    Ok(new_state)
}
