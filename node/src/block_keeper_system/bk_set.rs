use std::ops::Deref;
use std::sync::Arc;

use crate::block_keeper_system::BlockKeeperSet;
use crate::block_keeper_system::BlockKeeperSetChange;
use crate::types::AckiNackiBlock;

pub(crate) fn update_block_keeper_set_from_common_section(
    block: &AckiNackiBlock,
    current_bk_set: Arc<BlockKeeperSet>,
) -> anyhow::Result<Option<Arc<BlockKeeperSet>>> {
    let common_section = block.get_common_section();
    if common_section.block_keeper_set_changes.is_empty() {
        return Ok(None);
    }
    let mut new_bk_set = current_bk_set.deref().clone();
    tracing::trace!(
        "update_block_keeper_set_from_common_section block_id:{} {:?}",
        block.identifier(),
        common_section.block_keeper_set_changes
    );

    for block_keeper_change in &common_section.block_keeper_set_changes {
        if let BlockKeeperSetChange::BlockKeeperRemoved((signer_index, block_keeper_data)) =
            block_keeper_change
        {
            tracing::trace!("Remove block keeper key: {signer_index} {block_keeper_data:?}");
            tracing::trace!("Remove block keeper key: {:?}", new_bk_set);
            let block_keeper_data = new_bk_set.remove_signer(signer_index);
            tracing::trace!("Removed block keeper key: {:?}", block_keeper_data);
        }
    }
    for block_keeper_change in &common_section.block_keeper_set_changes {
        if let BlockKeeperSetChange::BlockKeeperAdded((signer_index, block_keeper_data)) =
            block_keeper_change
        {
            tracing::trace!("insert block keeper key: {signer_index} {block_keeper_data}");
            tracing::trace!("insert block keeper key: {:?}", new_bk_set);
            new_bk_set.insert(*signer_index, block_keeper_data.clone());
        }
    }
    tracing::trace!(
        "update_block_keeper_set_from_common_section block_id:{} final_bk_set: {:?}",
        block.identifier(),
        new_bk_set
    );
    Ok(Some(Arc::new(new_bk_set)))
}
