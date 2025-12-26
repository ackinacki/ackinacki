use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::ensure;
use tvm_types::AccountId;

use crate::block_keeper_system::epoch::decode_epoch_data;
use crate::block_keeper_system::BlockKeeperSet;
use crate::block_keeper_system::BlockKeeperSetChange;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::common_section::CommonSectionVersioned;
use crate::types::AckiNackiBlockVersioned;

pub(crate) fn update_block_keeper_set_from_common_section(
    block: &AckiNackiBlockVersioned,
    current_bk_set: Arc<BlockKeeperSet>,
    current_future_bk_set: Arc<BlockKeeperSet>,
    optimistic_state: &OptimisticStateImpl, /* TODO: only for transitioning version, remove after 0.13.0 */
) -> anyhow::Result<Option<(Arc<BlockKeeperSet>, Arc<BlockKeeperSet>)>> {
    let common_section = block.get_common_section();
    let bk_set_changes = match common_section {
        CommonSectionVersioned::New(common_section) => {
            common_section.block_keeper_set_changes.clone()
        }
        CommonSectionVersioned::Old(common_section) => {
            common_section.block_keeper_set_changes.into_iter().map(|v| v.into()).collect()
        }
    };
    if bk_set_changes.is_empty() {
        return Ok(None);
    }
    let mut new_bk_set = current_bk_set.deref().clone();
    let mut new_future_bk_set = current_future_bk_set.deref().clone();
    tracing::trace!(
        "update_block_keeper_set_from_common_section block_id:{} {:?}",
        block.identifier(),
        bk_set_changes
    );

    let mut print_bk_set = false;
    let mut print_future_bk_set = false;
    for block_keeper_change in &bk_set_changes {
        if let BlockKeeperSetChange::BlockKeeperRemoved((signer_index, block_keeper_data)) =
            block_keeper_change
        {
            tracing::trace!("Remove block keeper key: {signer_index} {block_keeper_data:?}");
            let block_keeper_data = new_bk_set.remove_signer(signer_index);
            tracing::trace!("Removed block keeper key: {:?}", block_keeper_data);
            print_bk_set = true;
        }
    }
    #[cfg(feature = "protocol_version_hash_in_block")]
    for block_keeper_change in &bk_set_changes {
        if let BlockKeeperSetChange::BlockKeeperChangedVersion((signer_index, block_keeper_data)) =
            block_keeper_change
        {
            tracing::trace!("Change block keeper version: {signer_index} {block_keeper_data}");
            let old_data = new_bk_set.insert(*signer_index, block_keeper_data.clone());
            ensure!(old_data.is_some(), "block keeper data should exist");
            let data = old_data.unwrap();
            tracing::trace!("change block keeper version old_data: {}", data);
            ensure!(
                block_keeper_data.protocol_support.same_base(&data.protocol_support),
                "Transition base does not match old version"
            );
            print_bk_set = true;
        }
    }
    let accounts = optimistic_state
        .get_shard_state()
        .read_accounts()
        .map_err(|e| anyhow::format_err!("Failed to read state accounts: {e}"))?;
    for block_keeper_change in &bk_set_changes {
        if let BlockKeeperSetChange::BlockKeeperAdded((signer_index, block_keeper_data)) =
            block_keeper_change
        {
            tracing::trace!(
                "insert block keeper key (original): {signer_index} {block_keeper_data}"
            );
            let epoch_address = AccountId::from_str(&block_keeper_data.address)
                .map_err(|e| anyhow::format_err!("Failed to convert epoch address: {e}"))?;
            let epoch_account = accounts
                .account(&epoch_address)
                .map_err(|e| {
                    anyhow::format_err!(
                        "Failed to get account({}): {:?}",
                        block_keeper_data.address,
                        e
                    )
                })?
                .ok_or(anyhow::format_err!("Epoch account is not on the state"))?
                .read_account()
                .map_err(|e| {
                    anyhow::format_err!("Failed to read account from shard account: {:?}", e)
                })?
                .as_struct()
                .map_err(|e| anyhow::format_err!("Failed to convert account: {:?}", e))?;
            let (signer_index_from_acc, epoch_data) = decode_epoch_data(&epoch_account)
                .map_err(|e| anyhow::format_err!("Failed to decode epoch data: {e}"))?
                .ok_or(anyhow::format_err!("Failed to decode epoch data"))?;
            ensure!(signer_index == &signer_index_from_acc, "signer index does not match account");
            tracing::trace!("insert block keeper key (from account): {signer_index} {epoch_data}");
            // Look for transition state
            let transition_state = new_bk_set.get_by_node_id(&epoch_data.node_id()).cloned();
            if let Some(old_bk_data) = transition_state {
                new_bk_set.remove_signer(&old_bk_data.signer_index);
            }
            new_bk_set.insert(*signer_index, epoch_data);
            print_bk_set = true;
            if new_future_bk_set.contains_signer(signer_index) {
                new_future_bk_set.remove_signer(signer_index);
                print_future_bk_set = true;
            }
        }
    }
    for block_keeper_change in &bk_set_changes {
        if let BlockKeeperSetChange::FutureBlockKeeperAdded((signer_index, block_keeper_data)) =
            block_keeper_change
        {
            tracing::trace!("insert future block keeper key: {signer_index} {block_keeper_data}");
            new_future_bk_set.insert(*signer_index, block_keeper_data.clone());
            print_future_bk_set = true;
        }
    }
    tracing::trace!(
        "update_block_keeper_set_from_common_section block_id:{} final_bk_set: {:?}, final_future_bk_set: {:?}",
        block.identifier(),
        new_bk_set,
        new_future_bk_set,
    );
    if print_bk_set {
        tracing::trace!("New bk set full: {}", new_bk_set);
    }
    if print_future_bk_set {
        tracing::trace!("New future bk set full: {}", new_future_bk_set);
    }
    Ok(Some((Arc::new(new_bk_set), Arc::new(new_future_bk_set))))
}
