use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;

use parking_lot::Mutex;
use typed_builder::TypedBuilder;

use crate::helper::get_temp_file_path;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::repository::cross_thread_ref_repository::CrossThreadRefDataHistory;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::repository_impl::ThreadSnapshot;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::storage::MessageDurableStorage;
use crate::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

impl AllowGuardedMut for Vec<JoinHandle<anyhow::Result<()>>> {}

#[derive(Clone, TypedBuilder)]
pub struct FileSavingService {
    root_path: PathBuf,
    #[builder(default = Arc::new(Mutex::new(Vec::new())))]
    threads: Arc<Mutex<Vec<JoinHandle<anyhow::Result<()>>>>>,
    repository: RepositoryImpl,
    block_state_repository: BlockStateRepository,
    shared_services: SharedServices,
    message_db: MessageDurableStorage,
}

impl FileSavingService {
    pub fn save_object(
        &self,
        state: Arc<OptimisticStateImpl>,
        path: PathBuf,
    ) -> anyhow::Result<()> {
        let path = self.root_path.join(path);
        let parent_dir = self.root_path.clone();
        let message_db = self.message_db.clone();
        let mut shared_services = self.shared_services.clone();
        let block_state_repository = self.block_state_repository.clone();
        let repository = self.repository.clone();
        let thread = std::thread::Builder::new()
            .name(format!("Saving state: {}", path.display()))
            .spawn(move || {
                let block_id = state.block_id.clone();
                let db_messages = state
                    .messages
                    .iter(&message_db)
                    .map(|range| range.remaining_messages_from_db().unwrap_or_default())
                    .collect();
                let serialized_state = bincode::serialize(&state)?;
                let cross_thread_ref_data_history =
                    shared_services.exec(|e| -> anyhow::Result<Vec<CrossThreadRefData>> {
                        e.cross_thread_ref_data_service.get_history_tail(&block_id)
                    })?;
                let finalized_block = repository
                    .get_finalized_block(&block_id)?
                    .ok_or(anyhow::format_err!("Failed to get block"))?;

                let block_state = block_state_repository.get(&block_id)?;
                let (
                    Some(bk_set),
                    Some(finalized_block_stats),
                    Some(attestation_target),
                    Some(producer_selector),
                    Some(block_height),
                    Some(prefinalization_proof),
                    Some(future_bk_set),
                    Some(descendant_bk_set),
                    Some(descendant_future_bk_set),
                    Some(ancestor_blocks_finalization_checkpoints),
                    Some(finalizes_blocks),
                    Some(parent_id),
                ) = block_state.guarded(|e| {
                    (
                        e.bk_set().clone(),
                        e.block_stats().clone(),
                        *e.attestation_target(),
                        e.producer_selector_data().clone(),
                        *e.block_height(),
                        e.prefinalization_proof().clone(),
                        e.future_bk_set().clone(),
                        e.descendant_bk_set().clone(),
                        e.descendant_future_bk_set().clone(),
                        e.ancestor_blocks_finalization_checkpoints().clone(),
                        e.finalizes_blocks().clone(),
                        e.parent_block_identifier().clone(),
                    )
                })
                else {
                    anyhow::bail!("Failed to get block data for sync");
                };
                let parent_block_state = block_state_repository.get(&parent_id)?;
                let Some(parent_ancestor_blocks_finalization_checkpoints) = parent_block_state
                    .guarded(|e| e.ancestor_blocks_finalization_checkpoints().clone())
                else {
                    anyhow::bail!("Failed to get parent block data for sync");
                };

                let shared_thread_state = ThreadSnapshot::builder()
                    .optimistic_state(serialized_state)
                    .cross_thread_ref_data(cross_thread_ref_data_history)
                    .db_messages(db_messages)
                    .finalized_block(finalized_block.deref().clone())
                    .bk_set(bk_set.deref().clone())
                    .future_bk_set(future_bk_set.deref().clone())
                    .finalized_block_stats(finalized_block_stats)
                    .attestation_target(attestation_target)
                    .producer_selector(producer_selector)
                    .block_height(block_height)
                    .prefinalization_proof(prefinalization_proof)
                    .descendant_bk_set(descendant_bk_set.deref().clone())
                    .descendant_future_bk_set(descendant_future_bk_set.deref().clone())
                    .ancestor_blocks_finalization_checkpoints(
                        ancestor_blocks_finalization_checkpoints,
                    )
                    .finalizes_blocks(finalizes_blocks)
                    .parent_ancestor_blocks_finalization_checkpoints(
                        parent_ancestor_blocks_finalization_checkpoints,
                    )
                    .build();

                let bytes = bincode::serialize(&shared_thread_state)?;
                let tmp_file_path = get_temp_file_path(&parent_dir);
                std::fs::write(tmp_file_path.clone(), bytes)?;
                std::fs::rename(tmp_file_path, path)?;
                Ok(())
            })?;
        self.threads.guarded_mut(|threads| {
            threads.retain(|thread| !thread.is_finished());
            threads.push(thread);
        });
        Ok(())
    }
}
