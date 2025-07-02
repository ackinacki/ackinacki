use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;

use parking_lot::Mutex;
use typed_builder::TypedBuilder;

use crate::message_storage::MessageDurableStorage;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::repository::cross_thread_ref_repository::CrossThreadRefDataHistory;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::repository_impl::ThreadSnapshot;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
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
                ) = block_state.guarded(|e| {
                    (
                        e.bk_set().clone(),
                        e.block_stats().clone(),
                        *e.initial_attestations_target(),
                        e.producer_selector_data().clone(),
                        *e.block_height(),
                        e.prefinalization_proof().clone(),
                    )
                })
                else {
                    anyhow::bail!("Failed to get block data");
                };

                let shared_thread_state = ThreadSnapshot::builder()
                    .optimistic_state(serialized_state)
                    .cross_thread_ref_data(cross_thread_ref_data_history)
                    .db_messages(db_messages)
                    .finalized_block(finalized_block.deref().clone())
                    .bk_set(bk_set.deref().clone())
                    .finalized_block_stats(finalized_block_stats)
                    .attestation_target(attestation_target)
                    .producer_selector(producer_selector)
                    .block_height(block_height)
                    .prefinalization_proof(prefinalization_proof)
                    .build();

                let bytes = bincode::serialize(&shared_thread_state)?;
                std::fs::write(path.clone(), bytes)?;
                Ok(())
            })?;
        self.threads.guarded_mut(|threads| {
            threads.retain(|thread| !thread.is_finished());
            threads.push(thread);
        });
        Ok(())
    }
}
