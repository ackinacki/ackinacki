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
                let bk_set = block_state_repository
                    .get(&block_id)?
                    .guarded(|e| e.bk_set().clone())
                    .ok_or(anyhow::format_err!("Failed to get bk_set"))?;
                let finalized_block_stats = block_state_repository
                    .get(&block_id)?
                    .guarded(|e| e.block_stats().clone())
                    .ok_or(anyhow::format_err!("Failed to get block_stats"))?;
                let attestation_target = block_state_repository
                    .get(&block_id)?
                    .guarded(|e| (*e.initial_attestations_target()))
                    .ok_or(anyhow::format_err!("Failed to get attestation target"))?;
                let producer_selector = block_state_repository
                    .get(&block_id)?
                    .guarded(|e| e.producer_selector_data().clone())
                    .ok_or(anyhow::format_err!("Failed to get bp selector"))?;
                let finalized_block = repository
                    .get_block(&block_id)?
                    .ok_or(anyhow::format_err!("Failed to get block"))?;

                let shared_thread_state = ThreadSnapshot::builder()
                    .optimistic_state(serialized_state)
                    .cross_thread_ref_data(cross_thread_ref_data_history)
                    .db_messages(db_messages)
                    .finalized_block(finalized_block.deref().clone())
                    .bk_set(bk_set.deref().clone())
                    .finalized_block_stats(finalized_block_stats)
                    .attestation_target(attestation_target)
                    .producer_selector(producer_selector)
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
