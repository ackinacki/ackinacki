// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::ops::Deref;
use std::time::Duration;

use telemetry_utils::mpsc::InstrumentedSender;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::message_storage::MessageDurableStorage;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::services::sync::StateSyncService;
use crate::node::shared_services::SharedServices;
use crate::repository::cross_thread_ref_repository::CrossThreadRefDataHistory;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::repository_impl::ThreadSnapshot;
use crate::repository::repository_impl::WrappedStateSnapshot;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::services::blob_sync::external_fileshares_based::ServiceInterface;
use crate::services::blob_sync::BlobSyncService;
use crate::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::utilities::guarded::Guarded;

#[derive(Clone)]
pub struct ExternalFileSharesBased {
    pub static_storages: Vec<url::Url>,
    pub max_download_tries: u8,
    pub retry_download_timeout: std::time::Duration,
    pub download_deadline_timeout: std::time::Duration,
    blob_sync: ServiceInterface,
}

impl ExternalFileSharesBased {
    pub fn new(blob_sync: ServiceInterface) -> Self {
        // TODO: move to config
        Self {
            static_storages: vec![],
            max_download_tries: 3,
            retry_download_timeout: Duration::from_secs(2),
            download_deadline_timeout: Duration::from_secs(120),
            blob_sync,
        }
    }
}

impl StateSyncService for ExternalFileSharesBased {
    type Repository = RepositoryImpl;
    type ResourceAddress = String;

    fn generate_resource_address(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Self::ResourceAddress> {
        Ok(block_id.to_string())
    }

    fn add_share_state_task(
        &mut self,
        finalized_block: &Envelope<GoshBLS, AckiNackiBlock>,
        message_db: &MessageDurableStorage,
        repository: &RepositoryImpl,
        block_state_repository: &BlockStateRepository,
        shared_services: &SharedServices,
    ) -> anyhow::Result<Self::ResourceAddress> {
        let mut shared_services = shared_services.clone();
        let cur_thread = finalized_block.data().get_common_section().thread_id;
        let cur_block_id = finalized_block.data().identifier();
        tracing::trace!("add_share_state_task: {:?} {:?}", cur_thread, cur_block_id);
        let mut threads = VecDeque::new();
        threads.push_back((cur_thread, cur_block_id.clone()));
        let mut all_threads_set = HashSet::new();

        let cid = self.generate_resource_address(&cur_block_id)?;
        // TODO: fix. do not load entire state into memory.
        let mut thread_states = HashMap::new();

        // Add refs
        while let Some((thread_id, block_id)) = threads.pop_front() {
            let state = repository
                .get_optimistic_state(&block_id, &thread_id, None)?
                .expect("missing optimistic state");
            if all_threads_set.is_empty() {
                all_threads_set.insert(thread_id);
                let thread_ref_state = state.get_thread_refs().clone();
                all_threads_set.extend(thread_ref_state.all_thread_refs().keys().cloned());
                threads.extend(thread_ref_state.all_thread_refs().iter().map(
                    |(thread_id, ref_block)| (*thread_id, ref_block.block_identifier.clone()),
                ));
            }
            let db_messages = state
                .messages
                .iter(message_db)
                .map(|range| range.remaining_messages_from_db().unwrap_or_default())
                .collect();
            let serialized_state = state.serialize_into_buf(false)?;
            let cross_thread_ref_data_history =
                shared_services.exec(|e| -> anyhow::Result<Vec<CrossThreadRefData>> {
                    e.cross_thread_ref_data_service.get_history_tail(&block_id)
                })?;
            let bk_set = block_state_repository
                .get(&block_id)?
                .guarded(|e| e.bk_set().clone().expect("Must be set"));
            let finalized_block_stats = block_state_repository
                .get(&block_id)?
                .guarded(|e| e.block_stats().clone().expect("Must be set"));
            let attestation_target = block_state_repository
                .get(&block_id)?
                .guarded(|e| (*e.initial_attestations_target()).expect("Must be set"));
            let producer_selector = block_state_repository
                .get(&block_id)?
                .guarded(|e| e.producer_selector_data().clone().expect("Must be set"));
            let finalized_block = repository.get_block(&block_id)?.expect("missing block");

            tracing::trace!(
                "add_share_state_task: add state for {:?} {:?}",
                finalized_block.data().get_common_section().thread_id,
                finalized_block.data().identifier()
            );

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
            thread_states.insert(thread_id, shared_thread_state);
            // TODO: check ref refs
        }

        let data = bincode::serialize(
            &WrappedStateSnapshot::builder().thread_states(thread_states).build(),
        )?;

        self.blob_sync.share_blob(cid.clone(), std::io::Cursor::new(data), |_| {
            // Refactoring from an old code. It didn't care about results :(
        })?;

        Ok(cid)
    }

    fn add_load_state_task(
        &mut self,
        resource_address: Self::ResourceAddress,
        output: InstrumentedSender<anyhow::Result<(Self::ResourceAddress, Vec<u8>)>>,
    ) -> anyhow::Result<()> {
        self.blob_sync.load_blob(
            resource_address.clone(),
            self.static_storages.clone(),
            self.max_download_tries,
            Some(self.retry_download_timeout),
            Some(std::time::Instant::now() + self.download_deadline_timeout),
            {
                // Handle success
                let resource_address = resource_address.clone();
                let output = output.clone();
                move |e| {
                    let mut buffer: Vec<u8> = vec![];
                    let _ = output.send(match e.read_to_end(&mut buffer) {
                        Ok(_size) => Ok((resource_address, buffer)),
                        Err(e) => Err(e.into()),
                    });
                }
            },
            {
                move |e| {
                    // Handle error
                    let _ = output.send(Err(e));
                }
            },
        )?;
        Ok(())
    }
}
