// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Duration;

use telemetry_utils::mpsc::InstrumentedSender;

use crate::block_keeper_system::BlockKeeperSet;
use crate::message_storage::MessageDurableStorage;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use crate::node::services::sync::StateSyncService;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::repository_impl::WrappedStateSnapshot;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::services::blob_sync::external_fileshares_based::ServiceInterface;
use crate::services::blob_sync::BlobSyncService;
use crate::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;

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
        Self {
            static_storages: vec![],
            max_download_tries: 3,
            retry_download_timeout: Duration::from_secs(2),
            download_deadline_timeout: Duration::from_secs(30),
            blob_sync,
        }
    }
}

impl StateSyncService for ExternalFileSharesBased {
    type Repository = RepositoryImpl;
    type ResourceAddress = String;

    fn generate_resource_address(
        &self,
        state: &OptimisticStateImpl,
    ) -> anyhow::Result<Self::ResourceAddress> {
        Ok(state.get_block_id().to_string())
    }

    fn add_share_state_task(
        &mut self,
        state: <Self::Repository as Repository>::OptimisticState,
        cross_thread_ref_data: Vec<CrossThreadRefData>,
        finalized_block_stats: BlockStatistics,
        bk_set: BlockKeeperSet,
        message_db: &MessageDurableStorage,
    ) -> anyhow::Result<Self::ResourceAddress> {
        tracing::trace!("add_share_state_task");
        let cid = self.generate_resource_address(&state)?;
        // TODO: fix. do not load entire state into memory.
        let db_messages = state
            .messages
            .iter(message_db)
            .map(|range| range.remaining_messages_from_db().unwrap_or_default())
            .collect();
        let serialized_state = state.serialize_into_buf(false)?;
        let data = bincode::serialize(
            &WrappedStateSnapshot::builder()
                .optimistic_state(serialized_state)
                .cross_thread_ref_data(cross_thread_ref_data)
                .finalized_block_stats(finalized_block_stats)
                .bk_set(bk_set)
                .db_messages(db_messages)
                .build(),
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
