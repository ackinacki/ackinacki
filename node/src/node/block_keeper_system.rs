// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.

use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::repository::repository_impl::RepositoryImpl;

const _EPOCH_TOUCH_RETRY_TIME_DELTA: u32 = 5;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    // BP node checks current epoch contracts and sends touch message to finish them
    pub(crate) fn _check_and_touch_block_keeper_epochs(&mut self) -> anyhow::Result<()> {
        // TODO: change this mechanism it will not work after moving bk set to block state
        // let now = chrono::Utc::now().timestamp() as u32;
        // let thread_id = self.thread_id.clone();
        // tracing::trace!("check block keepers: now={now}");
        // let (last_block_id, _last_block_seq_no) = self.find_thread_last_block_id_this_node_can_continue(&thread_id)?;
        // let mut last_bk_set = self.get_block_keeper_set_for_block_id(last_block_id).clone().unwrap().deref().clone();
        // for data in last_bk_set.values_mut() {
        //     if data.epoch_finish_timestamp < now {
        //         tracing::trace!("Epoch is outdated: now={now} {data:?}");
        //         match data.status {
        //             // If block keeper was not touched, send touch message, change its status
        //             // and increase saved timestamp with 5 seconds
        //             BlockKeeperStatus::Active => {
        //                 self.production_process.send_epoch_message(
        //                     &thread_id,
        //                     data.clone(),
        //                 );
        //                 data.epoch_finish_timestamp += EPOCH_TOUCH_RETRY_TIME_DELTA;
        //                 data.status = BlockKeeperStatus::CalledToFinish;
        //             },
        //             // If block keeper was already touched, touch it one more time and
        //             // change status for not to change.
        //             BlockKeeperStatus::CalledToFinish => {
        //                 self.production_process.send_epoch_message(
        //                     &thread_id,
        //                     data.clone(),
        //                 );
        //                 data.status = BlockKeeperStatus::Expired;
        //             },
        //             BlockKeeperStatus::Expired => {},
        //         }
        //     }
        // }
        // // TODO: save updated bk_set
        Ok(())
    }
}
