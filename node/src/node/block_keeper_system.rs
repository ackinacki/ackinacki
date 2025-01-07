// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block_keeper_system::BlockKeeperSet;
use crate::block_keeper_system::BlockKeeperSetChange;
use crate::block_keeper_system::BlockKeeperStatus;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::block_keeper_ring::BlockKeeperRing;
use crate::types::AckiNackiBlock;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

const EPOCH_TOUCH_RETRY_TIME_DELTA: u32 = 5;

impl<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

        >,
        TRepository: Repository<
            BLS = GoshBLS,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<GoshBLS, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<GoshBLS, AttestationData>,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        >,
        TRandomGenerator: rand::Rng,
{
    // BP node checks current epoch contracts and sends touch message to finish them
    pub(crate) fn check_and_touch_block_keeper_epochs(
        &mut self,
        // TODO: remove
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp() as u32;
        tracing::trace!("check block keepers: now={now}");
        self.block_keeper_sets.with_last_entry(|last_bk_set| {
            let mut last_bk_set = last_bk_set.expect("Block keeper set map should not be empty");
            for data in last_bk_set.get_mut().values_mut() {
                if data.epoch_finish_timestamp < now {
                    tracing::trace!("Epoch is outdated: now={now} {data:?}");

                    match data.status {
                        // If block keeper was not touched, send touch message, change its status
                        // and increase saved timestamp with 5 seconds
                        BlockKeeperStatus::Active => {
                            self.production_process.send_epoch_message(
                                &self.thread_id,
                                data.clone(),
                            );
                            data.epoch_finish_timestamp += EPOCH_TOUCH_RETRY_TIME_DELTA;
                            data.status = BlockKeeperStatus::CalledToFinish;
                        },
                        // If block keeper was already touched, touch it one more time and
                        // change status for not to change.
                        BlockKeeperStatus::CalledToFinish => {
                            self.production_process.send_epoch_message(
                                &self.thread_id,
                                data.clone(),
                            );
                            data.status = BlockKeeperStatus::Expired;
                        },
                        BlockKeeperStatus::Expired => {},
                    }
                }
            }
        });
        Ok(())
    }

    pub(crate) fn update_block_keeper_set_from_common_section(
        &mut self,
        block: &AckiNackiBlock,
        // TODO: remove
        _thread_id: &ThreadIdentifier
    ) -> anyhow::Result<()> {
        let common_section = block.get_common_section();
        if common_section.block_keeper_set_changes.is_empty() {
            return Ok(());
        }
        tracing::trace!(
            "update_block_keeper_set_from_common_section {:?} {:?}",
            self.block_keeper_sets,
            common_section.block_keeper_set_changes
        );
        let block_seq_no = block.seq_no();
        let (_last_bk_set_seq_no, mut new_bk_set) = self.block_keeper_sets.with_last_entry(|e| {
            let e = e.expect("Block keeper sets should not be empty");
            (*e.key(), e.get().clone())
        });
        // TODO: it was not thought through for multithreaded environment.
        // ensure!( block_seq_no > _last_bk_set_seq_no);
        // Process removes first, because remove and add can happen in one block
        for block_keeper_change in &common_section.block_keeper_set_changes {
            if let BlockKeeperSetChange::BlockKeeperRemoved((signer_index, block_keeper_data)) = block_keeper_change {
                tracing::trace!("Remove block keeper key: {signer_index} {block_keeper_data:?}");
                tracing::trace!("Remove block keeper key: {:?}", new_bk_set);
                let block_keeper_data = new_bk_set.remove(signer_index);
                tracing::trace!("Removed block keeper key: {:?}", block_keeper_data);
            }
        }
        for block_keeper_change in &common_section.block_keeper_set_changes {
            if let BlockKeeperSetChange::BlockKeeperAdded((signer_index, block_keeper_data)) = block_keeper_change {
                tracing::trace!("insert block keeper key: {signer_index} {block_keeper_data}");
                tracing::trace!("insert block keeper key: {:?}", new_bk_set);
                new_bk_set.insert(*signer_index, block_keeper_data.clone());
                if block_keeper_data.wallet_index == self.config.local.node_id {
                    self.signer_index_map.insert(
                        block.seq_no(),
                        *signer_index,
                    );
                }
            }
        }
        self.block_keeper_sets.insert(block_seq_no, new_bk_set);
        tracing::trace!("update_block_keeper_set_from_common_section finished {:?}", self.block_keeper_sets);
        Ok(())
    }

    pub fn get_block_keeper_set(
        &self,
        block_seq_no: &BlockSeqNo,
        // TODO: remove
        _thread_id: &ThreadIdentifier,
    ) -> BlockKeeperSet {
        // TODO: use _thread_id to shuffle bk_set
        let block_keeper_sets_for_thread = self.block_keeper_sets.clone_inner();
        for (seq_no, bk_set) in block_keeper_sets_for_thread.iter().rev() {
            if seq_no > block_seq_no {
                continue;
            }
            return bk_set.clone();
        }
        panic!("Failed to find BK set for block with seq_no: {block_seq_no:?}")
    }

    pub fn current_block_keeper_set(
        &mut self,
        // TODO: remove
        _thread_id: &ThreadIdentifier,
    ) -> BlockKeeperSet {
        self.block_keeper_sets.with_last_entry(|e| {
            e.expect("Node should have latest BK set").get().clone()
        })
    }

    pub fn set_block_keeper_sets(
        &mut self,
        block_keeper_sets: BTreeMap<BlockSeqNo, BlockKeeperSet>
    ) {
        self.block_keeper_sets.replace(block_keeper_sets);
    }

    pub fn get_block_keeper_sets_for_all_threads(
        &self,
    ) -> BlockKeeperRing {
        self.block_keeper_sets.clone()
    }
}
