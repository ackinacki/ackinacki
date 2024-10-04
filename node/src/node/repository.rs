// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::hash::Hash;

use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
use crate::block::BlockIdentifier;
use crate::block::BlockSeqNo;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockFor;
use crate::node::associated_types::BlockIdentifierFor;
use crate::node::associated_types::BlockSeqNoFor;
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::ThreadIdentifierFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::node::DEFAULT_PRODUCTION_TIME_MULTIPLIER;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;

impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
        <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        BlockProducerProcess<Block = BlockFor<TBlockProducerProcess>, Repository = TRepository>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BLSSignatureScheme = TBLSSignatureScheme>,
        <<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block as Block>::BlockSeqNo:
        Eq + Hash,
        ThreadIdentifierFor<TBlockProducerProcess>: Default,
        BlockFor<TBlockProducerProcess>: Clone + Display,
        BlockIdentifierFor<TBlockProducerProcess>: Serialize + for<'de> Deserialize<'de>,
        TValidationProcess: BlockKeeperProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
            BlockSeqNo = BlockSeqNoFor<TBlockProducerProcess>,
            BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,
            ThreadIdentifier = ThreadIdentifierFor<TBlockProducerProcess>,
            Block = BlockFor<TBlockProducerProcess>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Block: From<<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block>,
        TRandomGenerator: rand::Rng,
{
    // Note:
    // TODO: Fix fs repository implementation. reads should not require mut self
    // Future help:
    // https://stackoverflow.com/questions/32062285/how-does-interior-mutability-work-for-caching-behavior
    pub(crate) fn find_thread_last_block_id_this_node_can_continue(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
    ) -> anyhow::Result<(BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>)>
    {
        if let Some(&OptimisticForwardState::ProducedBlock(
            ref block_id_to_continue,
            block_seq_no_to_continue,
        )) = self.cache_forward_optimistic.get(thread_id) {
            // if self.repository.is_optimistic_state_present(block_id_to_continue) {
                return Ok((block_id_to_continue.clone(), block_seq_no_to_continue));
            // }
        }

        let (mut cursor_id, mut cursor_seq_no) =
            self.repository.select_thread_last_finalized_block(thread_id)?;
        loop {
            let next_seq_no = cursor_seq_no.next();
            let mut is_moved = false;
            for candidate in self.repository.list_blocks_with_seq_no(&next_seq_no)? {
                if candidate.data().parent() == cursor_id
                    && self.is_candidate_block_signed_by_this_node(&candidate)?
                    // && self.repository.is_optimistic_state_present(&candidate.data().identifier())
                {
                    cursor_id = candidate.data().identifier();
                    cursor_seq_no = next_seq_no;
                    is_moved = true;
                    break;
                }
            }
            if !is_moved {
                return Ok((cursor_id, cursor_seq_no));
            }
        }
    }

    pub(crate) fn does_this_node_have_signed_block_of_the_same_height(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<bool> {
        let block_seq_no = candidate_block.data().seq_no();
        for candidate in self.repository.list_blocks_with_seq_no(&block_seq_no)? {
            if self.is_candidate_block_signed_by_this_node(&candidate)? && candidate.data().identifier() != candidate_block.data().identifier() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn find_thread_earliest_non_finalized_main_candidate_block_id(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
    ) -> anyhow::Result<
        Option<(BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>)>,
    > {
        let (cursor_id, cursor_seq_no) =
            self.repository.select_thread_last_finalized_block(thread_id)?;
        let next_seq_no = cursor_seq_no.next();
        let _is_moved = false;
        for candidate in self.repository.list_blocks_with_seq_no(&next_seq_no)? {
            let candidate_seq_no = candidate.data().seq_no();
            let candidate_id = candidate.data().identifier();
            if candidate.data().parent() == cursor_id
                && self.repository.is_block_accepted_as_main_candidate(&candidate_id)? == Some(true)
            {
                return Ok(Some((candidate_id, candidate_seq_no)));
            }
        }
        Ok(None)
    }

    pub(crate) fn find_thread_earliest_non_finalized_block(
        &self,
        thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
    ) -> anyhow::Result<
        Option<<Self as NodeAssociatedTypes>::CandidateBlock>,
    > {
        let (cursor_id, cursor_seq_no) =
            self.repository.select_thread_last_finalized_block(thread_id)?;
        let next_seq_no = cursor_seq_no.next();
        let _is_moved = false;
        for candidate in self.repository.list_blocks_with_seq_no(&next_seq_no)? {
            if candidate.data().parent() == cursor_id
                && self.is_candidate_block_signed_by_this_node(&candidate)?
            {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    pub(crate) fn is_candidate_block_older_than_the_last_finalized_block(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<bool> {
        let mut is_older_than_the_last_finalized_block = false;
        let block_seq_no: BlockSeqNoFor<TBlockProducerProcess> = candidate_block.data().seq_no();
        let thread = self.get_block_thread_id(candidate_block)?;
        if self.repository.select_thread_last_finalized_block(&thread)?.1 > block_seq_no {
            is_older_than_the_last_finalized_block = true;
        }

        Ok(is_older_than_the_last_finalized_block)
    }

    pub(crate) fn is_candidate_block_older_or_equal_to_the_last_finalized_block(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<bool> {
        let mut is_older_than_the_last_finalized_block = false;
        let block_seq_no: BlockSeqNoFor<TBlockProducerProcess> = candidate_block.data().seq_no();
        let thread = self.get_block_thread_id(candidate_block)?;
        if self.repository.select_thread_last_finalized_block(&thread)?.1 >= block_seq_no {
            is_older_than_the_last_finalized_block = true;
       }

        Ok(is_older_than_the_last_finalized_block)
    }

    pub(crate) fn is_candidate_block_can_be_applied(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<bool> {
        let parent_id = candidate_block.data().parent();
        // let res = self.repository.is_optimistic_state_present(&parent_id);
        let res = self.validation_process.is_candidate_block_can_be_applied(&parent_id)
            || self.repository.is_optimistic_state_present(&parent_id)
            || self.repository.is_block_verified(&parent_id)?;
        tracing::trace!("is_candidate_block_can_be_applied {:?} parent:{parent_id:?} {res}", candidate_block.data().identifier());
        Ok(res)
    }

    pub(crate) fn on_candidate_block_is_accepted_by_majority(
        &mut self,
        block: BlockFor<TBlockProducerProcess>,
    ) -> anyhow::Result<BlockStatus> {
        let block_id = block.identifier();
        let block_seq_no = block.seq_no();
        log::info!(
            "on_candidate_block_is_accepted_by_majority: {:?} {:?}",
            block.seq_no(),
            &block_id
        );

        let thread_id = ThreadIdentifierFor::<TBlockProducerProcess>::default();

        if self.current_block_producer_id(&thread_id, &block_seq_no) != block.get_common_section().producer_id {
            let producer_group_from_block = block.get_common_section().producer_group;
            let current_producer_group = self.get_producer_group(&thread_id, &block_seq_no);
            if current_producer_group != producer_group_from_block {
                tracing::trace!("set producers group from finalized block: {:?}", producer_group_from_block);
                self.set_producer_groups_from_finalized_state(thread_id, block_seq_no, producer_group_from_block);
            }
            return Ok(BlockStatus::SynchronizationRequired);
        }

        // Check parent block
        let mut parent_id = block.parent();
        log::trace!("check parent id: {parent_id:?}");
        if (parent_id.is_zero()
            || self.repository.is_block_accepted_as_main_candidate(&parent_id)?.unwrap_or(false))
            && !self.repository.is_block_accepted_as_main_candidate(&block_id)?.unwrap_or(false)
        {
            log::info!("Block accepted as main candidate: {:?} {:?}", block.seq_no(), &block_id);
            // If parent was accepted as a main candidate, accept the current block.
            self.repository.mark_block_as_accepted_as_main_candidate(&block_id)?;

            // Check descendant blocks if they can already be accepted.
            let mut next_seq_no = block.seq_no().next();
            parent_id = block_id;
            loop {
                let mut descendant_found = false;
                for block in self.repository.list_blocks_with_seq_no(&next_seq_no)? {
                    if block.data().parent() == parent_id {
                        let signatures_cnt =
                            block.clone_signature_occurrences().iter().filter(|e| *e.1 > 0).count();
                        tracing::trace!("Check accepted block child: {next_seq_no:?} {:?} {signatures_cnt}", block.data().parent());
                        if signatures_cnt
                            >= self.min_signatures_count_to_accept_broadcasted_state(
                            next_seq_no,
                        )
                        {
                            log::info!(
                                "Block accepted as main candidate: {:?} {:?}",
                                block.data().seq_no(),
                                block.data().identifier()
                            );
                            self.repository.mark_block_as_accepted_as_main_candidate(
                                &block.data().identifier(),
                            )?;
                            descendant_found = true;
                            parent_id = block.data().identifier();
                            next_seq_no = next_seq_no.next();
                            break;
                        }
                    }
                }
                if !descendant_found {
                    break;
                }
            }
        } else if !self.repository.is_block_accepted_as_main_candidate(&parent_id)?.unwrap_or(false)
        && !self.repository.is_block_accepted_as_main_candidate(&block_id)?.unwrap_or(false) {
            // If the accepted block can not be applied on this node
            // it usually means that this node is out of sync
            // if !self.repository.is_optimistic_state_present(&block.parent()) {
            //     tracing::trace!("Block was accepted as a main candidate, but can't be applied. Node shall be synced");
            //     return Ok(BlockStatus::TooBigBlockDiff);
            // }

            // If block was not accepted as main candidate, we may have a gap of blocks which were
            // not accepted but have enough signatures. Check them here.
            let (last_accepted_block_id, last_accepted_seq_no) = self.repository.select_thread_last_main_candidate_block(
                &ThreadIdentifierFor::<TBlockProducerProcess>::default()
            )?;

            // Check descendant blocks if they can already be accepted.
            let mut next_seq_no = last_accepted_seq_no.next();
            parent_id = last_accepted_block_id;
            loop {
                let mut descendant_found = false;
                for block in self.repository.list_blocks_with_seq_no(&next_seq_no)? {
                    if block.data().parent() == parent_id {
                        let signatures_cnt =
                            block.clone_signature_occurrences().iter().filter(|e| *e.1 > 0).count();
                        tracing::trace!("Check accepted block child: {next_seq_no:?} {:?} {signatures_cnt}", block.data().parent());
                        if signatures_cnt
                            >= self.min_signatures_count_to_accept_broadcasted_state(
                            next_seq_no,
                        )
                        {
                            log::info!(
                            "Block accepted as main candidate: {:?} {:?}",
                            block.data().seq_no(),
                            block.data().identifier()
                        );
                            self.repository.mark_block_as_accepted_as_main_candidate(
                                &block.data().identifier(),
                            )?;
                            descendant_found = true;
                            parent_id = block.data().identifier();
                            next_seq_no = next_seq_no.next();
                            break;
                        }
                    }
                }
                if !descendant_found {
                    break;
                }
            }
        }
        // TODO:
        // 1. For each block accepted as a candidate by majority
        // we have to check running producers
        // if they are building blocks for the correct parent.
        // In case accepted block is diverged from the one this node
        // expected, than the running production for this thread
        // must be reset to produce from the new wildly accepted
        // candidate block.
        //
        // 2. Check if this node has to validate the block
        // validation_process
        // todo!();
        // 3. Finalize all blocks that have time passed
        // Ok(BlockStatus::Ok)
        Ok(BlockStatus::Ok)
    }

    pub(crate) fn on_block_finalized(
        &mut self,
        block_id: &BlockIdentifierFor<TBlockProducerProcess>,
        block_seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        if self.production_timeout_multiplier != DEFAULT_PRODUCTION_TIME_MULTIPLIER {
            let last_processed_block_seq_no = self.repository.last_stored_block_by_seq_no()?;
            if (last_processed_block_seq_no.into() - (*block_seq_no).into())
                < self.config.global.finalization_delay_to_slow_down
            {
                log::trace!(
                    "Finalization delay is within normal range ({} < {}), speed up production.",
                    last_processed_block_seq_no.into() - (*block_seq_no).into(),
                    self.config.global.finalization_delay_to_slow_down
                );
                self.production_timeout_multiplier = DEFAULT_PRODUCTION_TIME_MULTIPLIER;
                self.production_process.set_timeout(self.get_production_timeout());
            } else if (last_processed_block_seq_no.into() - (*block_seq_no).into())
                < self.config.global.finalization_delay_to_stop
            {
                log::trace!(
                    "Finalization proceed ({} < {}), speed up production.",
                    last_processed_block_seq_no.into() - (*block_seq_no).into(),
                    self.config.global.finalization_delay_to_stop
                );
                self.production_timeout_multiplier = self.config.global.slow_down_multiplier;
                self.production_process.set_timeout(self.get_production_timeout());
            }
        }
        log::info!("on_block_finalized: {:?} {:?}", block_seq_no, block_id);
        self.repository.mark_block_as_finalized(block_id)?;
        log::info!("Block marked as finalized: {:?} {:?}", block_seq_no, block_id);
        let block = self.repository.get_block(block_id)?.expect("Just finalized");
        log::info!("Last finalized block data: {}", block);
        // let wrapped_block = block.data().clone();
        // self.repository.get_finalized_state_as_mut().apply_block(wrapped_block)?;

        self.raw_block_tx.send(bincode::serialize(&block)?)?;

        if block.data().directives().share_state_resource_address.is_some() {
            self.broadcast_candidate_block(block.clone())?;
        }
        let thread_id = ThreadIdentifierFor::<TBlockProducerProcess>::default();
        if self.current_block_producer_id(&thread_id, block_seq_no) != block.data().get_common_section().producer_id {
            let producer_group_from_block = block.data().get_common_section().producer_group;
            let current_producer_group = self.get_producer_group(&thread_id, block_seq_no);
            if current_producer_group != producer_group_from_block {
                tracing::trace!("set producers group from finalized block: {:?}", producer_group_from_block);
                self.set_producer_groups_from_finalized_state(thread_id, *block_seq_no, producer_group_from_block);
            }
        }

        log::info!("Block loaded from repo: {:?}", block_id);
        // Cleanup states
        // TODO:
        // This implementation works with single thread only,
        // since merges and splits would result in blocks being
        // in multiple threads simultaneously.

        let thread_id = self.get_block_thread_id(&block)?;
        let mut to_delete = vec![];
        let stored_finalized_blocks =
            self.repository.list_stored_thread_finalized_blocks(&thread_id)?;
        for (block_id, block_seq_no) in stored_finalized_blocks {
            if block.data().seq_no() > block_seq_no {
                to_delete.push(block_id);
            }
        }
        for block_id in to_delete.into_iter().unique() {
            self.repository.erase_block_and_optimistic_state(&block_id)?;
        }
        self.clear_unprocessed_till(block_seq_no)?;
        Ok(())
    }
}
