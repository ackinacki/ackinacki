// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

use itertools::Itertools;
use rand::prelude::SliceRandom;

use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
#[cfg(feature = "misbehave")]
use crate::misbehavior::misbehave_rules;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use crate::node::services::block_processor::rules;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::DEFAULT_PRODUCTION_TIME_MULTIPLIER;
use crate::repository::cross_thread_ref_repository::CrossThreadRefDataHistory;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

const CLEANUP_HACK: u32 = 1000;

impl<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = RepositoryImpl>,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateImpl,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = RepositoryImpl
        >,
        TRandomGenerator: rand::Rng,
{
    // Note:
    // TODO: Fix fs repository implementation. reads should not require mut self
    // Future help:
    // https://stackoverflow.com/questions/32062285/how-does-interior-mutability-work-for-caching-behavior
    pub(crate) fn find_thread_last_block_id_this_node_can_continue(
        &mut self,
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo)>>
    {
        tracing::trace!("find_thread_last_block_id_this_node_can_continue start");
        let (finalized_block_id, finalized_block_seq_no) = self.repository.select_thread_last_finalized_block(&self.thread_id)?;
        if let Some(OptimisticForwardState::ProducedBlock(
            ref block_id_to_continue,
            block_seq_no_to_continue,
        )) = self.cache_forward_optimistic {
            if block_seq_no_to_continue >= finalized_block_seq_no {
                tracing::trace!("find_thread_last_block_id_this_node_can_continue take from cache: {:?} {:?}", block_seq_no_to_continue, block_id_to_continue);
                return Ok(Some((block_id_to_continue.clone(), block_seq_no_to_continue)));
            }
        }
        tracing::trace!("find_thread_last_block_id_this_node_can_continue cache is not valid for continue or empty, clear it");
        self.cache_forward_optimistic = None;

        let mut unprocessed_blocks = {
            self.unprocessed_blocks_cache.lock().clone()
        };
        unprocessed_blocks.retain(|block_state| block_state.guarded(|e| {
            if !e.is_invalidated() && !e.is_finalized() && e.is_block_already_applied() && e.descendant_bk_set().is_some() {
                let bk_set = e.get_descendant_bk_set();
                let node_in_bk_set = bk_set.iter_node_ids().any(|node_id| node_id == &self.config.local.node_id);
                let block_seq_no = (*e.block_seq_no()).expect("must be set");
                node_in_bk_set && block_seq_no > finalized_block_seq_no
            } else {
                false
            }
        }));

        tracing::trace!("find_thread_last_block_id_this_node_can_continue unprocessed applied blocks len: {}", unprocessed_blocks.len());
        let mut applied_unprocessed_tails: HashSet<BlockIdentifier> = HashSet::from_iter(unprocessed_blocks.iter().map(|block_state| block_state.block_identifier().clone()).collect::<Vec<_>>());
        for block_state in unprocessed_blocks.iter() {
            let parent_id = block_state.guarded(|e| e.parent_block_identifier().clone()).expect("Applied block must have parent id set");
            applied_unprocessed_tails.remove(&parent_id);
        }
        tracing::trace!("find_thread_last_block_id_this_node_can_continue applied unprocessed tails: {:?}", applied_unprocessed_tails);


        // Filter chains that do not lead to the finalized block
        unprocessed_blocks.retain(|block_state|
            applied_unprocessed_tails.contains(block_state.block_identifier())
                && self.blocks_states.select_unfinalized_ancestor_blocks(block_state.clone(), finalized_block_seq_no).is_ok());

        unprocessed_blocks.shuffle(&mut rand::thread_rng());

        // Check if one of tails was produced by this node
        for block_state in &unprocessed_blocks {
            if block_state.guarded(|e| e.producer().clone()).expect("Applied block must have parent id set") == self.config.local.node_id {
                let (block_id, block_seq_no) = block_state.guarded(|e| (e.block_identifier().clone(), (*e.block_seq_no()).expect("must be set")));
                tracing::trace!("find_thread_last_block_id_this_node_can_continue found tail produced by this node seq_no: {} block_id:{:?}", block_seq_no, block_id);
                return Ok(Some((block_id, block_seq_no)));
            }
        }

        // Check is one of tails can be continued by this node based on blocks gap
        let gap = {
            *self.block_gap_length.lock()
        };
        let offset = gap / self.config.global.producer_change_gap_size;
        tracing::trace!("find_thread_last_block_id_this_node_can_continue check with offset={}", offset);

        #[cfg(feature = "misbehave")]
        {
            match misbehave_rules() {
                Ok(Some(rules)) => {
                    for block_state in unprocessed_blocks.clone() {
                        let (block_id, block_seq_no) = block_state
                            .guarded(|e| (e.block_identifier().clone(), (*e.block_seq_no())
                            .expect("must be set")));

                        // Enable misbehaviour if block seqno is in a range
                        let seq_no: u32 = block_seq_no.into();
                        if seq_no >= rules.fork_test.from_seq && seq_no <= rules.fork_test.to_seq {
                            tracing::trace!("Misbehaving, unprocessed block seq_no: {} block_id:{:?}", block_seq_no, block_id);
                            return Ok(Some((block_id, block_seq_no)));
                        }
                    }
                    let seq_no: u32 = finalized_block_seq_no.into();
                    if seq_no >= rules.fork_test.from_seq && seq_no <= rules.fork_test.to_seq {
                        tracing::trace!("Misbehaving, finalized block seq_no: {} block_id:{:?}", finalized_block_seq_no, finalized_block_id);
                        return Ok(Some((finalized_block_id, finalized_block_seq_no)));
                    }
                },
                Ok(None) => {
                    // this host should nor apply misbehave rules
                },
                Err(error) => {
                    tracing::error!("Can not parse misbehaving rules from provided file, fatal error: {:?}", error);
                    std::process::exit(1);
                }
            }
        } //-- end of misbehave block

        for block_state in unprocessed_blocks {
            if block_state.guarded(|e| {
                let bk_set = e.get_descendant_bk_set();
                let producer_selector = e.producer_selector_data().clone().expect("must be set");
                producer_selector.check_whether_this_node_is_bp_based_on_bk_set_and_index_offset(
                    &bk_set,
                    &self.config.local.node_id,
                    offset,
                )
            }) {
                let (block_id, block_seq_no) = block_state.guarded(|e| (e.block_identifier().clone(), (*e.block_seq_no()).expect("must be set")));
                tracing::trace!("find_thread_last_block_id_this_node_can_continue found tail this node can continue base on offset seq_no: {} block_id:{:?}", block_seq_no, block_id);
                return Ok(Some((block_id, block_seq_no)));
            }
        }
        tracing::trace!("find_thread_last_block_id_this_node_can_continue check last finalized block seq_no: {} block_id: {:?}", finalized_block_seq_no, finalized_block_id);
        let block_state = self.blocks_states.get(&finalized_block_id)?;
        rules::descendant_bk_set::set_descendant_bk_set(&block_state, &self.repository);
        if block_state.guarded(|e| e.descendant_bk_set().is_none()) {
            return Ok(None);
        }
        if block_state.guarded(|e| {
            assert!(e.bk_set().is_some(), "Finalized block must have bk set set");
            let bk_set = e.get_descendant_bk_set();
            let producer_selector = e.producer_selector_data().clone().expect("must be set");
            producer_selector.check_whether_this_node_is_bp_based_on_bk_set_and_index_offset(
                &bk_set,
                &self.config.local.node_id,
                offset,
            )
        }) {
            tracing::trace!("find_thread_last_block_id_this_node_can_continue node can continue last finalized block");
            return Ok(Some((finalized_block_id, finalized_block_seq_no)));
        }
        Ok(None)
    }

    // TODO: remove this method, check common parent instead
    pub(crate) fn does_this_node_have_signed_block_of_the_same_height(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<bool> {
        let block_seq_no = candidate_block.data().seq_no();
        let thread_id = candidate_block.data().get_common_section().thread_id;
        for candidate in self.repository.list_blocks_with_seq_no(&block_seq_no, &thread_id)? {
            if self.is_candidate_block_signed_by_this_node(&candidate)? && candidate.data().identifier() != candidate_block.data().identifier() {
                return Ok(true);
            }
        }
        Ok(false)
    }


    pub(crate) fn _find_thread_earliest_non_finalized_block(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<
        Option<<Self as NodeAssociatedTypes>::CandidateBlock>,
    > {
        let (cursor_id, cursor_seq_no) =
            self.repository.select_thread_last_finalized_block(thread_id)?;
        let local_next_seq_no = next_seq_no(cursor_seq_no);
        let _is_moved = false;
        for candidate in self.repository.list_blocks_with_seq_no(&local_next_seq_no, thread_id)? {
            if candidate.data().parent() == cursor_id
                // && self.is_candidate_block_signed_by_this_node(&candidate)?
            {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    pub(crate) fn on_block_finalized(
        &mut self,
        block: &<Self as NodeAssociatedTypes>::CandidateBlock
    ) -> anyhow::Result<()> {
        let block_seq_no = block.data().seq_no();
        let block_id = block.data().identifier();
        let thread_id = block.data().get_common_section().thread_id;
        if self.production_timeout_multiplier != DEFAULT_PRODUCTION_TIME_MULTIPLIER {
            let last_processed_block_seq_no = self.repository.last_stored_block_by_seq_no(&thread_id)?;
            if (last_processed_block_seq_no - block_seq_no)
                < self.config.global.finalization_delay_to_slow_down
            {
                tracing::trace!(
                    "Finalization delay is within normal range ({} < {}), speed up production.",
                    last_processed_block_seq_no - block_seq_no,
                    self.config.global.finalization_delay_to_slow_down
                );
                self.production_timeout_multiplier = DEFAULT_PRODUCTION_TIME_MULTIPLIER;
                self.production_process.set_timeout(self.get_production_timeout());
            } else if (last_processed_block_seq_no - block_seq_no)
                < self.config.global.finalization_delay_to_stop
            {
                tracing::trace!(
                    "Finalization proceed ({} < {}), speed up production.",
                    last_processed_block_seq_no - block_seq_no,
                    self.config.global.finalization_delay_to_stop
                );
                self.production_timeout_multiplier = self.config.global.slow_down_multiplier;
                self.production_process.set_timeout(self.get_production_timeout());
            }
        }
        tracing::info!("on_block_finalized: {:?} {:?}", block_seq_no, block_id);
        self.repository.mark_block_as_finalized(
            block,
            Arc::clone(&self.nack_set_cache),
            self.blocks_states.get(&block_id)?
        )?;
        self.clear_block_gap();
        tracing::info!("Block marked as finalized: {:?} {:?} {:?}", block_seq_no, block_id, thread_id);
        let block = self.repository.get_block(&block_id)?.expect("Just finalized");
        tracing::info!(
            "Last finalized block data: seq_no: {:?}, block_id: {:?}, producer_id: {}, signatures: {:?}, thread_id: {:?}, tx_cnt: {}",
            block.data().seq_no(),
            block.data().identifier(),
            block.data().get_common_section().producer_id,
            block.clone_signature_occurrences(),
            block.data().get_common_section().thread_id,
            block.data().tx_cnt(),
        );
        tracing::info!("Last finalized block common section: {:?}", block.data().get_common_section());

        self.raw_block_tx.send(bincode::serialize(&block)?)?;

        // Share finalized state, producer of this block has already shared this state after block production
        if block.data().directives().share_state_resource_address.is_some() {
            let optimistic_state = self.repository.get_optimistic_state(&block.data().identifier(), Arc::clone(&self.nack_set_cache))?.expect("Failed to get finalized block optimistic state");
            let cross_thread_ref_data_history = self.shared_services.exec(|e| -> anyhow::Result<Vec<CrossThreadRefData>> {
                e.cross_thread_ref_data_service.get_history_tail(&block.data().identifier())
            })?;
            let (finalized_block_stats, bk_set) = self.blocks_states.get(&block.data().identifier())?.guarded(|e| (e.block_stats().clone().expect("Must be set"), e.bk_set().clone().expect("Must be set").deref().clone()));
            let _resource_address = self.state_sync_service.add_share_state_task(
                optimistic_state.clone(),
                cross_thread_ref_data_history,
                finalized_block_stats,
                bk_set,
            )?;
            // self.broadcast_candidate_block(block.clone())?;
        }
        // TODO: Fix code for new BP selector
        // if self.current_block_producer_id(&block_seq_no) != block.data().get_common_section().producer_id {
        //     let producer_group_from_block = block.data().get_common_section().producer_group.clone();
        //     let current_producer_group = self.get_producer_group(&block_seq_no);
        //     if current_producer_group != producer_group_from_block {
        //         tracing::trace!("set producers group from finalized block: {:?}", producer_group_from_block);
        //         self.set_producer_groups_from_finalized_state(block_seq_no, producer_group_from_block);
        //     }
        // }

        tracing::info!("Block loaded from repo: {:?}", block_id);
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
             if block.data().seq_no() > block_seq_no + CLEANUP_HACK {
                to_delete.push(block_id);
            }
        }
        for block_id in to_delete.into_iter().unique() {
            self.repository.erase_block_and_optimistic_state(&block_id, &thread_id)?;
        }
        self.clear_old_acks_and_nacks(&block_seq_no)?;

        self.shared_services.on_block_finalized(
            block.data(),
            &mut self.repository.get_optimistic_state(&block.data().identifier(), Arc::clone(&self.nack_set_cache))?
                .ok_or(anyhow::anyhow!("Block must be in the repo"))?
        );
        Ok(())
    }
}
