// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::services::sync::StateSyncService;
use crate::node::GoshBLS;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

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
    pub(crate) fn on_incoming_candidate_block(
        &mut self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
        resend_source_node_id: Option<NodeIdentifier>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "Incoming block candidate: {}, signatures: {:?}, resend_source_node_id: {:?}",
            candidate_block.data(),
            candidate_block.clone_signature_occurrences(),
            resend_source_node_id
        );
        // Check if we already have this block
        let block_state = self.blocks_states.get(&candidate_block.data().identifier())?;
        block_state.guarded_mut(|e|{
            let block_producer = candidate_block.data().get_common_section().producer_id.clone();
            e.try_add_attestations_interest(block_producer)
        })?;
        if let Some(node_id) = resend_source_node_id {
            block_state
                .guarded_mut(|e|e.try_add_attestations_interest(node_id))?;
        };

        if block_state.guarded(|e| e.is_stored()) {
            // We already have this block stored in repo
            tracing::trace!("Block with the same id was already stored in repo");
            // TODO: need to handle a valid situation when BP has not received our attestations and resends the block
            return Ok(());
        }

        let Some(producer_selector) = candidate_block.data().get_common_section().producer_selector.clone()
        else {
            tracing::trace!("Incoming block doesn't have producer selector, Skip ir");
            return Ok(());
        };

        // Store the incoming block
        self.repository.store_block(candidate_block.clone())?;

        let mut guarded_block_state = block_state.lock();
        // Initialize block state
        guarded_block_state.set_stored()?;
        guarded_block_state.set_block_seq_no(candidate_block.data().seq_no())?;
        guarded_block_state.set_thread_identifier(candidate_block.data().get_common_section().thread_id)?;

        // Guard to not set it to the produced block
        if guarded_block_state.producer_selector_data().is_none() {
            guarded_block_state.set_producer_selector_data(producer_selector)?;
        }

        let parent_id = candidate_block.data().parent();
        guarded_block_state.set_parent_block_identifier(parent_id.clone())?;
        drop(guarded_block_state);

        // Update parent block state
        // lock parent only after child lock is dropped
        let parent_block_has_several_children = self.blocks_states.get(&parent_id)?.guarded_mut(|e| {
            e.add_child(candidate_block.data().identifier())?;
            Ok::<bool,anyhow::Error>(e.known_children().len() > 1)
        })?;
        if parent_block_has_several_children {
            self.fork_resolution_service.found_fork(&parent_id)?;
        }

        tracing::info!("Add candidate block state to cache: {:?}", candidate_block.data().identifier());
        self.unprocessed_blocks_cache.guarded_mut(|e| {
            e.push(block_state.clone());
        });

        // TODO: move send block to validation service to block processing (send when it is necessary)
        self.validation_service.send(block_state.clone());
        Ok(())
    }

    fn resend_finalized(
        &self,
        destination_node_id: NodeIdentifier,
        tail: BlockIdentifier,
        cutoff: BlockSeqNo
    ) -> anyhow::Result<()> {
        const MAX_TO_RESEND: usize = 200;
        let mut cache = vec![];
        let mut cursor = tail;
        loop {
            if cache.len() > MAX_TO_RESEND {
                anyhow::bail!("too far into the history (MAX_TO_RESEND)");
            }
            let (
                Some(block_seq_no),
                Some(parent)
            ) = self.blocks_states.get(&cursor)?
                .guarded(|e| (
                    *e.block_seq_no(),
                    e.parent_block_identifier().clone(),
                ))
            else {
                anyhow::bail!("too far into the history (None state)");
            };
            let Some(block) = self.repository.get_block(&cursor)?
            else {
                anyhow::bail!("too far into the history (None block)");
            };
            cache.push(block);
            if cutoff > block_seq_no {
                return self.resend(destination_node_id, cache);
            }
            cursor = parent;
        }
    }

    fn resend(&self, destination: NodeIdentifier, blocks: Vec<<Self as NodeAssociatedTypes>::CandidateBlock>) -> anyhow::Result<()> {
        for block in blocks.into_iter() {
            self.send_candidate_block(block, destination.clone())?;
        }
        Ok(())
    }

    pub(crate) fn on_incoming_block_request(
        &self,
        from_inclusive: BlockSeqNo,
        to_exclusive: BlockSeqNo,
        node_id: NodeIdentifier,
    ) -> anyhow::Result<()> {
        tracing::trace!("on_incoming_block_request from {node_id}: {:?} - {:?}", from_inclusive, to_exclusive);
        let (last_finalized_block_id, last_finalized_block_seq_no) = self.repository
            .select_thread_last_finalized_block(&self.thread_id)?;
        let unprocessed_blocks_cache = self.unprocessed_blocks_cache.guarded(|e|e.clone());
        if last_finalized_block_seq_no > from_inclusive {
            self.resend_finalized(node_id.clone(), last_finalized_block_id, from_inclusive)?;
        }
        let cached = unprocessed_blocks_cache.into_iter().filter_map(|e| {
            let (block_id, Some(seq_no)) = e.guarded(|x| (
                x.block_identifier().clone(),
                *x.block_seq_no(),
            ))
            else {
                return None;
            };
            if seq_no >= from_inclusive
            && seq_no < to_exclusive    {
            Some(block_id) } else {
                None
            }
        });
        let cached = cached.map(|e| {
            let Some(block) = self.repository.get_block(&e)?
            else {
                anyhow::bail!("too far into the history (None block)");
            };
            Ok(block)
        })
            .collect::<Result<Vec<<Self as NodeAssociatedTypes>::CandidateBlock>,_>>()?;
        self.resend(node_id.clone(), cached)?;
        Ok(())
    }
}
