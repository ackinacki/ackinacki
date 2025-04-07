// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;

use telemetry_utils::now_ms;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::services::sync::StateSyncService;
use crate::node::BlockState;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
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
        let block_state = self.block_state_repository.get(&candidate_block.data().identifier())?;
        block_state.guarded_mut(|e| {
            let block_producer = candidate_block.data().get_common_section().producer_id.clone();
            e.try_add_attestations_interest(block_producer)
        })?;
        if let Some(node_id) = resend_source_node_id {
            block_state.guarded_mut(|e| e.try_add_attestations_interest(node_id))?;
        };

        if block_state.guarded(|e| e.is_stored()) {
            // We already have this block stored in repo
            tracing::trace!("Block with the same id was already stored in repo");
            // TODO: need to handle a valid situation when BP has not received our attestations and resends the block
            return Ok(());
        }

        let Some(producer_selector) =
            candidate_block.data().get_common_section().producer_selector.clone()
        else {
            tracing::trace!("Incoming block doesn't have producer selector, Skip ir");
            return Ok(());
        };

        block_state.guarded_mut(|state| {
            if state.event_timestamps.received_ms.is_none() {
                state.event_timestamps.received_ms = Some(now_ms());
            }
        });

        let moment = std::time::Instant::now();
        self.repository.store_block(candidate_block.clone())?;
        self.metrics.as_ref().inspect(|m| {
            m.report_store_block_on_disk(moment.elapsed().as_millis() as u64, &self.thread_id)
        });

        let parent_id = candidate_block.data().parent();
        let thread_identifier = candidate_block.data().get_common_section().thread_id;
        let block_time = candidate_block.data().time()?;
        // Initialize block state
        block_state.guarded_mut(|state| {
            state.set_stored()?;
            state.set_block_seq_no(candidate_block.data().seq_no())?;
            state.set_thread_identifier(thread_identifier)?;
            // Guard against setting the value repeatedly on the producer
            if state.producer_selector_data().is_none() {
                state.set_producer_selector_data(producer_selector)?;
            }
            state.set_parent_block_identifier(parent_id.clone())?;

            state.set_block_time_ms(block_time)?;
            Ok::<(), anyhow::Error>(())
        })?;

        // Update parent block state
        // lock parent only after child lock is dropped
        let siblings = self.block_state_repository.get(&parent_id)?.guarded_mut(|e| {
            e.add_child(thread_identifier, candidate_block.data().identifier())?;
            let mut siblings = e.known_children(&thread_identifier).cloned().unwrap_or_default();
            siblings.remove(&candidate_block.data().identifier());
            Ok::<_, anyhow::Error>(siblings)
        })?;
        if !siblings.is_empty() {
            self.fork_resolution_service.found_fork(&parent_id)?;
        }

        tracing::info!(
            "Add candidate block state to cache: {:?}",
            candidate_block.data().identifier()
        );
        self.unprocessed_blocks_cache.insert(block_state.clone(), candidate_block.clone());

        // Steal block attestations
        for attestation in &candidate_block.data().get_common_section().block_attestations {
            self.last_block_attestations.guarded_mut(|e| {
                e.add(attestation.clone(), |block_id| {
                    let Ok(block_state) = self.block_state_repository.get(block_id) else {
                        return None;
                    };
                    block_state.guarded(|e| e.bk_set().clone())
                })
            })?;
        }
        Ok(())
    }

    // Returns number of blocks sent
    fn resend_finalized(
        &self,
        destination_node_id: NodeIdentifier,
        tail: BlockIdentifier,
        cutoff: BlockSeqNo,
    ) -> anyhow::Result<usize> {
        const MAX_TO_RESEND: usize = 200;
        let mut cache = vec![];
        let mut cursor = tail;
        loop {
            if cache.len() > MAX_TO_RESEND {
                anyhow::bail!("too far into the history (MAX_TO_RESEND)");
            }
            let (Some(block_seq_no), Some(parent)) = self
                .block_state_repository
                .get(&cursor)?
                .guarded(|e| (*e.block_seq_no(), e.parent_block_identifier().clone()))
            else {
                anyhow::bail!("too far into the history (None state)");
            };
            let Some(block) = self.repository.get_block(&cursor)? else {
                anyhow::bail!("too far into the history (None block)");
            };
            cache.push(block);
            if cutoff > block_seq_no {
                return self.resend(destination_node_id, cache);
            }
            cursor = parent;
        }
    }

    // Returns number of blocks sent
    fn resend(
        &self,
        destination: NodeIdentifier,
        blocks: Vec<<Self as NodeAssociatedTypes>::CandidateBlock>,
    ) -> anyhow::Result<usize> {
        self.metrics.as_ref().inspect(|m| m.report_resend(&self.thread_id));
        let n = blocks.len();
        for block in blocks.into_iter() {
            self.send_candidate_block(block, destination.clone())?;
        }
        Ok(n)
    }

    pub(crate) fn on_incoming_block_request(
        &self,
        from_inclusive: BlockSeqNo,
        to_exclusive: BlockSeqNo,
        node_id: NodeIdentifier,
        at_least_n_blocks: Option<usize>,
    ) -> anyhow::Result<()> {
        tracing::trace!(
            "on_incoming_block_request from {node_id}: {:?} - {:?}",
            from_inclusive,
            to_exclusive
        );
        let mut at_least_n_blocks = at_least_n_blocks.unwrap_or_default();
        let (last_finalized_block_id, last_finalized_block_seq_no) =
            self.repository.select_thread_last_finalized_block(&self.thread_id)?;
        #[allow(clippy::mutable_key_type)]
        let unprocessed_blocks_cache = self.unprocessed_blocks_cache.clone_queue();
        if last_finalized_block_seq_no > from_inclusive {
            let count = self.resend_finalized(
                node_id.clone(),
                last_finalized_block_id.clone(),
                from_inclusive,
            )?;
            at_least_n_blocks = at_least_n_blocks.saturating_sub(count);
        }
        #[allow(clippy::mutable_key_type)]
        let mut demanded = HashSet::<BlockState>::new();
        if at_least_n_blocks > 0 {
            let mut generation = vec![last_finalized_block_id];
            for _ in 0..at_least_n_blocks {
                let mut next_generation = vec![];
                for block_id in generation.iter() {
                    let block = self.block_state_repository.get(block_id)?;
                    if block.guarded(|e| e.is_invalidated()) {
                        // Skip
                        continue;
                    }
                    block.guarded(|e| e.known_children(&self.thread_id).cloned()).inspect(
                        |descendants| {
                            next_generation.extend(descendants.iter().cloned());
                        },
                    );
                }
                for block_id in next_generation.iter() {
                    let block = self.block_state_repository.get(block_id)?;
                    if block.guarded(|e| !e.is_invalidated()) {
                        demanded.insert(block);
                    }
                }
                generation = next_generation;
            }
        }
        let cached = unprocessed_blocks_cache.into_iter().filter_map(|(_, (e, _))| {
            if demanded.contains(&e) {
                return Some(e.block_identifier().clone());
            }
            let (block_id, Some(seq_no)) =
                e.guarded(|x| (x.block_identifier().clone(), *x.block_seq_no()))
            else {
                return None;
            };
            if seq_no >= from_inclusive && seq_no < to_exclusive {
                Some(block_id)
            } else {
                None
            }
        });
        let cached = cached
            .map(|e| {
                let Some(block) = self.repository.get_block(&e)? else {
                    anyhow::bail!("too far into the history (None block)");
                };
                Ok(block)
            })
            .collect::<Result<Vec<<Self as NodeAssociatedTypes>::CandidateBlock>, _>>()?;
        self.resend(node_id.clone(), cached)?;
        Ok(())
    }
}
