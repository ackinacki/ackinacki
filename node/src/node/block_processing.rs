// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use telemetry_utils::now_ms;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::services::sync::StateSyncService;
use crate::node::NetBlock;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub(crate) fn on_incoming_candidate_block(
        &mut self,
        net_block: &NetBlock,
        resend_source_node_id: Option<NodeIdentifier>,
    ) -> anyhow::Result<Option<<Self as NodeAssociatedTypes>::CandidateBlock>> {
        tracing::info!(
            "Incoming block candidate: {}, resend_source_node_id: {:?}",
            net_block,
            resend_source_node_id
        );
        // Check if we already have this block
        let block_state = self.block_state_repository.get(&net_block.identifier)?;
        block_state
            .guarded_mut(|e| e.try_add_attestations_interest(net_block.producer_id.clone()))?;
        if let Some(ref node_id) = resend_source_node_id {
            block_state.guarded_mut(|e| e.try_add_attestations_interest(node_id.clone()))?;
        };

        if block_state.guarded(|e| e.is_stored()) {
            // We already have this block stored in repo
            tracing::trace!("Block with the same id was already stored in repo");
            // TODO: need to handle a valid situation when BP has not received our attestations and resends the block
            return Ok(None);
        }

        let Some(producer_selector) = net_block.producer_selector.clone() else {
            tracing::trace!("Incoming block doesn't have producer selector, Skip it");
            return Ok(None);
        };

        let envelope = net_block.get_envelope()?;
        tracing::info!(
            "Incoming block candidate: {}, signatures: {:?}, resend_source_node_id: {:?}",
            envelope.data(),
            envelope.clone_signature_occurrences(),
            resend_source_node_id
        );

        block_state.guarded_mut(|state| {
            if state.event_timestamps.received_ms.is_none() {
                state.event_timestamps.received_ms = Some(now_ms());
            }
        });

        let moment = std::time::Instant::now();
        self.metrics.as_ref().inspect(|m| {
            m.report_store_block_on_disk(moment.elapsed().as_millis() as u64, &self.thread_id)
        });

        let parent_id = envelope.data().parent();
        let thread_identifier = net_block.thread_id;
        let block_time = envelope.data().time()?;
        // Initialize block state
        block_state.guarded_mut(|state| {
            state.set_stored()?;
            state.set_block_seq_no(net_block.seq_no)?;
            state.set_thread_identifier(thread_identifier)?;
            // Guard against setting the value repeatedly on the producer
            if state.producer_selector_data().is_none() {
                state.set_producer_selector_data(producer_selector)?;
            }
            state.set_parent_block_identifier(parent_id.clone())?;

            state.set_block_time_ms(block_time)?;
            Ok::<(), anyhow::Error>(())
        })?;

        tracing::info!("Add candidate block state to cache: {:?}", net_block.identifier);
        self.repository.unprocessed_blocks_cache().insert(block_state.clone(), envelope.clone());

        // Update parent block state
        // lock parent only after child lock is dropped
        let siblings = self.block_state_repository.get(&parent_id)?.guarded_mut(|e| {
            e.add_child(thread_identifier, net_block.identifier.clone())?;
            let mut siblings = e.known_children(&thread_identifier).cloned().unwrap_or_default();
            siblings.remove(&net_block.identifier);
            Ok::<_, anyhow::Error>(siblings)
        })?;
        if !siblings.is_empty() {
            self.fork_resolution_service.found_fork(&parent_id)?;
        }

        // Steal block attestations
        for attestation in &envelope.data().get_common_section().block_attestations {
            self.last_block_attestations.guarded_mut(|e| {
                e.add(attestation.clone(), |block_id| {
                    let Ok(block_state) = self.block_state_repository.get(block_id) else {
                        return None;
                    };
                    block_state.guarded(|e| e.bk_set().clone())
                })
            })?;
        }
        Ok(Some(envelope))
    }
}
