// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use telemetry_utils::now_ms;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::block_state::tools::connect;
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
            target: "monit",
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
            target: "monit",
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
            m.report_store_block_on_disk(moment.elapsed().as_millis() as u64, &self.thread_id);

            // Save the maximum value of incoming seq_no and report if it increases non-monotonically.
            let incoming_seq_no: u32 = net_block.seq_no.into();

            let last_processed_block_seq_no =
                self.last_processed_block_seq_no.get_or_insert(incoming_seq_no);
            if incoming_seq_no > *last_processed_block_seq_no {
                if incoming_seq_no > *last_processed_block_seq_no + 1 {
                    m.report_missed_blocks(
                        incoming_seq_no - *last_processed_block_seq_no - 1,
                        &self.thread_id,
                    );
                }
                *last_processed_block_seq_no = incoming_seq_no;
            }
        });

        let parent_id = envelope.data().parent();
        let thread_identifier = net_block.thread_id;
        let block_time = envelope.data().time()?;
        let block_round = envelope.data().get_common_section().round;
        let block_height = envelope.data().get_common_section().block_height;
        let parent = self.block_state_repository.get(&parent_id).unwrap();

        // Initialize block state
        block_state.guarded_mut(|state| {
            state.set_stored(&envelope)?;
            state.set_block_seq_no(net_block.seq_no)?;
            state.set_thread_identifier(thread_identifier)?;
            // Guard against setting the value repeatedly on the producer
            if state.producer_selector_data().is_none() {
                state.set_producer_selector_data(producer_selector)?;
            }

            state.set_block_time_ms(block_time)?;
            if let Some(r) = state.block_round() {
                assert!(*r == block_round);
            } else {
                state.set_block_round(block_round)?;
            }
            if let Some(h) = state.block_height() {
                assert!(h == &block_height);
            } else {
                state.set_block_height(block_height)?;
            }
            Ok::<(), anyhow::Error>(())
        })?;

        connect!(parent = parent, child = block_state, &self.block_state_repository);
        if let Some(hint) =
            envelope.data().get_common_section().directives.share_state_resources().clone()
        {
            self.last_synced_state =
                Some((envelope.data().identifier().clone(), envelope.data().seq_no(), hint));
        }

        tracing::info!("Add candidate block state to cache: {:?}", net_block.identifier);
        self.unprocessed_blocks_cache.insert(block_state.clone(), envelope.clone());

        // Update parent block state
        // lock parent only after child lock is dropped
        let parent_is_finalized =
            self.block_state_repository.get(&parent_id)?.guarded_mut(|e| {
                e.add_child(thread_identifier, net_block.identifier.clone())?;
                Ok::<bool, anyhow::Error>(e.is_finalized())
            })?;
        if parent_is_finalized {
            block_state.guarded_mut(|state| state.set_has_parent_finalized())?;
        }

        // Steal block attestations
        for attestation in &envelope.data().get_common_section().block_attestations {
            self.last_block_attestations.guarded_mut(|e| e.add(attestation.clone(), true))?;
        }
        Ok(Some(envelope))
    }
}
