// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use std::time::Instant;

use telemetry_utils::now_ms;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::block_state::tools::connect;
use crate::node::services::sync::StateSyncService;
use crate::node::NetBlock;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::add_to_aggregated_attestations_cache;
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
        let total_started_at = Instant::now();
        let mut step_started_at = Instant::now();
        tracing::debug!(
            target: "monit",
            "Incoming block candidate: {}, resend_source_node_id: {:?}",
            net_block,
            resend_source_node_id
        );
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=initial_log elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        // Check if we already have this block
        step_started_at = Instant::now();
        let block_state = self.block_state_repository.get(&net_block.identifier)?;
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=block_state_repository_get elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        step_started_at = Instant::now();
        block_state
            .guarded_mut(|e| e.try_add_attestations_interest(net_block.producer_id.clone()))?;
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=try_add_producer_attestations_interest elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );
        if let Some(ref node_id) = resend_source_node_id {
            step_started_at = Instant::now();
            block_state.guarded_mut(|e| e.try_add_attestations_interest(node_id.clone()))?;
            tracing::trace!(
                target: "node_execution_detailed",
                "incoming_candidate_block timing: step=try_add_resend_attestations_interest elapsed_ms={} seq_no={:?} block_id={:?}",
                step_started_at.elapsed().as_millis(),
                net_block.seq_no,
                net_block.identifier,
            );
        };

        step_started_at = Instant::now();
        if block_state.guarded(|e| e.is_stored()) {
            let elapsed_ms = step_started_at.elapsed().as_millis();
            // We already have this block stored in repo
            tracing::trace!(
                "Block with the same id was already stored in repo; is_stored_check_elapsed_ms={elapsed_ms} seq_no={:?} block_id={:?}",
                net_block.seq_no,
                net_block.identifier,
            );
            // TODO: need to handle a valid situation when BP has not received our attestations and resends the block
            return Ok(None);
        }
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=is_stored_check elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        // Block deserialization can fail. Silently skip this block
        step_started_at = Instant::now();
        let envelope = match net_block.get_envelope() {
            Ok(envelope) => {
                tracing::trace!(
                    target: "node_execution_detailed",
                    "incoming_candidate_block timing: step=get_envelope elapsed_ms={} seq_no={:?} block_id={:?} envelope_bytes={}",
                    step_started_at.elapsed().as_millis(),
                    net_block.seq_no,
                    net_block.identifier,
                    net_block.envelope_data.len(),
                );
                envelope
            }
            Err(err) => {
                tracing::error!(
                    "Block deserialization {err:?}; elapsed_ms={} seq_no={:?} block_id={:?} envelope_bytes={}",
                    step_started_at.elapsed().as_millis(),
                    net_block.seq_no,
                    net_block.identifier,
                    net_block.envelope_data.len(),
                );
                return Ok(None);
            }
        };

        step_started_at = Instant::now();
        let Some(producer_selector) = net_block.descendant_producer_selector.clone() else {
            tracing::trace!("Incoming block doesn't have producer selector, Skip it");
            return Ok(None);
        };
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=producer_selector_clone elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        step_started_at = Instant::now();
        tracing::debug!(
            target: "monit",
            "Incoming block candidate: {}, signatures: {:?}, resend_source_node_id: {:?}",
            envelope.data(),
            envelope.clone_signature_occurrences(),
            resend_source_node_id
        );
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=full_envelope_log elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        step_started_at = Instant::now();
        block_state.guarded_mut(|state| {
            if state.event_timestamps.received_ms.is_none() {
                state.event_timestamps.received_ms = Some(now_ms());
            }
        });
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=set_received_timestamp elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        step_started_at = Instant::now();
        self.metrics.as_ref().inspect(|m| {
            let moment = Instant::now();
            if let Some(last_instant) = self.last_call_on_incoming_candidate_block {
                m.report_block_processing_jitter(
                    last_instant.elapsed().as_millis() as f64,
                    &self.thread_id,
                );
            }
            self.last_call_on_incoming_candidate_block = Some(moment);
        });
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=report_jitter elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        step_started_at = Instant::now();
        let parent_id = envelope.data().parent();
        let thread_identifier = net_block.thread_id;
        let block_time = envelope.data().time()?;
        let block_round = *envelope.data().common_section().round();
        let block_height = *envelope.data().common_section().block_height();
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=read_envelope_fields elapsed_ms={} seq_no={:?} block_id={:?} parent_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
            parent_id,
        );

        step_started_at = Instant::now();
        let parent = self.block_state_repository.get(&parent_id).unwrap();
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=parent_block_state_repository_get elapsed_ms={} seq_no={:?} block_id={:?} parent_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
            parent_id,
        );

        // Initialize block state
        step_started_at = Instant::now();
        block_state.guarded_mut(|state| {
            state.set_stored(&envelope)?;
            state.set_block_seq_no(net_block.seq_no)?;
            state.set_thread_identifier(thread_identifier)?;
            // Guard against setting the value repeatedly on the producer
            if state.descendant_producer_selector_data().is_none() {
                state.set_descendant_producer_selector_data(producer_selector)?;
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
            #[cfg(feature = "test_verify_all_blocks")]
            if state.must_be_validated() != &Some(true) {
                state.set_must_be_validated()?;
            }
            Ok::<(), anyhow::Error>(())
        })?;
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=initialize_block_state elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        step_started_at = Instant::now();
        connect!(parent = parent, child = block_state, &self.block_state_repository);
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=connect_parent_child elapsed_ms={} seq_no={:?} block_id={:?} parent_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
            parent_id,
        );

        step_started_at = Instant::now();
        if *envelope.data().common_section().directives().share_state_resources() {
            self.last_synced_state = Some((
                envelope.data().identifier(),
                envelope.data().seq_no(),
                *envelope.data().common_section().block_height(),
            ));
        }
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=share_state_resources_check elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        tracing::debug!("Add candidate block state to cache: {:?}", net_block.identifier);
        step_started_at = Instant::now();
        self.unprocessed_blocks_cache.insert(block_state.clone(), envelope.clone());
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=unprocessed_blocks_cache_insert elapsed_ms={} seq_no={:?} block_id={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );

        // Update parent block state
        // lock parent only after child lock is dropped
        step_started_at = Instant::now();
        let parent_is_finalized =
            self.block_state_repository.get(&parent_id)?.guarded_mut(|e| {
                e.add_child(thread_identifier, net_block.identifier)?;
                Ok::<bool, anyhow::Error>(e.is_finalized())
            })?;
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=update_parent_block_state elapsed_ms={} seq_no={:?} block_id={:?} parent_id={:?} parent_is_finalized={}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
            parent_id,
            parent_is_finalized,
        );
        if parent_is_finalized {
            step_started_at = Instant::now();
            block_state.guarded_mut(|state| state.set_has_parent_finalized())?;
            tracing::trace!(
                target: "node_execution_detailed",
                "incoming_candidate_block timing: step=set_has_parent_finalized elapsed_ms={} seq_no={:?} block_id={:?}",
                step_started_at.elapsed().as_millis(),
                net_block.seq_no,
                net_block.identifier,
            );
        }

        step_started_at = Instant::now();
        let candidate_block_height = *envelope.data().common_section().block_height();

        if let Some(last_height) = self.last_processed_block_height {
            match last_height.signed_distance_to(&candidate_block_height) {
                Some(diff) => {
                    let diff_abs = diff.unsigned_abs() as u64;
                    if diff_abs > 1 {
                        self.metrics.as_ref().inspect(|m| {
                            m.report_missed_blocks(diff_abs - 1, &self.thread_id);
                        });
                    }
                }
                None => {
                    tracing::error!("Received block for other thread");
                }
            }
        }
        self.last_processed_block_height = Some(candidate_block_height);
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=update_last_processed_height elapsed_ms={} seq_no={:?} block_id={:?} height={:?}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
            candidate_block_height,
        );

        // Steal block attestations
        step_started_at = Instant::now();
        for attestation in envelope.data().common_section().block_attestations() {
            add_to_aggregated_attestations_cache(
                &self.aggregated_attestations_cache,
                attestation.clone(),
            );
            self.last_block_attestations.guarded_mut(|e| e.add(attestation.clone(), true))?;
        }
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=steal_block_attestations elapsed_ms={} seq_no={:?} block_id={:?} attestations={}",
            step_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
            envelope.data().common_section().block_attestations().len(),
        );
        tracing::trace!(
            target: "node_execution_detailed",
            "incoming_candidate_block timing: step=total elapsed_ms={} seq_no={:?} block_id={:?}",
            total_started_at.elapsed().as_millis(),
            net_block.seq_no,
            net_block.identifier,
        );
        Ok(Some(envelope))
    }
}
