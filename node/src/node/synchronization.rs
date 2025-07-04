// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::time::Duration;

use telemetry_utils::mpsc::instrumented_channel;
use tokio::time::Instant;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::helper::block_flow_trace;
use crate::node::associated_types::SynchronizationResult;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub(crate) fn execute_synchronizing(
        &mut self,
    ) -> anyhow::Result<SynchronizationResult<NetworkMessage>> {
        tracing::trace!("Start synchronization");
        self.state_sync_service.reset_sync();
        let (synchronization_tx, synchronization_rx) = instrumented_channel(
            self.metrics.clone(),
            crate::helper::metrics::STATE_LOAD_RESULT_CHANNEL,
        );
        let mut initial_state: Option<(BlockIdentifier, BlockSeqNo)> = None;
        let mut initial_state_shared_resource_address: Option<
            HashMap<ThreadIdentifier, BlockIdentifier>,
        > = None;
        let mut last_node_join_message_time = Instant::now();
        // Broadcast NodeJoining only the first start of the node, do not broadcast it on split
        if self.thread_id == ThreadIdentifier::default() {
            self.broadcast_node_joining()?;
        }
        let mut block_request_was_sent = false;
        let mut recieved_sync_from = None;

        loop {
            // We have already synced with some nodes before launching the execution, but we
            // could have not reached the producer and possibly should send
            // NodeJoin again
            if last_node_join_message_time.elapsed() > self.config.global.node_joining_timeout {
                self.broadcast_node_joining()?;
                last_node_join_message_time = Instant::now();
                // If we have to broadcast NodeJoining, we definitely did not get state from previous
                initial_state = None;
                initial_state_shared_resource_address = None;
                self.state_sync_service.reset_sync();
            }
            if let Some(ref resource_address) = initial_state_shared_resource_address {
                match synchronization_rx.try_recv() {
                    Ok(Ok(())) => {
                        let synced_block_id = resource_address
                            .get(&self.thread_id)
                            .cloned()
                            .expect("We sync this thread, it must exist in the resources map");
                        let synced_block_seq_no = self
                            .block_state_repository
                            .get(&synced_block_id)
                            .expect("We have just synced this block")
                            .guarded(|e| *e.block_seq_no())
                            .expect("We have just synced this block");
                        self.unprocessed_blocks_cache.retain(|e| {
                            e.guarded(|e| *e.block_seq_no())
                                .map(|seq_no| seq_no >= synced_block_seq_no)
                                .unwrap_or(false)
                        });
                        tracing::trace!(
                            "Consensus received sync: {synced_block_seq_no} {synced_block_id:?}"
                        );
                        // if &task_resource_address == resource_address {
                        // Save
                        // let (block_id, _seq_no) = initial_state.clone().unwrap();
                        // tracing::trace!(
                        //     "[synchronization] set state from shared resource: {block_id:?}"
                        // );
                        // self.repository.set_state_from_snapshot(
                        //     <RepositoryImpl as Repository>::StateSnapshot::from(state_buffer),
                        //     &current_thread_id,
                        //     self.skipped_attestation_ids.clone(),
                        // )?;

                        // initial_state_shared_resource_address = None;

                        // if let Some(block) = self.repository.get_block(&block_id)? {
                        //     tracing::trace!("loaded synced finalized block: {block}");
                        // let mut signs = block.clone_signature_occurrences();
                        // signs.retain(|_k, count| *count > 0);
                        // if signs.len() >= self.min_signatures_count_to_accept_broadcasted_state(seq_no) {
                        // self.finalize_synced_block(&block)?;
                        // return match self.take_next_unprocessed_block(block_id, seq_no)? {
                        //     Some(next_block) => {
                        //         tracing::trace!(
                        //             "Next unprocessed block after sync: {:?} {:?}",
                        //             next_block.data().seq_no(),
                        //             next_block.data().identifier()
                        //         );
                        //         Ok(SynchronizationResult::Forward(
                        //             NetworkMessage::Candidate(next_block),
                        //         ))
                        //     }
                        //     None => {
                        //         tracing::trace!(
                        //             "Next unprocessed block after sync was not found"
                        //         );
                        return Ok(SynchronizationResult::Ok);
                        // }
                        // };
                        // } else {
                        //     tracing::trace!("Loaded block does not have enough signatures");
                        // }
                        // } else {
                        //     tracing::trace!("Synced finalized block was not found");
                        // }
                        // otherwise wait for the block
                        // continue;
                        // }
                    }
                    Ok(Err(e)) => {
                        initial_state_shared_resource_address = None;
                        // Note: State download failed.
                        // Nothing can be done at this moment. Should be investigated.
                        tracing::error!("Synchronization error: {}", e);
                    }
                    Err(TryRecvError::Disconnected) => {
                        // TODO: we have to fix channel and move it entirely so this event may
                        // happen. Otherwise it is possible in case of the sync service failure to
                        // stay in this loop forever.
                        anyhow::bail!(
                            "Something went wrong. Sync Service failed to load the state."
                        );
                    }
                    Err(TryRecvError::Empty) => {}
                };
            }
            tracing::info!("[synchronizing]({:?}) waiting for a network message", self.thread_id);
            match self.network_rx.recv_timeout(Duration::from_millis(
                self.config.global.time_to_produce_block_millis,
            )) {
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::error!("Disconnect signal received (synchronization)");
                    return Ok(SynchronizationResult::Interrupted);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Ok(msg) => match msg {
                    NetworkMessage::AuthoritySwitchProtocol(_) => {
                        // todo!("monitor authority switch accepts");
                        // Authority switch is processed in execute_normal
                        continue;
                    }
                    NetworkMessage::Candidate(ref net_block)
                    | NetworkMessage::ResentCandidate((ref net_block, _)) => {
                        tracing::info!("[synchronizing] Incoming candidate block");
                        tracing::info!("[synchronizing] Incoming block candidate: {}", net_block,);
                        block_flow_trace(
                            "received candidate [synchronizing]",
                            &net_block.identifier,
                            &self.config.local.node_id,
                            [],
                        );
                        let resend_node_id =
                            if let NetworkMessage::ResentCandidate((_, node_id)) = &msg {
                                Some(node_id.clone())
                            } else {
                                None
                            };
                        let mut envelope =
                            self.on_incoming_candidate_block(net_block, resend_node_id)?;

                        if let Some((block_id, seq_no)) = initial_state.clone() {
                            tracing::info!(
                                "[synchronizing] initial_state block: {}, seqno: {}",
                                block_id.clone(),
                                seq_no.clone(),
                            );
                            if net_block.seq_no < seq_no {
                                continue;
                            }
                            // We have received finalized block from stopped BP
                            // It can be older than our last finalized, so process here
                            if block_id == net_block.identifier {
                                if envelope.is_none() {
                                    envelope = Some(net_block.get_envelope()?);
                                }
                                let envelope = envelope.as_ref().unwrap();
                                if self.check_block_signature(envelope) != Some(true) {
                                    continue;
                                }
                                if initial_state_shared_resource_address.is_none() {
                                    // Here we can have such situation:
                                    // nodes were working well, but suddenly BP lost network,
                                    // or all nodes were turned off. When nodes rise up they can
                                    // have block finalized newer than BP. In that case they will not
                                    // send their attestation to BP again and BP will not finalize block.
                                    // So we set finalized block force with possible downgrade.
                                    // But this should be fixed with attestation resending mechanism.
                                    // self.repository.store_block(candidate_block.clone())?;
                                    // self.finalize_synced_block(candidate_block)?;
                                    // return match self.take_next_unprocessed_block(block_id, seq_no)? {
                                    //     Some(next_block) => {
                                    //         tracing::trace!(
                                    //             "Next unprocessed block after sync: {:?} {:?}",
                                    //             next_block.data().seq_no(),
                                    //             next_block.data().identifier()
                                    //         );
                                    //         Ok(SynchronizationResult::Forward(
                                    //             NetworkMessage::Candidate(next_block),
                                    //         ))
                                    //     }
                                    //     None => {
                                    tracing::trace!(
                                        "Next unprocessed block after sync was not found"
                                    );
                                    return Ok(SynchronizationResult::Ok);
                                    //     }
                                    // };
                                } else {
                                    // Otherwise wait for state
                                    // TODO: fix adding to unprocessed cache
                                    // self.add_unprocessed_block(candidate_block.clone())?;
                                }
                            }
                        }
                        if let Some(seq_no) = recieved_sync_from.as_ref() {
                            if &net_block.seq_no < seq_no {
                                continue;
                            }
                        }

                        if envelope.is_none() {
                            envelope = Some(net_block.get_envelope()?);
                        }
                        let envelope = envelope.as_ref().unwrap();
                        // if self.is_candidate_block_can_be_applied(candidate_block)
                        //         .unwrap_or(false)
                        let parent_id = envelope.data().parent();
                        if self
                            .block_state_repository
                            .get(&parent_id)?
                            .guarded(|e| e.is_block_already_applied())
                        {
                            // If we have received block that can be applied end synchronization
                            tracing::trace!("[synchronizing] Candidate block can be applied");
                            // let parent_id = candidate_block.data().parent();
                            // if parent_id == BlockIdentifier::default() {
                            return Ok(SynchronizationResult::Forward(msg));
                            // }
                            // if let Some(block) = self.repository.get_block(&parent_id)? {
                            //     if self.is_candidate_block_signed_by_this_node(&block)? {
                            //         return Ok(SynchronizationResult::Forward(msg));
                            //     }
                            // }
                            // self.add_unprocessed_block(candidate_block.clone())?;
                        } else if !block_request_was_sent {
                            tracing::trace!("[synchronizing] Candidate block can't be applied check for block request");
                            let (_last_block_id, last_block_seq_no) = {
                                // if self.is_spawned_from_node_sync {
                                (BlockIdentifier::default(), BlockSeqNo::default())
                                // } else {
                                // self.find_thread_last_block_id_this_node_can_continue(
                                //      &self.thread_id
                                //  )?
                                // }
                            };
                            let first_missed_block_seq_no = next_seq_no(last_block_seq_no);
                            tracing::trace!("[synchronizing] first_missed_block_seq_no: {first_missed_block_seq_no:?}");
                            let incoming_block_seq_no = net_block.seq_no;
                            let seq_no_diff = incoming_block_seq_no - first_missed_block_seq_no;
                            if seq_no_diff < self.config.global.need_synchronization_block_diff
                                && !block_request_was_sent
                            {
                                block_request_was_sent = true;
                                self.send_block_request(
                                    net_block.producer_id.clone(),
                                    first_missed_block_seq_no,
                                    incoming_block_seq_no,
                                    None,
                                )?;
                            }
                        }

                        if initial_state_shared_resource_address.is_none() {
                            if let Some(resource_address) =
                                envelope.data().directives().share_state_resources()
                            {
                                last_node_join_message_time = Instant::now();
                                initial_state_shared_resource_address =
                                    Some(resource_address.clone());
                                initial_state =
                                    Some((net_block.identifier.clone(), net_block.seq_no));

                                self.state_sync_service.add_load_state_task(
                                    resource_address.clone(),
                                    self.repository.clone(),
                                    synchronization_tx.clone(),
                                )?;
                            }
                        }
                    }
                    NetworkMessage::Ack(_ack) => {
                        tracing::info!("[synchronizing] Received Ack block");
                    }
                    NetworkMessage::Nack(_nack) => {
                        // It seems we have joined in the middle of some big sht happening.
                        // Well. since there's no state we can't join any commitee, ignoring.
                        tracing::info!("[synchronizing] Received Nack block");
                    }
                    NetworkMessage::NodeJoining(_) => {
                        tracing::info!("[synchronizing] Received NodeJoining");
                    }
                    NetworkMessage::ExternalMessage(_) => {
                        tracing::info!("[synchronizing] Received ExternalMessage");
                    }
                    NetworkMessage::BlockAttestation(_) => {
                        tracing::info!("[synchronizing] Received BlockAttestation");
                    }
                    NetworkMessage::BlockRequest { .. } => {
                        tracing::info!("[synchronizing] Received BlockRequest");
                    }
                    NetworkMessage::SyncFrom((seq_no_from, _)) => {
                        tracing::info!("[synchronizing] Received SyncFrom: {:?}", seq_no_from);
                        // Clear previous waited
                        initial_state_shared_resource_address = None;
                        initial_state = None;
                        recieved_sync_from = Some(seq_no_from);
                    }
                    NetworkMessage::SyncFinalized((identifier, seq_no, address, _)) => {
                        tracing::info!(
                            "[synchronizing] Received SyncFinalized: {:?} {:?} {:?}",
                            seq_no,
                            identifier,
                            address
                        );
                        if initial_state_shared_resource_address.is_none() {
                            tracing::trace!("[synchronizing] start loading shared state");
                            initial_state_shared_resource_address = Some(address.clone());
                            initial_state = Some((identifier.clone(), seq_no));
                            self.state_sync_service.add_load_state_task(
                                address,
                                self.repository.clone(),
                                synchronization_tx.clone(),
                            )?;
                        }
                        self.on_sync_finalized(seq_no, identifier)?;
                    }
                },
            }
        }
    }

    pub(crate) fn on_sync_finalized(
        &self,
        sync_finalized_seq_no: BlockSeqNo,
        sync_finalized_block_id: BlockIdentifier,
    ) -> anyhow::Result<()> {
        // We have received Sync from (last finalized block) from BP,
        // but this node may have sent attestations, BP did not get, so resend them
        tracing::trace!("on_sync_finalized: {sync_finalized_seq_no:?} {sync_finalized_block_id:?}");
        let mut parent_block_id = sync_finalized_block_id;
        let mut attestation_seq_no = next_seq_no(sync_finalized_seq_no);
        loop {
            let blocks = self
                .repository
                .get_block_from_repo_or_archive_by_seq_no(&attestation_seq_no, &self.thread_id)?;
            if blocks.is_empty() {
                break;
            }
            attestation_seq_no = next_seq_no(attestation_seq_no);
            let mut found_attestation_to_send = false;
            for block in blocks {
                if self.is_candidate_block_signed_by_this_node(&block)?
                    && block.data().parent() == parent_block_id
                {
                    found_attestation_to_send = true;
                    // let envelope_hash = crate::types::ackinacki_block::envelope_hash::envelope_hash(block)
                    // let _block_attestation =
                    // <Self as NodeAssociatedTypes>::BlockAttestation::create(
                    // block.aggregated_signature().clone(),
                    // block.clone_signature_occurrences(),
                    // AttestationData::builder()
                    // .block_id(block.data().identifier())
                    // .block_seq_no(block.data().seq_no())
                    // .parent_block_id(block.data().parent())
                    // .envelope_hash(envelope_hash)
                    // .build(),
                    // );
                    tracing::trace!(
                        "on_sync_finalized sending attestation: {:?} {:?}",
                        block.data().seq_no(),
                        block.data().identifier(),
                    );
                    // self.send_block_attestation(self.current_block_producer_id(&block.data().seq_no()), block_attestation)?;
                    parent_block_id = block.data().identifier();
                    std::thread::sleep(Duration::from_millis(
                        self.config.global.time_to_produce_block_millis,
                    ));
                }
            }
            if !found_attestation_to_send {
                break;
            }
        }

        Ok(())
    }

    pub(crate) fn share_finalized_state(
        &mut self,
        last_finalized_seq_no: BlockSeqNo,
        last_finalized_block_id: BlockIdentifier,
    ) -> anyhow::Result<()> {
        if last_finalized_seq_no != BlockSeqNo::default() {
            tracing::trace!(
                "share finalized state: id: {:?}; seq_no: {:?}",
                last_finalized_block_id,
                last_finalized_seq_no
            );
            let finalized_block = self
                .repository
                .get_finalized_block(&last_finalized_block_id)?
                .ok_or(anyhow::format_err!("missing last finalized block"))?;
            let finalized_state = self
                .repository
                .get_optimistic_state(
                    &last_finalized_block_id,
                    &finalized_block.data().get_common_section().thread_id,
                    None,
                )?
                .ok_or(anyhow::format_err!("Failed to load finalized state"))?;
            let share_state_hint = finalized_state.get_share_stare_refs();
            for (thread_id, block_id) in &share_state_hint {
                if let Some(state) =
                    self.repository.get_full_optimistic_state(block_id, thread_id, None)?
                {
                    self.state_sync_service.save_state_for_sharing(Arc::new(state))?;
                }
            }

            self.broadcast_sync_finalized(
                last_finalized_block_id,
                last_finalized_seq_no,
                share_state_hint,
            )?;
        }
        Ok(())
    }
}
