// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use telemetry_utils::mpsc::instrumented_channel;
use tokio::time::Instant;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::helper::block_flow_trace;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::SynchronizationResult;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::repository::cross_thread_ref_repository::CrossThreadRefDataHistory;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::FixedSizeHashSet;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub(crate) fn execute_synchronizing(
        &mut self,
    ) -> anyhow::Result<SynchronizationResult<NetworkMessage>> {
        tracing::trace!("Start synchronization");
        let (synchronization_tx, synchronization_rx) = instrumented_channel(
            self.metrics.clone(),
            crate::helper::metrics::STATE_LOAD_RESULT_CHANNEL,
        );
        let mut initial_state: Option<(BlockIdentifier, BlockSeqNo)> = None;
        let mut initial_state_shared_resource_address: Option<
            <TStateSyncService as StateSyncService>::ResourceAddress,
        > = None;
        let mut last_node_join_message_time = Instant::now();
        // Broadcast NodeJoining only the first start of the node, do not broadcast it on split
        if self.thread_id == ThreadIdentifier::default() {
            self.broadcast_node_joining()?;
        }
        let mut block_request_was_sent = false;
        let mut recieved_sync_from = None;
        let current_thread_id = self.thread_id;

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
            }
            if let Some(ref resource_address) = initial_state_shared_resource_address {
                match synchronization_rx.try_recv() {
                    Ok(Ok((task_resource_address, state_buffer))) => {
                        tracing::trace!(
                            "Consensus received sync: task_resource_address={task_resource_address} resource_address={resource_address}"
                        );
                        if &task_resource_address == resource_address {
                            // Save
                            let (block_id, _seq_no) = initial_state.clone().unwrap();
                            tracing::trace!(
                                "[synchronization] set state from shared resource: {block_id:?}"
                            );
                            let loaded_cross_thread_ref_data =
                                self.repository.set_state_from_snapshot(
                                    &block_id,
                                    <RepositoryImpl as Repository>::StateSnapshot::from(
                                        state_buffer,
                                    ),
                                    &current_thread_id,
                                    self.skipped_attestation_ids.clone(),
                                    self.external_messages.clone(),
                                )?;
                            //
                            self.shared_services.exec(|services| {
                                for ref_data in loaded_cross_thread_ref_data.into_iter() {
                                    services
                                        .cross_thread_ref_data_service
                                        .set_cross_thread_ref_data(ref_data)
                                        .expect("Failed to load cross-thread-ref-data");
                                }
                            });

                            // self.set_producer_groups_from_finalized_state(seq_no, new_groups);

                            initial_state_shared_resource_address = None;

                            if let Some(block) = self.repository.get_block(&block_id)? {
                                tracing::trace!("loaded synced finalized block: {block}");
                                // let mut signs = block.clone_signature_occurrences();
                                // signs.retain(|_k, count| *count > 0);
                                // if signs.len() >= self.min_signatures_count_to_accept_broadcasted_state(seq_no) {
                                self.finalize_synced_block(&block)?;
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
                            }
                            // otherwise wait for the block
                            continue;
                        }
                    }
                    Ok(Err(e)) => {
                        // Note: State download failed.
                        // Nothing can be done at this moment. Should be investigated.
                        panic!("{}", e);
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
                    NetworkMessage::Candidate(ref candidate_block)
                    | NetworkMessage::ResentCandidate((ref candidate_block, _)) => {
                        tracing::info!("[synchronizing] Incoming candidate block");
                        tracing::info!(
                            "[synchronizing] Incoming block candidate: {}, signatures: {:?}",
                            candidate_block.data(),
                            candidate_block.clone_signature_occurrences(),
                        );
                        block_flow_trace(
                            "received candidate [synchronizing]",
                            &candidate_block.data().identifier(),
                            &self.config.local.node_id,
                            [],
                        );
                        let resend_node_id =
                            if let NetworkMessage::ResentCandidate((_, node_id)) = &msg {
                                Some(node_id.clone())
                            } else {
                                None
                            };
                        self.on_incoming_candidate_block(candidate_block, resend_node_id)?;
                        if let Some((block_id, seq_no)) = initial_state.clone() {
                            tracing::info!(
                                "[synchronizing] initial_state block: {}, seqno: {}",
                                block_id.clone(),
                                seq_no.clone(),
                            );
                            if candidate_block.data().seq_no() < seq_no {
                                continue;
                            }
                            // We have received finalized block from stopped BP
                            // It can be older than our last finalized, so process here
                            if block_id == candidate_block.data().identifier() {
                                if self.check_block_signature(candidate_block) != Some(true) {
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
                                    self.repository.store_block(candidate_block.clone())?;
                                    self.finalize_synced_block(candidate_block)?;
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
                            if &candidate_block.data().seq_no() < seq_no {
                                continue;
                            }
                        }

                        // if self.is_candidate_block_can_be_applied(candidate_block)
                        //         .unwrap_or(false)
                        let parent_id = candidate_block.data().parent();
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
                            let incoming_block_seq_no = candidate_block.data().seq_no();
                            let seq_no_diff = incoming_block_seq_no - first_missed_block_seq_no;
                            if seq_no_diff < self.config.global.need_synchronization_block_diff
                                && !block_request_was_sent
                            {
                                block_request_was_sent = true;
                                self.send_block_request(
                                    candidate_block.data().get_common_section().producer_id.clone(),
                                    first_missed_block_seq_no,
                                    incoming_block_seq_no,
                                    None,
                                )?;
                            }
                        }

                        if initial_state_shared_resource_address.is_none() {
                            if let Some(resource_address) =
                                candidate_block.data().directives().share_state_resource_address
                            {
                                tracing::trace!(
                                    "[synchronizing] Incoming block contains directives: {resource_address}"
                                );
                                let resource_address: TStateSyncService::ResourceAddress =
                                    serde_json::from_str(&resource_address)?;
                                initial_state_shared_resource_address =
                                    Some(resource_address.clone());
                                initial_state = Some((
                                    candidate_block.data().identifier(),
                                    candidate_block.data().seq_no(),
                                ));

                                self.state_sync_service.add_load_state_task(
                                    resource_address,
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
                            "[synchronizing] Received SyncFinalized: {:?} {:?} {}",
                            seq_no,
                            identifier,
                            address
                        );
                        if initial_state_shared_resource_address.is_none() {
                            tracing::trace!("[synchronizing] start loading shared state");
                            let resource_address: TStateSyncService::ResourceAddress =
                                serde_json::from_str(&address)?;
                            initial_state_shared_resource_address = Some(resource_address.clone());
                            initial_state = Some((identifier.clone(), seq_no));
                            self.state_sync_service.add_load_state_task(
                                resource_address,
                                synchronization_tx.clone(),
                            )?;
                        }
                        self.on_sync_finalized(seq_no, identifier)?;
                    }
                },
            }
        }
    }

    fn finalize_synced_block(
        &mut self,
        block: &Envelope<GoshBLS, AckiNackiBlock>,
    ) -> anyhow::Result<()> {
        tracing::trace!("[synchronization] Marked synced block as finalized: {:?}", block);
        let block_id = block.data().identifier();
        let seq_no = block.data().seq_no();
        let current_thread_id = self.thread_id;
        // We have already received the last finalized block end sync
        let parent_block_id = block.data().parent();
        self.shared_services.exec(|services| {
            services.threads_tracking.init_thread(
                parent_block_id.clone(),
                HashSet::from_iter(vec![current_thread_id]),
                &mut (&mut services.load_balancing,),
            );
            let threads: Vec<ThreadIdentifier> = {
                if let Some(threads_table) =
                    block.data().get_common_section().threads_table.as_ref()
                {
                    threads_table.list_threads().copied().collect()
                } else {
                    let state = self
                        .repository
                        .get_optimistic_state(
                            &block_id,
                            &current_thread_id,
                            Arc::new(Mutex::new(FixedSizeHashSet::new(0))),
                            None,
                        )
                        .expect("Failed to load finalized state")
                        .expect("Repo must have finalized state on this point");
                    state.get_produced_threads_table().list_threads().copied().collect()
                }
            };
            for thread_identifier in threads {
                services.router.join_thread(thread_identifier);
            }
        });
        self.shared_services.on_block_appended(block.data());
        self.repository.mark_block_as_finalized(
            block,
            Arc::clone(&self.nack_set_cache),
            self.block_state_repository.get(&block_id)?,
        )?;
        self.shared_services.on_block_finalized(
            block.data(),
            &mut self
                .repository
                .get_optimistic_state(
                    &block.data().identifier(),
                    &current_thread_id,
                    Arc::clone(&self.nack_set_cache),
                    None,
                )?
                .expect("set above"),
        );
        // self.clear_unprocessed_till(&seq_no, &current_thread_id)?;
        self.repository.clear_verification_markers(&seq_no, &current_thread_id)?;

        tracing::trace!("[synchronization] clear unprocessed block cache");
        self.unprocessed_blocks_cache.retain(|block_state| {
            block_state.guarded(|e| *e.block_seq_no()).map(|e| e > seq_no).unwrap_or(false)
        });
        Ok(())
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
                    let _block_attestation =
                        <Self as NodeAssociatedTypes>::BlockAttestation::create(
                            block.aggregated_signature().clone(),
                            block.clone_signature_occurrences(),
                            AttestationData::builder()
                                .block_id(block.data().identifier())
                                .block_seq_no(block.data().seq_no())
                                .parent_block_id(block.data().parent())
                                .build(),
                        );
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
            let (finalized_block_stats, bk_set, thread_id) =
                self.block_state_repository.get(&last_finalized_block_id)?.guarded(|e| {
                    (
                        e.block_stats().clone().expect("Must be set"),
                        e.bk_set().clone().expect("Must be set").deref().clone(),
                        (*e.thread_identifier()).expect("Must be set"),
                    )
                });
            let state = self
                .repository
                .get_optimistic_state(
                    &last_finalized_block_id,
                    &thread_id,
                    Arc::clone(&self.nack_set_cache),
                    None,
                )?
                .expect("Failed to load finalized state");
            let cross_thread_ref_data_history =
                self.shared_services.exec(|e| -> anyhow::Result<Vec<CrossThreadRefData>> {
                    e.cross_thread_ref_data_service.get_history_tail(&last_finalized_block_id)
                })?;
            let resource_address = self.state_sync_service.add_share_state_task(
                state,
                cross_thread_ref_data_history,
                finalized_block_stats,
                bk_set,
                &self.message_db,
            )?;
            let resource_address = serde_json::to_string(&resource_address)?;
            self.broadcast_sync_finalized(
                last_finalized_block_id,
                last_finalized_seq_no,
                resource_address,
            )?;
        }
        Ok(())
    }
}
