// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::ops::Sub;
use std::sync::atomic::Ordering;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::helper::block_flow_trace;
use crate::node::associated_types::ExecutionResult;
use crate::node::associated_types::SynchronizationResult;
use crate::node::block_request_service::BlockRequestParams;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::protocol::authority_switch::action_lock::OnNextRoundIncomingRequestResult;
use crate::protocol::authority_switch::network_message::AuthoritySwitch;
use crate::protocol::authority_switch::network_message::NextRoundSuccess;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub const LOOP_PAUSE_DURATION: Duration = Duration::from_millis(10);

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub fn execute(&mut self) -> anyhow::Result<()> {
        // Note:
        // This whole thing assumes that there is only one thread.
        // In case of multiple threads this logic will immediately
        // become very complicated thus require refactoring.
        let mut needs_synchronizing = false;
        loop {
            // Synchronization can be executed not once if node looses too much blocks.
            let synchronization_result = {
                //     // TODO: Check this code
                //     if self.is_spawned_from_node_sync {
                //         // Note: repository is not ready at this stage. Just sync the node thread.
                //         // let result = self.execute_synchronizing();
                //         // self.is_spawned_from_node_sync = false;
                //         // result?
                //     } else {
                //         self.restart_bk()?;
                //         // if let Some((block_id_to_continue, block_seq_no_to_continue)) =
                //         //     self.find_thread_last_block_id_this_node_can_continue()?
                //         // {
                //         //     self.execute_restarted_producer(
                //         //         block_id_to_continue,
                //         //         block_seq_no_to_continue,
                //         //     )?
                //         // } else {
                if needs_synchronizing {
                    self.execute_synchronizing()?
                } else {
                    SynchronizationResult::Ok
                }
                //     }
            };
            tracing::trace!("Synchronization finished: {:?}", synchronization_result);
            let exec_result = match synchronization_result {
                SynchronizationResult::Ok => self.execute_normal(),
                SynchronizationResult::Forward(network_message) => {
                    self.execute_normal_forwarded(Some(network_message))
                }
                SynchronizationResult::Interrupted => {
                    tracing::error!("Synchronization was interrupted. Stop execution.");
                    return Ok(());
                }
            }?;
            match exec_result {
                ExecutionResult::SynchronizationRequired => {
                    needs_synchronizing = true;
                    continue;
                }
                ExecutionResult::Disconnected => {
                    tracing::error!("Node was disconnected. Stop execution.");
                    break;
                }
            }
        }
        Ok(())
    }

    fn execute_normal(&mut self) -> anyhow::Result<ExecutionResult> {
        self.execute_normal_forwarded(None)
    }

    // TODO: CRITICAL: Refactor. We should return from the method since it is in the loop.
    // Refactor to ensure loop does not break. Same applies for a result propogation (use of <?>)
    fn execute_normal_forwarded(
        &mut self,
        mut next_message: Option<NetworkMessage>,
    ) -> anyhow::Result<ExecutionResult> {
        tracing::trace!("Start execute_normal_forwarded: {next_message:?}");
        let mut is_stop_signal_received = false;
        // let mut in_flight_productions = self.start_block_production()?;

        let last_state_sync_executed = Arc::new(parking_lot::Mutex::new(
            std::time::Instant::now()
                .sub(self.config.global.min_time_between_state_publish_directives),
        ));
        // if self.is_this_node_a_producer() {
        // Allow producer sync its state from the very beginning
        //    let mut guard = last_state_sync_executed.lock();
        //    *guard = guard.sub(self.config.global.min_time_between_state_publish_directives);
        //}

        let iteration_start = std::time::Instant::now();
        // let mut clear_ext_messages_timestamp = std::time::Instant::now();
        let sync_delay: Option<std::time::Instant> = None;
        // let mut memento = None;
        while !is_stop_signal_received {
            tracing::info!("Execution iteration start on node for thread: {:?}", self.thread_id);
            tracing::trace!(
                "Elapsed from last producer cut off: {:?}ms",
                iteration_start.elapsed().as_millis()
            );

            if self.stop_rx.try_recv().is_ok() {
                self.repository
                    .dump_unfinalized_blocks(self.unprocessed_blocks_cache.clone_queue());
                tracing::trace!("Stop execution");
                return Ok(ExecutionResult::Disconnected);
            }
            let thread_id = self.thread_id;

            if let Some(sync_delay_value) = sync_delay.as_ref() {
                let elapsed = sync_delay_value.elapsed().as_millis();
                tracing::trace!("sync is planned. elapsed: {}ms", elapsed);
                if elapsed >= self.config.global.sync_delay_milliseconds {
                    // TODO: revert this code with fixes
                    // sync_delay = None;
                    // let (last_block_id, last_block_seq_no) = self.find_thread_last_block_id_this_node_can_continue(&thread_id)?;
                    // let first_missed_block_seq_no = next_seq_no(last_block_seq_no);

                    // let first_unprocessed_block_seq_no = self.unprocessed_blocks_cache.first_key_value().map(|(k, _v)| *k).unwrap_or_default();
                    // if first_unprocessed_block_seq_no == first_missed_block_seq_no {
                    //     if let Some(block) = self.take_next_unprocessed_block(last_block_id, last_block_seq_no)? {
                    //         next_message = Some(NetworkMessage::Candidate(block));
                    //     }
                    // }
                    // tracing::trace!("Sync from {first_missed_block_seq_no:?} to {first_unprocessed_block_seq_no:?}");
                    // if (first_unprocessed_block_seq_no == BlockSeqNo::default()) ||
                    //     (first_unprocessed_block_seq_no - first_missed_block_seq_no >
                    //         self.config.global.need_synchronization_block_diff) {
                    return Ok(ExecutionResult::SynchronizationRequired);
                    // } else {
                    //     self.send_block_request(
                    //         self.current_block_producer_id(&thread_id, &first_unprocessed_block_seq_no),
                    //         first_missed_block_seq_no,
                    //         first_unprocessed_block_seq_no,
                    //     )?;
                    // }
                }
            }
            //     if  clear_ext_messages_timestamp.elapsed() > Duration::from_secs(EXT_MESSAGE_STORE_TIMEOUT_SECONDS as u64) {
            //         self.repository.clear_ext_messages_queue_by_time(&self.thread_id)?;
            //         clear_ext_messages_timestamp = std::time::Instant::now();
            //     }

            let recv_timeout =
                Duration::from_millis(self.config.global.time_to_produce_block_millis);
            tracing::trace!("recv_timeout: {recv_timeout:?}");

            let next = {
                if next_message.is_some() {
                    Ok(next_message.take().unwrap())
                } else if recv_timeout.is_zero() {
                    Err(RecvTimeoutError::Timeout)
                } else {
                    tracing::trace!("trying to receive messages from other nodes");

                    self.network_rx.recv_timeout(recv_timeout)
                }
            };
            tracing::trace!("Node message receive result: {:?}", next);
            if let Some(db_service) = self.db_service.as_ref() {
                db_service.check();
            }
            match next {
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::info!("Disconnect signal received");
                    is_stop_signal_received = true;
                }
                Err(RecvTimeoutError::Timeout) => {
                    tracing::info!("Recv timeout");
                    self.producer_service.touch();
                }
                Ok(msg) => match msg {
                    NetworkMessage::AuthoritySwitchProtocol(auth_switch) => match auth_switch {
                        AuthoritySwitch::Request(next_round) => {
                            tracing::trace!("Received NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Request: {next_round:?})");
                            if let Some(attestation) = next_round.locked_block_attestation().clone()
                            {
                                self.last_block_attestations.guarded_mut(|e| {
                                    e.add(
                                        attestation,
                                        |block_id| {
                                            let Ok(block_state) =
                                                self.block_state_repository.get(block_id)
                                            else {
                                                return None;
                                            };
                                            block_state.guarded(|e| e.bk_set().clone())
                                        },
                                        |block_id| {
                                            let Ok(block_state) =
                                                self.block_state_repository.get(block_id)
                                            else {
                                                return None;
                                            };
                                            block_state.guarded(|e| e.envelope_hash().clone())
                                        },
                                    )
                                })?;
                            }
                            let unprocessed_cache = self.unprocessed_blocks_cache.clone();
                            let action = self.authority_state.guarded_mut(|e| {
                                e.on_next_round_incoming_request(next_round, unprocessed_cache)
                            });
                            match action {
                                OnNextRoundIncomingRequestResult::StartingBlockProducer => {}
                                OnNextRoundIncomingRequestResult::DoNothing => {}
                                OnNextRoundIncomingRequestResult::Broadcast(e) => {
                                    tracing::trace!("Broadcast AuthoritySwitch::Switched({e:?})");
                                    let _ = self.network_broadcast_tx.send(
                                        NetworkMessage::AuthoritySwitchProtocol(
                                            AuthoritySwitch::Switched(e.clone()),
                                        ),
                                    )?;
                                    self.on_authority_switch_success(e)?;
                                }
                                OnNextRoundIncomingRequestResult::Reject((e, source)) => {
                                    self.network_direct_tx.send((
                                        source,
                                        NetworkMessage::AuthoritySwitchProtocol(
                                            AuthoritySwitch::Reject(e),
                                        ),
                                    ))?
                                }
                                OnNextRoundIncomingRequestResult::RejectTooOldRequest(source) => {
                                    self.network_direct_tx.send((
                                        source,
                                        NetworkMessage::AuthoritySwitchProtocol(
                                            AuthoritySwitch::RejectTooOld(self.thread_id),
                                        ),
                                    ))?
                                }
                            }
                        }
                        AuthoritySwitch::Switched(next_round_success) => {
                            //
                            let Ok(proposed_block) =
                                next_round_success.data().proposed_block().get_envelope()
                            else {
                                tracing::trace!("Failed to parse a proposed block");
                                continue;
                            };
                            let parent_block_identifier = proposed_block.data().parent();
                            let parent_block_state =
                                self.block_state_repository.get(&parent_block_identifier).unwrap();
                            let Some(bk_set) =
                                parent_block_state.guarded(|e| e.descendant_bk_set().clone())
                            else {
                                tracing::trace!("Failed to get descendant bk_set");
                                continue;
                            };
                            match next_round_success
                                .verify_signatures(bk_set.get_pubkeys_by_signers())
                            {
                                Ok(false) => {
                                    tracing::trace!("Invalid signature");
                                    continue;
                                }
                                Err(e) => {
                                    tracing::trace!("Signature verification failed: {e:?}");
                                    continue;
                                }
                                Ok(true) => {
                                    // Consistency check
                                    let is_correct = {
                                        let signature_occurrences =
                                            next_round_success.clone_signature_occurrences();

                                        let Some(expected_signer_index) = bk_set
                                            .get_by_node_id(
                                                next_round_success.data().node_identifier(),
                                            )
                                            .map(|e| e.signer_index)
                                        else {
                                            tracing::trace!(
                                                "Incorrect or malicious message: incorrect node id"
                                            );
                                            continue;
                                        };
                                        signature_occurrences.len() == 1
                                            && signature_occurrences.keys().cloned().last()
                                                == Some(expected_signer_index)
                                    };
                                    if !is_correct {
                                        tracing::trace!("Incorrect or malicious message");
                                        continue;
                                    }
                                }
                            }
                            self.on_authority_switch_success(next_round_success)?;
                        }
                        AuthoritySwitch::Reject(e) => {
                            let net_block = e.prefinalized_block();
                            self.on_incoming_candidate_block(net_block, None)?;
                            let attestations = e.proof_of_prefinalization().clone();
                            self.last_block_attestations.guarded_mut(|e| {
                                e.add(
                                    attestations,
                                    |block_id| {
                                        let Ok(block_state) =
                                            self.block_state_repository.get(block_id)
                                        else {
                                            return None;
                                        };
                                        block_state.guarded(|e| e.bk_set().clone())
                                    },
                                    |block_id| {
                                        let Ok(block_state) =
                                            self.block_state_repository.get(block_id)
                                        else {
                                            return None;
                                        };
                                        block_state.guarded(|e| e.envelope_hash().clone())
                                    },
                                )
                            })?;
                        }
                        AuthoritySwitch::RejectTooOld(_) => {
                            return Ok(ExecutionResult::SynchronizationRequired);
                        }
                        AuthoritySwitch::Failed(e) => {
                            let net_block = e.data().proposed_block();
                            self.on_incoming_candidate_block(net_block, None)?;
                            if let Some(attestations) = e.data().attestations_aggregated().clone() {
                                self.last_block_attestations.guarded_mut(|e| {
                                    e.add(
                                        attestations,
                                        |block_id| {
                                            let Ok(block_state) =
                                                self.block_state_repository.get(block_id)
                                            else {
                                                return None;
                                            };
                                            block_state.guarded(|e| e.bk_set().clone())
                                        },
                                        |block_id| {
                                            let Ok(block_state) =
                                                self.block_state_repository.get(block_id)
                                            else {
                                                return None;
                                            };
                                            block_state.guarded(|e| e.envelope_hash().clone())
                                        },
                                    )
                                })?;
                            }
                        }
                    },
                    NetworkMessage::NodeJoining((node_id, _)) => {
                        tracing::info!("Received NodeJoining message({node_id})");

                        let elapsed = last_state_sync_executed.guarded(|e| e.elapsed());
                        tracing::trace!(
                            "Elapsed from the last state sync: {}ms",
                            elapsed.as_millis()
                        );
                        if elapsed > self.config.global.min_time_between_state_publish_directives {
                            {
                                let mut guard = last_state_sync_executed.lock();
                                *guard = std::time::Instant::now();
                            }

                            let (last_finalized_id, last_finalized_seq_no) = self
                                .repository
                                .select_thread_last_finalized_block(&thread_id)?
                                .expect("Must be known here");
                            if self.production_timeout_multiplier == 0
                                || last_finalized_seq_no == BlockSeqNo::default()
                            {
                                // If node is stopped or this is init of network and some
                                tracing::trace!("BP is stopped or has no finalized blocks, share last finalized state");
                                self.share_finalized_state(
                                    last_finalized_seq_no,
                                    last_finalized_id,
                                )?;
                            } else {
                                // Otherwise share state in one of the next blocks if we have not marked one block yet
                                if self.is_state_sync_requested.guarded(|e| e.is_none()) {
                                    // TODO check that we need to take finalized block, not stored
                                    let (_, mut block_seq_no_with_sync) = self
                                        .repository
                                        .select_thread_last_finalized_block(&thread_id)?
                                        .expect("Must be known here");
                                    for _i in 0..self.config.global.sync_gap {
                                        block_seq_no_with_sync =
                                            next_seq_no(block_seq_no_with_sync);
                                    }
                                    tracing::trace!("Mark next block to share state: {block_seq_no_with_sync:?}");
                                    self.send_sync_from(node_id, block_seq_no_with_sync)?;
                                    self.is_state_sync_requested
                                        .guarded_mut(|e| *e = Some(block_seq_no_with_sync));
                                }
                            }
                        } else {
                            tracing::trace!("Ignore NodeJoining message");
                        }
                    }

                    NetworkMessage::ResentCandidate((ref net_block, _))
                    | NetworkMessage::Candidate(ref net_block) => {
                        tracing::info!("Incoming candidate block");
                        let resend_node_id =
                            if let NetworkMessage::ResentCandidate((_, node_id)) = msg {
                                Some(node_id)
                            } else {
                                None
                            };
                        block_flow_trace(
                            "received candidate",
                            &net_block.identifier,
                            &self.config.local.node_id,
                            [],
                        );
                        self.on_incoming_candidate_block(net_block, resend_node_id)?;
                    }
                    NetworkMessage::Ack((ack, _)) => {
                        tracing::info!(
                            "Ack block: {:?}, signatures: {:?}",
                            ack.data(),
                            ack.clone_signature_occurrences()
                        );
                        self.on_ack(&ack)?;
                    }
                    NetworkMessage::Nack((nack, _)) => {
                        tracing::info!(
                            "Nack block: {:?}, signatures: {:?}",
                            nack.data(),
                            nack.clone_signature_occurrences()
                        );
                        self.on_nack(&nack)?;
                    }
                    NetworkMessage::ExternalMessage((msg, _)) => {
                        let mut ext_messages = vec![msg];

                        loop {
                            match self.network_rx.try_recv() {
                                Ok(NetworkMessage::ExternalMessage((msg, _))) => {
                                    ext_messages.push(msg);
                                }
                                Ok(other) => {
                                    next_message = Some(other);
                                    break;
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }

                        // Filter not external inbound messages
                        ext_messages.retain(|msg| msg.message.ext_in_header().is_some());
                        if !ext_messages.is_empty() {
                            tracing::info!("Received external messages: {}", ext_messages.len());
                            // TODO: here we get incoming ext_messages one by one and in case of big amount of messages we can spend a lot of time processing them one by one
                            let _ = self.external_messages.push_external_messages(&ext_messages);
                        }
                    }
                    NetworkMessage::BlockAttestation((attestation, _)) => {
                        tracing::info!(
                            "Received block attestation for thread {:?} {attestation:?}",
                            self.thread_id
                        );
                        block_flow_trace(
                            "received attestation",
                            attestation.data().block_id(),
                            &self.config.local.node_id,
                            [],
                        );
                        let mut is_new = self.last_block_attestations.guarded_mut(|e| {
                            e.add(
                                attestation,
                                |block_id| {
                                    let Ok(block_state) = self.block_state_repository.get(block_id)
                                    else {
                                        return None;
                                    };
                                    block_state.guarded(|e| e.bk_set().clone())
                                },
                                |block_id| {
                                    let Ok(block_state) = self.block_state_repository.get(block_id)
                                    else {
                                        return None;
                                    };
                                    block_state.guarded(|e| e.envelope_hash().clone())
                                },
                            )
                        })?;
                        loop {
                            match self.network_rx.try_recv() {
                                Ok(NetworkMessage::BlockAttestation((attestation, _))) => {
                                    tracing::info!(
                                        "Received block attestation for thread {:?} {attestation:?}",
                                        self.thread_id
                                    );
                                    if self.last_block_attestations.guarded_mut(|e| {
                                        e.add(
                                            attestation,
                                            |block_id| {
                                                let Ok(block_state) =
                                                    self.block_state_repository.get(block_id)
                                                else {
                                                    return None;
                                                };
                                                block_state.guarded(|e| e.bk_set().clone())
                                            },
                                            |block_id| {
                                                let Ok(block_state) =
                                                    self.block_state_repository.get(block_id)
                                                else {
                                                    return None;
                                                };
                                                block_state.guarded(|e| e.envelope_hash().clone())
                                            },
                                        )
                                    })? {
                                        is_new = true;
                                    }
                                }
                                Ok(other) => {
                                    next_message = Some(other);
                                    break;
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                        if is_new {
                            self.block_state_repository.touch();
                        }
                    }
                    NetworkMessage::BlockRequest {
                        inclusive_from: start,
                        exclusive_to: end,
                        requester: node_id,
                        thread_id: _,
                        at_least_n_blocks,
                    } => {
                        tracing::info!(
                            "Received BlockRequest from {node_id}: [{:?},{:?}) + min_n: {:?}",
                            start,
                            end,
                            at_least_n_blocks,
                        );

                        // Note: Isn't it better to throttle right here?

                        let request_params = BlockRequestParams {
                            start,
                            end,
                            node_id,
                            at_least_n_blocks,
                            last_state_sync_executed: last_state_sync_executed.clone(),
                            is_state_sync_requested: self.is_state_sync_requested.clone(),
                            thread_id: self.thread_id,
                        };
                        if self.blk_req_tx.send(request_params).is_err() {
                            tracing::error!("BlockRequestService (receiver) has gone")
                        }
                    }
                    NetworkMessage::SyncFrom((seq_no_from, _)) => {
                        // while normal execution we ignore sync messages
                        log::info!("Received SyncFrom: {seq_no_from:?}");
                        let elapsed = last_state_sync_executed.guarded(|e| e.elapsed());
                        log::info!(
                            "Received SyncFrom: elapsed from last sync ms: {}",
                            elapsed.as_millis()
                        );
                        let blocks_were_requested = self
                            .block_processor_service
                            .missing_blocks_were_requested
                            .load(Ordering::Relaxed);
                        log::info!(
                            "Received SyncFrom: blocks_were_requested={blocks_were_requested:?}"
                        );
                        if elapsed > self.config.global.min_time_between_state_publish_directives
                            && blocks_were_requested
                        {
                            return Ok(ExecutionResult::SynchronizationRequired);
                        }
                    }
                    NetworkMessage::SyncFinalized((identifier, seq_no, address, _)) => {
                        // TODO: we'd better check that this node is up to date and does not need to sync
                        tracing::info!(
                            "Received SyncFinalized: {:?} {:?} {:?}",
                            seq_no,
                            identifier,
                            address
                        );
                        self.on_sync_finalized(seq_no, identifier)?;
                    }
                },
            }
        }
        Ok(ExecutionResult::Disconnected)
    }

    fn on_authority_switch_success(
        &mut self,
        next_round_success: Envelope<GoshBLS, NextRoundSuccess>,
    ) -> anyhow::Result<()> {
        tracing::trace!("Received NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Switched({next_round_success:?}))");
        // TODO: check signatures.
        let next_round_success = next_round_success.data();
        let proposed_block = next_round_success.proposed_block().clone();
        let resend_node_id = Some(next_round_success.node_identifier().clone());
        self.block_state_repository
            .get(&proposed_block.identifier)
            .unwrap()
            .guarded_mut(|e| e.add_proposed_in_round(*next_round_success.round()))?;
        self.on_incoming_candidate_block(&proposed_block, resend_node_id)?;
        let attestation = next_round_success.attestations_aggregated().clone();
        if let Some(attestation) = attestation.clone() {
            let block_state = self.block_state_repository.get(attestation.data().block_id())?;
            let Some(attestation_target) =
                block_state.guarded(|e| *e.initial_attestations_target())
            else {
                tracing::trace!("Attestation target is not set for switched block {attestation:?}");
                return Ok(());
            };
            if attestation.clone_signature_occurrences().len()
                >= attestation_target.main_attestations_target
            {
                block_state.guarded_mut(|e| {
                    e.set_prefinalized()?;
                    e.set_prefinalization_proof(attestation)
                })?;
            }
        }
        let is_new = self.last_block_attestations.guarded_mut(|e| {
            if let Some(attestation) = attestation {
                e.add(
                    attestation,
                    |block_id| {
                        let Ok(block_state) = self.block_state_repository.get(block_id) else {
                            return None;
                        };
                        block_state.guarded(|e| e.bk_set().clone())
                    },
                    |block_id| {
                        let Ok(block_state) = self.block_state_repository.get(block_id) else {
                            return None;
                        };
                        block_state.guarded(|e| e.envelope_hash().clone())
                    },
                )
            } else {
                Ok(false)
            }
        })?;
        self.authority_state.guarded_mut(|e| e.on_next_round_success(next_round_success));
        if is_new {
            self.block_state_repository.touch();
        }
        Ok(())
    }
}
