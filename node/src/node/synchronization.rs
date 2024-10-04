// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::hash::Hash;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use tokio::time::Instant;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
use crate::block::BlockSeqNo;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockFor;
use crate::node::associated_types::BlockIdentifierFor;
use crate::node::associated_types::BlockSeqNoFor;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::SynchronizationResult;
use crate::node::associated_types::ThreadIdentifierFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
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
    pub(crate) fn execute_synchronizing(
        &mut self,
    ) -> anyhow::Result<SynchronizationResult<<Self as NodeAssociatedTypes>::NetworkMessage>> {
        tracing::trace!("Start synchronization");
        let (synchronization_tx, synchronization_rx) = std::sync::mpsc::channel();
        let mut initial_state: Option<(
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
        )> = None;
        let mut initial_state_shared_resource_address: Option<
            <TStateSyncService as StateSyncService>::ResourceAddress,
        > = None;
        let mut last_node_join_message_time = Instant::now();
        self.broadcast_node_joining()?;
        let mut block_request_was_sent = false;
        let mut recived_sync_from = None;
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
                            let (block_id, seq_no) = initial_state.clone().unwrap();
                            tracing::trace!("[synchronization] set state from shared resource: {block_id:?}");
                            let (new_groups, block_keeper_set)  = self.repository.set_state_from_snapshot(
                                &block_id,
                                <TRepository as Repository>::StateSnapshot::from(state_buffer),
                            )?;

                            for (thread_id, producer_group) in new_groups {
                                self.set_producer_groups_from_finalized_state(thread_id, seq_no, producer_group);
                            }
                            {
                                let mut block_keeper_set_ref = self.block_keeper_ring_pubkeys.lock();
                                tracing::trace!("[synchronization] set state from shared state: {:?}", block_keeper_set);
                                *block_keeper_set_ref = block_keeper_set;
                            }
                            initial_state_shared_resource_address = None;

                            if let Some(block) = self.repository.get_block(&block_id)? {
                                tracing::trace!("loaded synced finalized block: {block}");
                                let mut signs = block.clone_signature_occurrences();
                                signs.retain(|_k, count| *count > 0);
                                if signs.len() >= self.min_signatures_count_to_accept_broadcasted_state(seq_no) {
                                    tracing::trace!("Marked synced block as finalized");
                                    // We have already received the last finalized block end sync
                                    self.repository.mark_block_as_accepted_as_main_candidate(&block_id)?;
                                    self.repository.mark_block_as_finalized(&block_id)?;
                                    self.clear_unprocessed_till(&seq_no)?;
                                    self.repository.clear_verification_markers(&seq_no)?;
                                    return match self.take_next_unprocessed_block(block_id, seq_no)? {
                                        Some(next_block) => {
                                            tracing::trace!(
                                                "Next unprocessed block after sync: {:?} {:?}",
                                                next_block.data().seq_no(),
                                                next_block.data().identifier()
                                            );
                                            Ok(SynchronizationResult::Forward(
                                                NetworkMessage::Candidate(next_block),
                                            ))
                                        }
                                        None => {
                                            tracing::trace!(
                                                "Next unprocessed block after sync was not found"
                                            );
                                            Ok(SynchronizationResult::Ok)
                                        }
                                    };
                                } else {
                                    tracing::trace!("Loaded block does not have enough signatures");
                                }
                            } else {
                                tracing::trace!("Synced finalized block was not found");
                            }
                            // otherwise wait for the block
                            continue;
                        }
                    }
                    Ok(Err(_e)) => {
                        unreachable!();
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
            log::info!("[synchronizing] waiting for a network message");
            match self.rx.recv_timeout(Duration::from_millis(self.config.global.time_to_produce_block_millis)) {
                Err(RecvTimeoutError::Disconnected) => {
                    log::info!("Disconnect signal received");
                    return Ok(SynchronizationResult::Interrupted);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Ok(msg) => match msg {
                    NetworkMessage::Candidate(ref candidate_block) => {
                        log::info!("[synchronizing] Incoming candidate block");
                        log::info!(
                            "[synchronizing] Incoming block candidate: {}, signatures: {:?}",
                            candidate_block.data(),
                            candidate_block.clone_signature_occurrences(),
                        );
                        if let Some((block_id, seq_no)) = initial_state.clone() {
                            if candidate_block.data().seq_no() < seq_no {
                                continue;
                            }
                            // We have received finalized block from stopped BP
                            // It can be older than our last finalized, so process here
                            if block_id == candidate_block.data().identifier() {
                                if !self.check_block_signature(candidate_block) {
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
                                    self.repository.mark_block_as_accepted_as_main_candidate(&block_id)?;
                                    self.repository.mark_block_as_finalized(&block_id)?;
                                    self.repository.clear_verification_markers(&seq_no)?;
                                    tracing::trace!(
                                        "[synchronization] Mark sync block as finalized: {:?} {:?}",
                                        seq_no,
                                        block_id
                                    );
                                    // if we have already received state, end synchronization
                                    self.clear_unprocessed_till(&seq_no)?;
                                    return match self.take_next_unprocessed_block(block_id, seq_no)? {
                                        Some(next_block) => {
                                            tracing::trace!(
                                                "Next unprocessed block after sync: {:?} {:?}",
                                                next_block.data().seq_no(),
                                                next_block.data().identifier()
                                            );
                                            Ok(SynchronizationResult::Forward(
                                                NetworkMessage::Candidate(next_block),
                                            ))
                                        }
                                        None => {
                                            tracing::trace!(
                                                "Next unprocessed block after sync was not found"
                                            );
                                            Ok(SynchronizationResult::Ok)
                                        }
                                    };
                                } else {
                                    // Otherwise wait for state
                                    self.add_unprocessed_block(candidate_block.clone())?;
                                }
                            }
                        }
                        if let Some(seq_no) = recived_sync_from.as_ref() {
                            if &candidate_block.data().seq_no() < seq_no {
                                continue;
                            }
                        }

                        if self.is_candidate_block_older_than_the_last_finalized_block(
                            candidate_block,
                        )? {
                            log::info!(
                                "[synchronizing] Incoming candidate block is older than last finalized"
                            );
                            continue;
                        }

                        if !self.check_block_signature(candidate_block) {
                            continue;
                        }

                        // If we have received block that can be applied end synchronization
                        if self.is_candidate_block_can_be_applied(candidate_block)? {
                            tracing::trace!("[synchronizing] Candidate block can be applied");
                            let parent_id = candidate_block.data().parent();
                            if parent_id == BlockIdentifierFor::<TBlockProducerProcess>::default() {
                                return Ok(SynchronizationResult::Forward(msg));
                            }
                            if let Some(block) = self.repository.get_block(&parent_id)? {
                                if self.is_candidate_block_signed_by_this_node(&block)? {
                                    return Ok(SynchronizationResult::Forward(msg));
                                }
                            }
                            tracing::trace!("[synchronizing] Candidate block can be applied but it's parent was not signed by this node");
                            self.add_unprocessed_block(candidate_block.clone())?;
                        } else {
                            if !block_request_was_sent {
                                tracing::trace!("[synchronizing] Candidate block can't be applied check for block request");
                                let (_last_block_id, last_block_seq_no) = self.find_thread_last_block_id_this_node_can_continue(&ThreadIdentifierFor::<TBlockProducerProcess>::default())?;
                                let first_missed_block_seq_no = last_block_seq_no.next();
                                tracing::trace!("[synchronizing] first_missed_block_seq_no: {first_missed_block_seq_no:?}");
                                let incoming_block_seq_no = candidate_block.data().seq_no();
                                let seq_no_diff: u64 = incoming_block_seq_no.into() - first_missed_block_seq_no.into();
                                if seq_no_diff < self.config.global.need_synchronization_block_diff && !block_request_was_sent {
                                    block_request_was_sent = true;
                                    self.send_block_request(
                                        candidate_block.data().get_common_section().producer_id,
                                        first_missed_block_seq_no,
                                        incoming_block_seq_no,
                                    )?;
                                }
                            }

                            if let Some((_, seq_no)) = initial_state.clone() {
                                if candidate_block.data().seq_no() >= seq_no {
                                    self.add_unprocessed_block(candidate_block.clone())?;
                                }
                            } else {
                                // If we don't wait for a specific block, save anyway
                                self.add_unprocessed_block(candidate_block.clone())?;
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
                                initial_state = Some((candidate_block.data().identifier(), candidate_block.data().seq_no()));
                                self.state_sync_service.add_load_state_task(
                                    resource_address,
                                    synchronization_tx.clone(),
                                )?;
                            }
                        }
                    }
                    NetworkMessage::Ack(_ack) => {
                        log::info!("[synchronizing] Received Ack block");
                    }
                    NetworkMessage::Nack(_nack) => {
                        // It seems we have joined in the middle of some big sht happening.
                        // Well. since there's no state we can't join any commitee, ignoring.
                        log::info!("[synchronizing] Received Nack block");
                    }
                    NetworkMessage::NodeJoining(_) => {
                        log::info!("[synchronizing] Received NodeJoining");
                    }
                    NetworkMessage::ExternalMessage(_) => {
                        log::info!("[synchronizing] Received ExternalMessage");
                    }
                    NetworkMessage::BlockAttestation(_) => {
                        log::info!("[synchronizing] Received BlockAttestation");
                    }
                    NetworkMessage::BlockRequest(_) => {
                        log::info!("[synchronizing] Received BlockRequest");
                    }
                    NetworkMessage::SyncFrom(seq_no_from) => {
                        log::info!("[synchronizing] Received SyncFrom: {:?}", seq_no_from);
                        // Clear previous waited
                        initial_state_shared_resource_address = None;
                        initial_state = None;
                        recived_sync_from = Some(seq_no_from);
                    }
                    NetworkMessage::SyncFinalized((identifier, seq_no, address)) => {
                        log::info!("[synchronizing] Received SyncFinalized: {:?} {:?} {}", seq_no, identifier, address);
                        if initial_state_shared_resource_address.is_none() {
                            log::trace!("[synchronizing] start loading shared state");
                            let resource_address: TStateSyncService::ResourceAddress =
                                serde_json::from_str(&address)?;
                            initial_state_shared_resource_address =
                                Some(resource_address.clone());
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

    pub(crate) fn on_sync_finalized(
        &self,
        sync_finalized_seq_no: BlockSeqNoFor<TBlockProducerProcess>,
        sync_finalized_block_id: BlockIdentifierFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        // We have received Sync from (last finalized block) from BP,
        // but this node may have sent attestations, BP did not get, so resend them
        tracing::trace!("on_sync_finalized: {sync_finalized_seq_no:?} {sync_finalized_block_id:?}");
        let mut parent_block_id = sync_finalized_block_id;
        let mut attestation_seq_no = sync_finalized_seq_no.next();
        loop {
            let blocks =  self.repository.get_block_from_repo_or_archive_by_seq_no(&attestation_seq_no)?;
            if blocks.is_empty() {
                break;
            }
            attestation_seq_no = attestation_seq_no.next();
            let mut found_attestation_to_send = false;
            for block in blocks {
                if self.is_candidate_block_signed_by_this_node(&block)? && block.data().parent() == parent_block_id {
                    found_attestation_to_send = true;
                    let block_attestation = <Self as NodeAssociatedTypes>::BlockAttestation::create(
                        block.aggregated_signature().clone(),
                        block.clone_signature_occurrences(),
                        AttestationData {
                            block_id: block.data().identifier(),
                            block_seq_no: block.data().seq_no(),
                        },
                    );
                    tracing::trace!("on_sync_finalized sending attestation: {:?} {:?}",
                        block.data().seq_no(),
                        block.data().identifier(),
                    );
                    self.send_block_attestation(self.current_block_producer_id(&ThreadIdentifierFor::<TBlockProducerProcess>::default(), &block.data().seq_no()), block_attestation)?;
                    parent_block_id = block.data().identifier();
                    std::thread::sleep(Duration::from_millis(self.config.global.time_to_produce_block_millis));
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
        last_finalized_seq_no: BlockSeqNoFor<TBlockProducerProcess>,
        last_finalized_block_id: BlockIdentifierFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        if last_finalized_seq_no.into() != 0 {
            log::trace!(
                "share finalized state: id: {:?}; seq_no: {:?}",
                last_finalized_block_id,
                last_finalized_seq_no
            );
            let block_keeper_set = {
                self.block_keeper_ring_pubkeys.lock().clone()
            };
            let snapshot = self.repository.take_state_snapshot(&last_finalized_block_id, self.get_latest_producer_groups_for_all_threads(), block_keeper_set)?;
            let resource_address =
                self.state_sync_service.add_share_state_task(snapshot.into())?;
            let resource_address = serde_json::to_string(&resource_address)?;
            self.broadcast_sync_finalized(last_finalized_block_id, last_finalized_seq_no, resource_address)?;
        }
        // broadcast blocks from last finalized till the last produced
        let last_processed_block_seq_no = self.repository.last_stored_block_by_seq_no()?;
        let mut start = last_finalized_seq_no;
        tracing::trace!("Add blocks [{:?}, {:?}] to blocks_for_resync_broadcasting", start, last_processed_block_seq_no);
        self.blocks_for_resync_broadcasting.clear();
        while start <= last_processed_block_seq_no {
            for block in self.repository.list_blocks_with_seq_no(&start)? {
                self.blocks_for_resync_broadcasting.push_back(block);
            }
            start = start.next();
        }
        tracing::trace!("Add blocks blocks_for_resync_broadcasting({})", self.blocks_for_resync_broadcasting.len());
        Ok(())
    }
}
