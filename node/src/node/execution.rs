// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::ops::Sub;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::ExecutionResult;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::SynchronizationResult;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::attestation_processor::LOOP_PAUSE_DURATION;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::EXT_MESSAGE_STORE_TIMEOUT_SECONDS;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::AckiNackiBlock;
use crate::types::BlockSeqNo;

const ATTESTATIONS_APPLY_BLOCK_SEQ_NO_GAP: u32 = 0;

impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
        <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData>,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
        >,
        TRandomGenerator: rand::Rng,
{
    pub fn execute(&mut self) -> anyhow::Result<()> {
        // Note:
        // This whole thing assumes that there is only one thread.
        // In case of multiple threads this logic will immediately
        // become very complicated thus require refactoring.
        loop {
            let thread_id = self.thread_id;
            // Synchronization can be executed not once if node lose too much blocks.
            let synchronization_result = if self.is_this_node_a_producer_for_new_block(
                &thread_id,
            ) {
                self.execute_restarted_producer()?
            } else {
                self.restart_bk()?;
                self.execute_synchronizing()?
            };
            tracing::trace!("Synchronization finished: {:?}", synchronization_result);
            tracing::trace!("Unprocessed blocks: {:?}", self.unprocessed_blocks_cache);
            let exec_result = match synchronization_result {
                SynchronizationResult::Ok => self.execute_normal(),
                SynchronizationResult::Forward(network_message) => {
                    self.execute_normal_forwarded(Some(network_message))
                }
                SynchronizationResult::Interrupted => {
                    tracing::error!("Synchronization was interrupted. Stop execution.");
                    return Ok(());
                },
            }?;
            match exec_result {
                ExecutionResult::SynchronizationRequired => {
                    self.validation_process.clear_queue()?;
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

    fn execute_normal_forwarded(
        &mut self,
        mut next_message: Option<<Self as NodeAssociatedTypes>::NetworkMessage>,
    ) -> anyhow::Result<ExecutionResult> {
        tracing::trace!("Start execute_normal_forwarded: {next_message:?}");
        let mut is_stop_signal_received = false;
        let mut in_flight_productions = self.start_block_production()?;
        let mut is_state_sync_requested = None;
        let mut share_producer_group = false;
        self.last_sent_attestation = None;

        let mut last_state_sync_executed = std::time::Instant::now();
        if !in_flight_productions.is_empty() {
            // Allow producer sync its state from the very beginning
            last_state_sync_executed = last_state_sync_executed.sub(self.config.global.min_time_between_state_publish_directives);
        }

        let mut iteration_start = std::time::Instant::now();
        let mut clear_ext_messages_timestamp = std::time::Instant::now();
        let mut sync_delay: Option<std::time::Instant> = None;
        let mut last_block_received = std::time::Instant::now();
        while !is_stop_signal_received {
            tracing::info!("Execution iteration start on node for thread: {:?}", self.thread_id);
            tracing::trace!("Elapsed from last producer cut off: {:?}ms", iteration_start.elapsed().as_millis());

            let thread_id = self.thread_id;
            // Save updated blocks from attestation processor
            if BlockStatus::SynchronizationRequired == self.process_blocks_from_attestation_processor()? {
                return Ok(ExecutionResult::SynchronizationRequired);
            }

            // Stop production for all threads this node is not producer for
            if !in_flight_productions.is_empty() {
                for thread_id in self.list_threads()? {
                    if !self.is_this_node_a_producer_for_new_block(&thread_id) {
                        self.production_process.stop_thread_production(&thread_id)?;
                        in_flight_productions.clear();
                        tracing::trace!("clear cache_forward_optimistic");
                        self.cache_forward_optimistic.clear();
                    }
                }
            }

            if let Some(sync_delay_value) = sync_delay.as_ref() {
                let elapsed = sync_delay_value.elapsed().as_millis();
                tracing::trace!("sync is planned. elapsed: {}ms", elapsed);
                if elapsed >= self.config.global.sync_delay_milliseconds {
                    sync_delay = None;
                    let (last_block_id, last_block_seq_no) = self.find_thread_last_block_id_this_node_can_continue(&thread_id)?;
                    let first_missed_block_seq_no = next_seq_no(last_block_seq_no);

                    let first_unprocessed_block_seq_no = self.unprocessed_blocks_cache.first_key_value().map(|(k, _v)| *k).unwrap_or_default();
                    if first_unprocessed_block_seq_no == first_missed_block_seq_no {
                        if let Some(block) = self.take_next_unprocessed_block(last_block_id, last_block_seq_no)? {
                            next_message = Some(NetworkMessage::Candidate(block));
                        }
                    }
                    tracing::trace!("Sync from {first_missed_block_seq_no:?} to {first_unprocessed_block_seq_no:?}");
                    if (first_unprocessed_block_seq_no == BlockSeqNo::default()) ||
                        (first_unprocessed_block_seq_no - first_missed_block_seq_no >
                            self.config.global.need_synchronization_block_diff) {
                        return Ok(ExecutionResult::SynchronizationRequired);
                    } else {
                        self.send_block_request(
                            self.current_block_producer_id(&thread_id, &first_unprocessed_block_seq_no),
                            first_missed_block_seq_no,
                            first_unprocessed_block_seq_no,
                        )?;
                    }
                }
            }
            if !self.is_this_node_a_producer_for_new_block(&thread_id) {
                if  clear_ext_messages_timestamp.elapsed() > Duration::from_secs(EXT_MESSAGE_STORE_TIMEOUT_SECONDS as u64) {
                    self.repository.clear_ext_messages_queue_by_time()?;
                    clear_ext_messages_timestamp = std::time::Instant::now();
                }
                self.send_attestations()?;
                self.send_acks_and_nacks_for_validated_blocks()?;
                let time_from_last_block_receipt = last_block_received.elapsed().as_millis();
                tracing::trace!("time_from_last_block_receipt: {time_from_last_block_receipt} ms");
            } else {
                // Check block keeper epochs. touch epoch if it is finished and 5 seconds after if it was not erased
                self.check_and_touch_block_keeper_epochs(&thread_id)?;
            }
            let recv_timeout = {
                if self.production_timeout_multiplier != 0 {
                    if in_flight_productions.is_empty() && self.blocks_for_resync_broadcasting.is_empty() {
                        if share_producer_group {
                            self.execute_restarted_producer()?;
                        }
                        in_flight_productions = self.start_block_production()?;
                        self.get_production_timeout()
                    } else {
                        self.get_production_timeout().saturating_sub(iteration_start.elapsed())
                    }
                } else {
                    // If production_timeout_multiplier is zero node should not produce new blocks
                    // But it should resend first non finalized block
                    for thread_id in self.list_threads()? {
                        self.production_process.stop_thread_production(&thread_id)?;
                        in_flight_productions.clear();
                    }

                    Duration::from_millis(self.config.global.time_to_produce_block_millis)
                }
            };
            tracing::trace!("recv_timeout: {recv_timeout:?}");

            let next = {
                if next_message.is_some() {
                    Ok(next_message.take().unwrap())
                } else if recv_timeout.is_zero() {
                    Err(RecvTimeoutError::Timeout)
                } else {
                    tracing::trace!("trying to receive messages from other nodes");
                    self.rx.recv_timeout(recv_timeout)
                }
            };
            tracing::trace!("Node message receive result: {:?}", next);
            match next {
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::info!("Disconnect signal received");
                    is_stop_signal_received = true;
                }
                Err(RecvTimeoutError::Timeout) => {
                    if !self.blocks_for_resync_broadcasting.is_empty() {
                        let block = self.blocks_for_resync_broadcasting.pop_front().unwrap();
                        tracing::trace!("send block from blocks_for_resync_broadcasting({})", self.blocks_for_resync_broadcasting.len());
                        self.broadcast_candidate_block(block)?;
                    }
                    if recv_timeout.is_zero() && !in_flight_productions.is_empty() {
                        tracing::info!("Cut off block producer");

                        // Cut off block producer. Send whatever it has
                        if self.on_production_timeout(&mut in_flight_productions, &mut is_state_sync_requested, &mut share_producer_group)? {
                            // If we have produced smth
                            iteration_start = std::time::Instant::now();
                            self.try_finalize_blocks()?;
                        } else {
                            // If producer is late, try to receive network message without blocking
                            if next_message.is_none() {
                                match self.rx.try_recv() {
                                    Ok(message) => {
                                        next_message = Some(message);
                                    },
                                    Err(_) => {
                                        // If producer process is not ready and there is no incoming
                                        // network messages, just wait for a small amount of time
                                        std::thread::sleep(LOOP_PAUSE_DURATION);
                                    }
                                }
                            } else {
                                // If producer process is not ready and there is no incoming
                                // network messages, just wait for a small amount of time
                                std::thread::sleep(LOOP_PAUSE_DURATION);
                            }
                        }
                    }
                    if in_flight_productions.is_empty() && !self.is_this_node_a_producer_for_new_block(&thread_id) {
                        // If this node became a producer share producers group
                        share_producer_group = share_producer_group || self.increase_block_gaps()?;
                    }
                    // If this node is a stopped BP resend the first not finalized block
                    if self.is_this_node_a_producer_for_new_block(&thread_id) && self.production_timeout_multiplier == 0 {
                        if let Some(block) = self.find_thread_earliest_non_finalized_block(&thread_id)? {
                            tracing::trace!("Broadcast first non finalized block: {block}");
                            self.broadcast_candidate_block(block)?;
                        }
                    }
                }
                Ok(msg) => match msg {
                    NetworkMessage::NodeJoining((node_id, _)) => {
                        tracing::info!("Received NodeJoining message({node_id})");
                        if self.is_this_node_a_producer_for_new_block(&thread_id)
                            && last_state_sync_executed.elapsed() > self.config.global.min_time_between_state_publish_directives {
                            last_state_sync_executed = std::time::Instant::now();
                            let (last_finalized_id, last_finalized_seq_no) =
                                self.repository.select_thread_last_finalized_block(&thread_id)?;
                            if self.production_timeout_multiplier == 0 || last_finalized_seq_no == BlockSeqNo::default() {
                                // If node is stopped or this is init of network and some
                                tracing::trace!("BP is stopped or has no finalized blocks, share last finalized state");
                                self.share_finalized_state(last_finalized_seq_no, last_finalized_id)?;
                            } else {
                                // Otherwise share state in one of the next blocks
                                let mut block_seq_no_with_sync = self.repository.last_stored_block_by_seq_no(&thread_id)?;
                                for _i in 0..self.config.global.sync_gap {
                                    block_seq_no_with_sync = next_seq_no(block_seq_no_with_sync);
                                }
                                tracing::trace!("Mark next block to share state: {block_seq_no_with_sync:?}");
                                self.send_sync_from(node_id, block_seq_no_with_sync)?;
                                is_state_sync_requested = Some(block_seq_no_with_sync);
                            }
                        }
                    }
                    NetworkMessage::Candidate(mut candidate_block) => {
                        tracing::info!("Incoming candidate block");
                        last_block_received = std::time::Instant::now();
                        let mut loaded_from_unprocessed = false;
                        loop {
                            let exec_res = self.on_incoming_candidate_block(&candidate_block, loaded_from_unprocessed)?;
                            tracing::trace!("block process status: {exec_res:?}");
                            self.check_cached_acks_and_nacks(&candidate_block)?;
                            let (block_id, block_seq_no) = match exec_res {
                                BlockStatus::Ok => {
                                    sync_delay = None;
                                    (candidate_block.data().identifier(),candidate_block.data().seq_no())
                                }
                                BlockStatus::BlockCantBeApplied => {
                                    if !loaded_from_unprocessed {
                                        if sync_delay.is_none() {
                                            tracing::trace!("Node has received block it can't apply, plan sync");
                                            sync_delay = Some(std::time::Instant::now());
                                        }
                                        if in_flight_productions.is_empty() {
                                            // If this node became a producer share producers group
                                            share_producer_group = share_producer_group || self.increase_block_gaps()?;
                                        }
                                        self.find_thread_last_block_id_this_node_can_continue(&thread_id)?
                                    } else {
                                        break;
                                    }
                                }
                                BlockStatus::Skipped => {
                                    break;
                                }
                                BlockStatus::BadBlock => {
                                    let block_nack = self.generate_nack(candidate_block.data().identifier(),candidate_block.data().seq_no())?;
                                    self.received_nacks.push(block_nack.clone());
                                    self.broadcast_nack(block_nack)?;
                                    self.rotate_producer_group(&thread_id)?;
                                    break;
                                }
                                BlockStatus::SynchronizationRequired => {
                                    return Ok(ExecutionResult::SynchronizationRequired);
                                }
                            };
                            if let Some(block) = self.take_next_unprocessed_block(block_id, block_seq_no)? {
                                loaded_from_unprocessed = true;
                                candidate_block = block;
                            } else {
                                break;
                            }
                        }
                        self.try_finalize_blocks()?;
                    }
                    NetworkMessage::Ack((ack, _)) => {
                        tracing::info!("Ack block: {:?}, signatures: {:?}", ack.data(), ack.clone_signature_occurrences());
                        self.on_ack(&ack)?;
                    }
                    NetworkMessage::Nack((nack, _)) => {
                        tracing::info!("Nack block: {:?}, signatures: {:?}", nack.data(), nack.clone_signature_occurrences());
                        self.on_nack(&nack)?;
                    }
                    NetworkMessage::ExternalMessage(msg) => {
                        let mut ext_messages = vec![msg];

                        loop {
                            match self.rx.try_recv() {
                                Ok(NetworkMessage::ExternalMessage(msg)) => {
                                    ext_messages.push(msg);
                                },
                                Ok(other) => {
                                    next_message = Some(other);
                                    break;
                                },
                                Err(_) => {
                                    break;
                                }
                            }
                        }

                        tracing::info!("Received external messages: {}", ext_messages.len());
                        // TODO: here we get incoming ext_messages one by one and in case of big amount of messages we can spend a lot of time processing them one by one
                        self.repository.add_external_message(ext_messages)?;
                    }
                    NetworkMessage::BlockAttestation((attestation, _)) => {
                        tracing::info!("Received block attestation {attestation:?}");
                        if self.replay_protection_for_incoming_attestations(&attestation)? && self.check_attestation(&attestation)? {
                            self.last_block_attestations.push(attestation.clone());
                            self.attestation_processor.process_block_attestation(attestation);

                            // self.on_incoming_block_attestation(attestation)?;
                            // If producer is stopped check finalization after receiving attestation
                            if self.production_timeout_multiplier == 0 {
                                self.try_finalize_blocks()?;
                            }
                        }
                    }
                    NetworkMessage::BlockRequest((start, end, node_id, _)) => {
                        tracing::info!("Received BlockRequest from {node_id}: [{:?},{:?})", start, end);
                        self.on_incoming_block_request(start, end, node_id)?;
                    }
                    NetworkMessage::SyncFrom((seq_no_from, _)) => {
                        // while normal execution we ignore sync messages
                        log::info!("Received SyncFrom: {:?}", seq_no_from);
                        // // TODO: fix. single thread implementation
                        // let (_last_accepted_id, last_accepted_seq_no) = self.repository.select_thread_last_main_candidate_block(&ThreadIdentifier::new(0))?;
                        // if seq_no_from > last_accepted_seq_no {
                        //     return Ok(ExecutionResult::SynchronizationRequired);
                        // }
                    }
                    NetworkMessage::SyncFinalized((identifier, seq_no, address, _)) => {
                        // TODO: we'd better check that this node is up to date and does not need to sync
                        tracing::info!("Received SyncFinalized: {:?} {:?} {}", seq_no, identifier, address);
                        // self.on_sync_finalized(seq_no, identifier)?;
                    }
                },
            }
        }
        Ok(ExecutionResult::Disconnected)
    }

    pub(crate) fn get_attestation_limit_seq_no(&self) -> anyhow::Result<BlockSeqNo> {
        // TODO: check that this function does not spend much time on checks after loading metadata
        let (_id, last_finalized_seq_no) = self.repository.select_thread_last_finalized_block(
            &self.thread_id
        )?;
        Ok(last_finalized_seq_no.saturating_sub(ATTESTATIONS_APPLY_BLOCK_SEQ_NO_GAP))
    }

    fn replay_protection_for_incoming_attestations(
        &mut self,
        block_attestation: &<Self as NodeAssociatedTypes>::BlockAttestation,
    ) -> anyhow::Result<bool> {
        let limit_seq_no = self.get_attestation_limit_seq_no()?;
        if block_attestation.data().block_seq_no < limit_seq_no {
            tracing::trace!("Replay protection too old attestation {block_attestation:?}");
            return Ok(false);
        }
        let mut signatures: Vec<SignerIndex> = block_attestation.clone_signature_occurrences().keys().cloned().collect();
        signatures.retain(|index| *index != self.config.local.node_id as SignerIndex);
        if signatures.is_empty() {
            tracing::trace!("Replay protection no new signatures {block_attestation:?}");
            return Ok(false);
        }
        {
            let saved_attestations = self.received_attestations.entry(block_attestation.data().block_seq_no).or_default();
            let saved_attestations_for_block_id = saved_attestations.entry(block_attestation.data().block_id.clone()).or_default();
            let mut attestation_has_new_signatures = false;
            for signature in signatures {
                if !saved_attestations_for_block_id.contains(&signature) {
                    saved_attestations_for_block_id.insert(signature);
                    attestation_has_new_signatures = true;
                }
            }
            if !attestation_has_new_signatures {
                tracing::trace!("Replay protection attestation is already processed {block_attestation:?}");
                return Ok(false);
            }
        }

        // Clear saved cache
        while let Some((first_key, _val)) = self.received_attestations.first_key_value() {
            if (*first_key) >= limit_seq_no {
                break;
            }
            self.received_attestations.pop_first();
        }

        Ok(true)
    }

    pub fn try_finalize_blocks(&mut self) -> anyhow::Result<()> {
        tracing::trace!("try_finalize_blocks start");
        #[cfg(feature = "timing")]
        let start = std::time::Instant::now();
        // Finalize all blocks that have at least N main candidate blocks as their
        // children and at least X seconds passed.
        for thread in self.list_threads()? {
            let (_last_main_candidate_id, last_main_candidate_seq_no) =
                self.repository.select_thread_last_main_candidate_block(&thread)?;
            tracing::trace!("try_finalize_blocks last_main_candidate_seq_no={last_main_candidate_seq_no:?}");
            while let Some(candidate_block) =
                self.find_thread_earliest_non_finalized_block(&thread)?
            {
                let block_seq_no = candidate_block.data().seq_no();
                tracing::trace!("try_finalize_blocks block_seq_no={block_seq_no:?}");
                let mut min_seq_no_to_finalize = block_seq_no;
                // Note: Critical! This is no longer valid.
                for _ in 0..self.config.global.require_minimum_blocks_to_finalize {
                    min_seq_no_to_finalize = next_seq_no(min_seq_no_to_finalize);
                }
                if min_seq_no_to_finalize > last_main_candidate_seq_no {
                    break;
                }
                if self.config.global.require_minimum_time_milliseconds_to_finalize > 0 {
                    todo!();
                }
                // TODO: Check if block has all required data.
                self.on_block_finalized(&candidate_block)?;
            }

            if let Some((_block_id, mut block_seq_no)) = self.find_thread_earliest_non_finalized_main_candidate_block_id(&thread)? {
                loop {
                    if !self.check_if_block_should_be_finalized(&thread, &block_seq_no)? {
                        break;
                    }
                    block_seq_no = next_seq_no(block_seq_no);
                }
            }
        }
        // // TODO: temporary hack to let node finalize blocks if it missed block attestations.
        // // This hack won't be needed when we add block request mechanism.
        // // Finalize block that is N blocks behind the last accepted as main candidate.
        // for thread in self.list_threads()? {
        //     let (_last_main_candidate_id, last_main_candidate_seq_no) =
        //         self.repository.select_thread_last_main_candidate_block(&thread)?;
        //     if last_main_candidate_seq_no.into() > (self.config.global.require_minimum_blocks_to_finalize as u64) {
        //         let (_, last_finalized_seq_no) =
        //             self.repository.select_thread_last_finalized_block(&thread)?;
        //         let mut finalize_candidate_seq_no = last_main_candidate_seq_no;
        //         for _ in 0..self.config.global.require_minimum_blocks_to_finalize {
        //             finalize_candidate_seq_no = finalize_candidate_seq_no.prev();
        //         }
        //         if finalize_candidate_seq_no > last_finalized_seq_no {
        //             tracing::trace!(
        //                 "check possible finalized candidate: {finalize_candidate_seq_no:?}"
        //             );
        //             for block_id in self.repository.get_block_id_by_seq_no(&finalize_candidate_seq_no)
        //             {
        //                 if let Some(true) =
        //                     self.repository.is_block_accepted_as_main_candidate(&block_id)?
        //                 {
        //                     self.on_block_finalized(&block_id, &finalize_candidate_seq_no)?;
        //                 }
        //             }
        //         }
        //     }
        // }
        #[cfg(feature = "timing")]
        tracing::trace!("try_finalize_blocks elapsed: {}ms", start.elapsed().as_millis());
        Ok(())
    }
}
