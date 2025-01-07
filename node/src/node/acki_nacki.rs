// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use tvm_executor::BlockchainConfig;

use crate::block::keeper::process::verify_block;
use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::NackData;
use crate::node::associated_types::NackReason;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::cross_thread_ref_repository::CrossThreadRefDataRead;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

const BROADCAST_ACK_BLOCK_DIFF: u32 = 10;

impl<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

        >,
        TRepository: Repository<
            BLS = GoshBLS,
            EnvelopeSignerIndex = SignerIndex,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<GoshBLS, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<GoshBLS, AttestationData>,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        >,
        TRandomGenerator: rand::Rng,
{
    // Validation process generated stores block validation results, here node takes them and sends
    // Acks and Nacks.
    pub(crate) fn send_acks_and_nacks_for_validated_blocks(&mut self) -> anyhow::Result<()> {
        tracing::trace!("send_acks_and_nacks_for_validated_blocks");
        // Get results from validation process
        let thread_id = self.thread_id;
        let current_bp_id = self.get_latest_block_producer(&thread_id);
        let verified_blocks = self.validation_process.get_verification_results()?;
        {
            // CRITICAL! This seems very broken. the last state in the validation process
            // may be quite different from the verified state.
            if let Some(last_state) = self.validation_process.get_last_state() {
                tracing::trace!("Share state with keeper process: {:?}", last_state.get_block_id());
                self.production_process.add_state_to_cache(thread_id, last_state);
            }
        }
        for (block_id, block_seq_no, result) in verified_blocks {
            tracing::trace!("block_seq_no: {:?}, result {:?}", block_seq_no, result);
            let candidate_block = self.repository.get_block_from_repo_or_archive(&block_id)?;
            if self.is_this_node_in_block_keeper_set(&block_seq_no, &thread_id) {
                if result {
                    // If validation succeeded send Ack to BP and save it to cache
                    if let Some(block_ack) = self.generate_ack(block_id.clone(), block_seq_no)? {
                        let mut received_acks_in = self.received_acks.lock();
                        received_acks_in.push(block_ack.clone());
                        drop(received_acks_in);
                        self.send_ack(current_bp_id, block_ack.clone())?;
                        self.sent_acks.insert(block_seq_no, block_ack);
                        let block_attestation = <Self as NodeAssociatedTypes>::BlockAttestation::create(
                            candidate_block.aggregated_signature().clone(),
                            candidate_block.clone_signature_occurrences(),
                            AttestationData {
                                block_id: candidate_block.data().identifier(),
                                block_seq_no: candidate_block.data().seq_no(),
                            }
                        );
                        self.sent_attestations
                            .entry(thread_id)
                            .or_default()
                            .push((candidate_block.data().seq_no(), block_attestation.clone()));
                        tracing::trace!("Insert to sent attestations: {:?} {:?}", candidate_block.data().seq_no(), block_attestation.data());
                        self.send_block_attestation(current_bp_id, block_attestation)?;
                        self.repository.dump_sent_attestations(self.sent_attestations.clone())?;
                    }
                } else {
                    if let Some(block_nack)  = self.generate_nack(block_id, block_seq_no, NackReason::BadBlock{envelope: candidate_block.clone()})? {
                        let mut received_nacks_in = self.received_nacks.lock();
                        received_nacks_in.push(block_nack.clone());
                        drop(received_nacks_in);
                        self.broadcast_nack(block_nack)?;
                    }
                    let nack_target_node_id = candidate_block.data().get_common_section().producer_id;
                    if current_bp_id ==  nack_target_node_id {
                        tracing::trace!("Rotate bp_id {:?}", nack_target_node_id);
                        self.rotate_producer_group(&thread_id)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn generate_ack(
        &self,
        block_id: BlockIdentifier,
        block_seq_no: BlockSeqNo
    ) -> anyhow::Result<Option<<Self as NodeAssociatedTypes>::Ack>> {
        if let Some(signer_index) = self.get_node_signer_index_for_block_seq_no(&block_seq_no) {
            let ack_data = AckData { block_id, block_seq_no };
            let signature =
                <GoshBLS as BLSSignatureScheme>::sign(&self.secret, &ack_data)?;
            let mut signature_occurrences = HashMap::new();
            signature_occurrences.insert(signer_index, 1);

            Ok(Some(<Self as NodeAssociatedTypes>::Ack::create(
                signature,
                signature_occurrences,
                ack_data,
            )))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn generate_nack(
        &self,
        block_id: BlockIdentifier,
        block_seq_no: BlockSeqNo,
        reason:  NackReason,
    ) -> anyhow::Result<Option<<Self as NodeAssociatedTypes>::Nack>> {
        if let Some(signer_index) = self.get_node_signer_index_for_block_seq_no(&block_seq_no) {
            let nack_data = NackData { block_id, block_seq_no, reason };
            let signature =
                <GoshBLS as BLSSignatureScheme>::sign(&self.secret, &nack_data)?;
            let mut signature_occurrences = HashMap::new();
            signature_occurrences.insert(signer_index, 1);

            Ok(Some(<Self as NodeAssociatedTypes>::Nack::create(
                signature,
                signature_occurrences,
                nack_data,
            )))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn parse_block_acks_and_nacks(
        &mut self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<BlockStatus> {
        tracing::trace!("parse_block_acks_and_nacks");
        tracing::trace!("sent_acks len: {}", self.sent_acks.len());
        let received_acks = &candidate_block.data().get_common_section().acks;
        tracing::trace!("received_acks len: {}", received_acks.len());
        let keys_ring =
            self.block_keeper_ring_signatures_map_for(&candidate_block.data().seq_no(), &self.get_block_thread_id(candidate_block)?);

        // clear received acks from sent acks
        for ack in received_acks {
            if !ack.verify_signatures(&keys_ring)? {
                return Ok(BlockStatus::BadBlock);
            }
            let sigs = ack.clone_signature_occurrences();
            if let Some(signer_index) = self.get_node_signer_index_for_block_seq_no(&ack.data().block_seq_no) {
                if sigs.contains_key(&signer_index) {
                    tracing::trace!("remove ack from cache: {:?}", ack.data());
                    self.sent_acks.remove(&ack.data().block_seq_no);
                }
            }
        }
        tracing::trace!("sent_acks after clear len: {}", self.sent_acks.len());

        // If Ack was sent long ago enough and was not added to block, broadcast it and remove from cache
        let keys: Vec<BlockSeqNo> = self.sent_acks.keys().copied().collect();
        for seq_no in keys {
            if seq_no + BROADCAST_ACK_BLOCK_DIFF < candidate_block.data().seq_no() {
                let ack = self.sent_acks.remove(&seq_no).unwrap();
                self.broadcast_ack(ack)?;
            }
        }

        let received_nacks = &candidate_block.data().get_common_section().nacks;
        tracing::trace!("received_nacks len: {}", received_nacks.len());
        for nack in received_nacks {
            if !nack.verify_signatures(&keys_ring)? {
                return Ok(BlockStatus::BadBlock);
            }
            self.on_nack(nack)?;
        }
        Ok(BlockStatus::Ok)
    }

    pub(crate) fn on_ack(&mut self, ack: &<Self as NodeAssociatedTypes>::Ack) -> anyhow::Result<()> {
        tracing::trace!("on_ack {:?}", ack);
        let block_id: &BlockIdentifier = &ack.data().block_id;
        let block_seq_no = ack.data().block_seq_no;
        let block = match self.repository.get_block_from_repo_or_archive(block_id) {
            Err(e) => {
                tracing::trace!("ack can't be processed now, save to cache. Error: {e:?}");
                let acks = self.ack_cache.entry(block_seq_no).or_default();
                acks.push(ack.clone());
                return Ok(());
            }
            Ok(block) => block,
        };
        let block_seq_no = block.data().seq_no();
        let signatures_map = self.block_keeper_ring_signatures_map_for(&block_seq_no, &self.get_block_thread_id(&block)?);
        let is_valid = ack
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        if is_valid {
            let mut received_acks_in = self.received_acks.lock();
            received_acks_in.push(ack.clone());
            drop(received_acks_in);
        }
        Ok(())
    }

    pub(crate) fn on_nack(&mut self, nack: &<Self as NodeAssociatedTypes>::Nack) -> anyhow::Result<()> {
        // TODO: self.repository.mark_block_as_suspicious
        // suspicious blocks must be verified despite the BK status
        // if nack was invalid remove the suspicious marker and resend the first nack to BP
        if let Ok(nack_hash) = nack.data().clone().reason.get_hash_nack() {
            let mut nack_set_cache_in = self.nack_set_cache.lock();
            if !nack_set_cache_in.contains(&nack_hash) {
                nack_set_cache_in.insert(nack_hash.clone());
                drop(nack_set_cache_in);
            } else {
                drop(nack_set_cache_in);
                return Ok(());
            }
        } else {
            return Ok(());
        }
        tracing::trace!("on_nack {:?}", nack);
        let block_id: &BlockIdentifier = &nack.data().block_id;
        let block_seq_no = nack.data().block_seq_no;
        let block = match self.repository.get_block_from_repo_or_archive(block_id) {
            Err(e) => {
                tracing::trace!("nack can't be processed now, save to cache. Error: {e:?}");
                let nacks = self.nack_cache.entry(block_seq_no).or_default();
                nacks.push(nack.clone());
                return Ok(());
            }
            Ok(block) => block,
        };
        let thread_id = &self.get_block_thread_id(&block)?;
        let last_fin_block = self.repository.select_thread_last_finalized_block(thread_id);
        tracing::trace!("on_nack: last finalized block, {:?}", last_fin_block);
        match last_fin_block {
            Ok((_, seq_no)) => {
                if seq_no >= block_seq_no {
                    tracing::warn!("Received nack target block is older than the last finalized block for this thread");
                    return Ok(())
                }
            },
            Err(_) => return Ok(()),
        }
        let signatures_map = self.block_keeper_ring_signatures_map_for(&block_seq_no, thread_id);

        let is_valid_signatute= match nack
            .verify_signatures(&signatures_map) {
            Ok(res) => res,
            Err(e) => {
                tracing::warn!("Failed to check Nack signatures: {e:?}");
                return Ok(());
            },
        };
        if !is_valid_signatute {
            tracing::warn!("Invalid Nack signature");
            return Ok(());
        }

        self.repository.mark_block_as_suspicious(block_id, true)?;

         let Ok(is_valid_nack) = self.is_valid_nack(nack, thread_id, block.clone())
         else {
            tracing::warn!("Failed to validate Nack");
            return Ok(())
        };

        if is_valid_nack {
            tracing::trace!("valid nack {:?}", nack);
            let mut received_nacks_in = self.received_nacks.lock();
            received_nacks_in.push(nack.clone());
            drop(received_nacks_in);
            let common_section = block.data().get_common_section();
            if self.get_latest_block_producer(&common_section.thread_id) == common_section.producer_id {
                self.rotate_producer_group(&common_section.thread_id)?;
            }
        } else {
            tracing::trace!("invalid nack {:?}", nack);
            self.repository.mark_block_as_suspicious(block_id, false)?;
            let block_seq_no = block.data().parent_seq_no();
            let bk_set = self.block_keeper_sets.get_block_keeper_data(&block_seq_no);
            if let Some(signer_index) = self.get_node_signer_index_for_block_seq_no(&block_seq_no) {
                if bk_set.contains_key(&signer_index) {
                    tracing::trace!("Forward nack");
                    let new_nack_data = NackReason::WrongNack { nack_data_envelope: Arc::new(nack.clone()) };
                    let new_nack = self.generate_nack(
                        nack.data().block_id.clone(),
                        nack.data().block_seq_no,
                        new_nack_data
                    )?.expect("Node has already checked that it has valid signer index, must not fail");
                    tracing::trace!("new nack {:?}", new_nack);
                    let mut received_nacks_in = self.received_nacks.lock();
                    received_nacks_in.push(new_nack);
                    drop(received_nacks_in);
                    let current_bp_id = self.get_latest_block_producer(thread_id);
                    self.single_tx.send((current_bp_id, NetworkMessage::Nack((nack.clone(), *thread_id))))?;
                    // TODO: handle situation when Nack was signed by current BP, rotate BP and broadcast nack in this case
                }
            }
        }
        Ok(())
    }

    pub(crate) fn is_valid_nack(&mut self, nack: &<Self as NodeAssociatedTypes>::Nack, thread_id: &ThreadIdentifier, block: Envelope<GoshBLS, AckiNackiBlock>) -> anyhow::Result<bool> {
        match nack.data().clone().reason {
            NackReason::SameHeightBlock{first_envelope, second_envelope} => {
                self.is_valid_same_height_nack(block, first_envelope, second_envelope, thread_id)
            },
            NackReason::BadBlock{envelope} => {
                self.is_valid_bad_block_nack(block, envelope, thread_id)
            },
            NackReason::WrongNack{nack_data_envelope: _} => {
                tracing::trace!("WrongNack nack"); 
                Ok(false)
            }
        }
    }


    // TODO: rework to check that blocks have the same parent id, were produced by the same node and have different hashes
    fn is_valid_same_height_nack(&mut self, block: Envelope<GoshBLS, AckiNackiBlock>, nack_first_envelope: Envelope<GoshBLS, AckiNackiBlock>, nack_second_envelope: Envelope<GoshBLS, AckiNackiBlock>, thread_id: &ThreadIdentifier) -> anyhow::Result<bool> {
        tracing::trace!("SameHeightBlock nack");   
        let nack_target_node_id = nack_first_envelope.data().get_common_section().producer_id;
        let bk_set = self.block_keeper_sets.get_block_keeper_data(&block.data().parent_seq_no());
        let data = match bk_set.get(&(nack_target_node_id as u16)) {
            Some(keeper_data) => keeper_data,
            None => return Ok(false),
        };
        let nack_key = data.pubkey.clone();
        if let Ok(thread1) = &self.get_block_thread_id(&nack_first_envelope) {
            if let Ok(thread2) = &self.get_block_thread_id(&nack_second_envelope) {
                if (thread_id != thread1) || (thread_id != thread2) {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
        let mut sign_producer_map = HashMap::new();
        sign_producer_map.insert(nack_target_node_id as u16, nack_key.clone());
        if !nack_first_envelope
            .verify_signatures(&sign_producer_map)
            .expect("Signatures verification should not crash.") {
            return Ok(false);
        }
        if !nack_second_envelope
            .verify_signatures(&sign_producer_map)
            .expect("Signatures verification should not crash.") {
            return Ok(false);
        }
        if let Ok(res) = nack_first_envelope.data().check_hash() {
            if !res {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
        if let Ok(res) = nack_second_envelope.data().check_hash() {
            if !res {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
        if nack_first_envelope.data().get_hash() == nack_second_envelope.data().get_hash() {
            return Ok(false);
        }
        Ok(true)
    }


    fn is_valid_bad_block_nack(&mut self, block: Envelope<GoshBLS, AckiNackiBlock>, nack_envelope: Envelope<GoshBLS, AckiNackiBlock>, thread_id: &ThreadIdentifier) -> anyhow::Result<bool> {
        tracing::trace!("BadBlock nack");   
        let nack_target_node_id = nack_envelope.data().get_common_section().producer_id;
        let block_nack = block.data().get_common_section().nacks.clone();
        let block_id = block.data().identifier();
        let bk_set = self.block_keeper_sets.get_block_keeper_data(&block.data().parent_seq_no());
        let data = match bk_set.get(&(nack_target_node_id as u16)) {
            Some(keeper_data) => keeper_data,
            None => return Ok(false),
        };
        let nack_key = data.pubkey.clone();
        if let Ok(thread1) = &self.get_block_thread_id(&nack_envelope) {
            if thread_id != thread1 {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
        let mut sign_producer_map = HashMap::new();
        sign_producer_map.insert(nack_target_node_id as u16, nack_key.clone());
        let is_valid_sig_bloc = nack_envelope
            .verify_signatures(&sign_producer_map)
            .expect("Signatures verification should not crash.");
        if (block.data().get_hash() == nack_envelope.data().get_hash()) || (!is_valid_sig_bloc) {
            return Ok(false);
        }
        let blockchain_config_path = &self.config.local.blockchain_config_path;
        let json = std::fs::read_to_string(blockchain_config_path)?;
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(&json)?;
        let config_params = tvm_block_json::parse_config(&map).map_err(|e| {
            anyhow::format_err!(
                "Failed to load config params from file {blockchain_config_path:?}: {e}"
            )
        })?;
        let blockchain_config = BlockchainConfig::with_config(config_params)
            .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))?;
        let zerostate_path = Some(self.config.local.zerostate_path.clone());
        let repository = RepositoryImpl::new(
            PathBuf::from("./data"),
            zerostate_path,
            (self.config.global.finalization_delay_to_stop * 2) as usize,
            self.shared_services.clone(),
            self.block_keeper_sets.clone(),
            Arc::clone(&self.nack_set_cache)
        );
        match repository
            .get_optimistic_state(&block_id, self.block_keeper_sets.clone(), Arc::clone(&self.nack_set_cache))
            .expect("Failed to get optimistic state of the previous block")
        {
            Some(mut state) => {
                let refs = self.shared_services.exec(|service| {
                    let mut refs = vec![];
                    for block_id in &nack_envelope.data().get_common_section().refs {
                        let state = service.cross_thread_ref_data_service
                            .get_cross_thread_ref_data(block_id)
                            .expect("Failed to load ref state");
                        refs.push(state);
                    }
                    refs
                });
                if let Ok(res) = verify_block(
                    nack_envelope.data(),
                    Arc::new(blockchain_config),
                    &mut state,
                    self.config.clone(),
                    refs,
                    self.shared_services.clone(),
                    block_nack,
                    self.block_keeper_sets.clone()
                ) {
                    if res {
                        return Ok(false);
                    }
                }
            },
            None => { return Ok(false); }
        }
        Ok(true)
    }

    pub(crate) fn check_cached_acks_and_nacks(&mut self, last_processed_block: &<Self as NodeAssociatedTypes>::CandidateBlock) -> anyhow::Result<()> {
        // TODO: need to track thread id of the finalized block
        let block_seq_no = last_processed_block.data().seq_no();
        let block_id = last_processed_block.data().identifier();
        let cached_acks = self.ack_cache.get(&block_seq_no).cloned().unwrap_or_default();
        for ack in cached_acks {
            if ack.data().block_id == block_id {
                self.on_ack(&ack)?;
                break;
            }
        }
        let cached_nacks = self.nack_cache.get(&block_seq_no).cloned().unwrap_or_default();
        for nack in cached_nacks {
            if nack.data().block_id == block_id {
                self.on_nack(&nack)?;
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn clear_old_acks_and_nacks(&mut self, finalized_block_seq_no: &BlockSeqNo) -> anyhow::Result<()> {
        let ack_keys: Vec<BlockSeqNo> = self.ack_cache.keys().cloned().collect();
        for key in ack_keys {
            if key <= *finalized_block_seq_no {
                let _ = self.ack_cache.remove(&key);
            }
        }
        let nack_keys: Vec<BlockSeqNo> = self.nack_cache.keys().cloned().collect();
        for key in nack_keys {
            if key <= *finalized_block_seq_no {
                let _ = self.nack_cache.remove(&key);
            }
        }
        let mut received_acks = self.received_acks.lock();
        let mut received_nacks = self.received_nacks.lock();
        received_acks.retain(|ack| ack.data().block_seq_no > *finalized_block_seq_no);
        received_nacks.retain(|nack| nack.data().block_seq_no > *finalized_block_seq_no);
        drop(received_acks);
        drop(received_nacks);
        Ok(())
    }
}
