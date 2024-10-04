// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::hash::Hash;

use serde::Deserialize;
use serde::Serialize;

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
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::ThreadIdentifierFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
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
    pub(crate) fn on_incoming_candidate_block(
        &mut self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
        loaded_from_unprocessed: bool,
    ) -> anyhow::Result<BlockStatus> {
        let start = std::time::Instant::now();
        let block_id = candidate_block.data().identifier();
        log::info!(
            "Incoming block candidate: {}, signatures: {:?}, from_unprocessed: {loaded_from_unprocessed}",
            candidate_block.data(),
            candidate_block.clone_signature_occurrences(),
        );

        let block_seq_no = candidate_block.data().seq_no();

        let is_block_already_verified = self.repository.is_block_verified(&block_id)?;
        if is_block_already_verified {
            tracing::trace!("Block was already verified. Skip it.");
            return Ok(BlockStatus::Skipped);
        }

        if self.is_candidate_block_older_or_equal_to_the_last_finalized_block(candidate_block)? {
            tracing::trace!("Incoming block is older or equal to the last finalized");
            return Ok(BlockStatus::Skipped);
        }

        if !loaded_from_unprocessed {
            // Check Wrapped block hash
            let candidate_block_clone = candidate_block.clone();
            let check_hash_thread = std::thread::Builder::new().name("Check wrapped block hash".to_string()).spawn(move || {
                candidate_block_clone.data().check_hash()
            })?;

            // Note:
            // This function has to be split into 2:
            // One accepts block and writes it into DB with the merge.
            // And sends it to the next step if new information was added.
            // The second step has to accept block seq_no and decide based on
            // the configuration what should be done next.
            // This way it will be easier to control forks and resolve conflicts.
            if !self.check_block_signature(candidate_block) {
                return Ok(BlockStatus::BadBlock);
            }

            tracing::trace!("Start waiting for check hash result");
            let check_wrapped_block_result = check_hash_thread.join()
                .map_err(|e| anyhow::format_err!("Failed to join check hash thread: {e:?}"))?;
            tracing::trace!("check hash result: {check_wrapped_block_result:?}");

            if !check_wrapped_block_result? {
                tracing::trace!("Block hash check failed: {}", candidate_block);
                return Ok(BlockStatus::BadBlock);
            }
        } else {
            tracing::trace!("Block was loaded from cache, skip checks");
        }
        let thread_id = self.get_block_thread_id(candidate_block)?;

        // Check if this node has already signed block of the same height
        if self.does_this_node_have_signed_block_of_the_same_height(candidate_block)? {
            tracing::trace!("Skip the block, because this node has already signed a block of the same height");
            return Ok(BlockStatus::Skipped);
        }

        if !self.is_candidate_block_can_be_applied(candidate_block)? {
            tracing::trace!("Candidate block can't be applied");
            let current_producer_id = self.current_block_producer_id(&thread_id, &block_seq_no);
            let received_block_producer_id = candidate_block.data().get_common_section().producer_id;
            self.add_unprocessed_block(candidate_block.clone())?;
            if current_producer_id != received_block_producer_id {
                tracing::trace!("Node received block from unexpected BP");
                self.parse_block_attestations(candidate_block)?;
                for attestation in candidate_block.data().get_common_section().block_attestations {
                    self.attestation_processor.process_block_attestation(attestation);
                }
            }
            return Ok(BlockStatus::BlockCantBeApplied);
        }
        // Clear thread gap counter
        self.clear_block_gap(&thread_id);

        self.update_block_keeper_set_from_common_section(candidate_block.data())?;
        let is_candidate_block_had_to_be_produced_by_this_node = self.config.local.node_id == candidate_block.data().get_common_section().producer_id;
        if !is_candidate_block_had_to_be_produced_by_this_node {
            if self.parse_block_acks_and_nacks(candidate_block)? == BlockStatus::BadBlock {
                return Ok(BlockStatus::BadBlock);
            }
            if self.parse_block_attestations(candidate_block)? == BlockStatus::BadBlock {
                return Ok(BlockStatus::BadBlock);
            }
        }

        let parent_block_id = candidate_block.data().parent();
        if parent_block_id != BlockIdentifierFor::<TBlockProducerProcess>::default() {
            let parent_block = self.repository.get_block(&parent_block_id)?.expect("Failed to load parent block");
            if candidate_block.data().get_common_section().producer_id != parent_block.data().get_common_section().producer_id {
                self.resend_attestations_on_bp_change(block_seq_no, candidate_block.data().get_common_section().producer_id)?;
            }
        }

        // TODO:
        // - Check that block identifier is valid for the block content.
        // - Check seq no is correct for the given parent
        let min_broadcast_acceptance =
            self.min_signatures_count_to_accept_broadcasted_state(block_seq_no);
        let stored_block_broadcast_signatures_count =
            match self.repository.get_block(&candidate_block.data().identifier())? {
                None => 0,
                Some(saved_candidate_block) => saved_candidate_block
                    .clone_signature_occurrences()
                    .iter()
                    .filter(|e| *e.1 > 0)
                    .count(),
            };
        log::info!(
            "stored_block_broadcast_signatures_count: {}",
            stored_block_broadcast_signatures_count
        );
        let mut merged_block_broadcast_signatures_count: usize;

        tracing::trace!(
            "Incoming block signatures cnt: {}",
            candidate_block.clone_signature_occurrences().len()
        );

        let mut merged_block = if stored_block_broadcast_signatures_count > 0 {
            let stored_block = self
                .repository
                .get_block(&candidate_block.data().identifier())?
                .expect("Must be there. Checked above for signatures.");
            let mut merged_signatures_occurences = stored_block.clone_signature_occurrences();
            let block_signature_occurences = candidate_block.clone_signature_occurrences();
            for signer_index in block_signature_occurences.keys() {
                let new_count = (*merged_signatures_occurences.get(signer_index).unwrap_or(&0))
                    + (*block_signature_occurences.get(signer_index).unwrap());
                merged_signatures_occurences.insert(*signer_index, new_count);
            }
            merged_signatures_occurences.retain(|_k, count| *count > 0);
            merged_block_broadcast_signatures_count = merged_signatures_occurences.len();
            if merged_signatures_occurences.len() > stored_block_broadcast_signatures_count {
                let stored_aggregated_signature = stored_block.aggregated_signature();
                let merged_aggregated_signature = TBLSSignatureScheme::merge(
                    stored_aggregated_signature,
                    candidate_block.aggregated_signature(),
                )?;
                let merged_envelope = <TRepository as Repository>::CandidateBlock::create(
                    merged_aggregated_signature,
                    merged_signatures_occurences,
                    candidate_block.data().clone(),
                );
                self.repository.store_block(merged_envelope.clone())?;
                merged_envelope
            } else {
                stored_block
            }
        } else {
            // Store directly
            let mut signatures_occurrences = candidate_block.clone_signature_occurrences();
            signatures_occurrences.retain(|_k, count| *count > 0);
            merged_block_broadcast_signatures_count = signatures_occurrences.len();
            let aggregated_signature = candidate_block.aggregated_signature().clone();
            let db_block = <TRepository as Repository>::CandidateBlock::create(
                aggregated_signature,
                signatures_occurrences,
                candidate_block.data().clone(),
            );
            // Block will be stored after signing by this node, no need to store it here
            // self.repository.store_block(db_block.clone())?;
            db_block
        };

        let is_this_block_signed_by_this_node =
            self.is_candidate_block_signed_by_this_node(&merged_block)?;
        tracing::trace!("is_this_block_signed_by_this_node={is_this_block_signed_by_this_node}");

        // if is_this_block_signed_by_this_node
        //     && merged_block_broadcast_signatures_count == stored_block_broadcast_signatures_count
        // {
        //     tracing::trace!("No new signatures were added, skip the block candidate");
        //     return Ok(BlockStatus::Ok);
        // }

        if !is_this_block_signed_by_this_node {
            // Ensure candidate block **parent** is either:
            // - a finalized block
            // - a widely accepted bock
            // - or this node accepts it as the most possible candidate (signed) and it is
            //   not in a diverged branch!
            // Add own signature to the block stored.
            if self.is_this_node_in_block_keeper_set(&block_seq_no) {
                self.sign_candidate_block_envelope(&mut merged_block)?;
                merged_block_broadcast_signatures_count += 1;
            }

            self.repository.store_block(merged_block.clone())?;

            // Check if it triggers subsequent stored block signatures.
            // todo!();
        }
        // dispatch updated saved block.
        // let block_id = merged_block.data().identifier();
        // let updated_candidate_block =
        //     self.repository.get_block(&block_id)?.expect("must be stored").clone();

        let do_verify = self.verify_and_apply_block(merged_block.clone())?;

        if self.is_this_node_a_producer_for(&thread_id, &merged_block.data().seq_no()) {
            if merged_block_broadcast_signatures_count == 1 {
                self.broadcast_candidate_block(merged_block.clone())?;
            }
        } else if !do_verify && self.is_this_node_in_block_keeper_set(&block_seq_no) {
            let block_attestation = <Self as NodeAssociatedTypes>::BlockAttestation::create(
                merged_block.aggregated_signature().clone(),
                merged_block.clone_signature_occurrences(),
                AttestationData {
                    block_id: merged_block.data().identifier(),
                    block_seq_no: merged_block.data().seq_no(),
                }
            );

            let attestations = self.attestations_to_send.entry(merged_block.data().seq_no()).or_default();
            attestations.push(block_attestation);
        }
        if merged_block_broadcast_signatures_count >= min_broadcast_acceptance
            && !self
            .repository
            .is_block_accepted_as_main_candidate(&merged_block.data().identifier())?
            .unwrap_or(false)
        {
            let accept_res = self.on_candidate_block_is_accepted_by_majority(merged_block.data().clone())?;
            if accept_res != BlockStatus::Ok {
                return Ok(accept_res);
            }
        }
        self.remove_from_unprocessed(&block_seq_no, &block_id)?;
        tracing::trace!("Block successfully processed: {block_id:?} {}ms", start.elapsed().as_millis());
        Ok(BlockStatus::Ok)
    }

    pub(crate) fn add_unprocessed_block(
        &mut self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<()> {
        let entry =
            self.unprocessed_blocks_cache.entry(candidate_block.data().seq_no()).or_default();

        entry.insert(candidate_block.data().identifier());
        tracing::trace!("Add unprocessed block: {} len:{}", candidate_block.data(), entry.len());
        self.repository.store_block(candidate_block)?;
        Ok(())
    }

    pub(crate) fn take_next_unprocessed_block(
        &mut self,
        parent_id: BlockIdentifierFor<TBlockProducerProcess>,
        parent_seq_no: BlockSeqNoFor<TBlockProducerProcess>,
    ) -> anyhow::Result<Option<<Self as NodeAssociatedTypes>::CandidateBlock>> {
        // TODO: refactor to use fork choice rule
        tracing::trace!("take_next_unprocessed_block {:?} {:?}", parent_seq_no, parent_id);
        let mut child_block: Option<Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>> =
            None;
        let child_seq_no = parent_seq_no.next();
        if let Some(potential_descendants) = self.unprocessed_blocks_cache.get_mut(&child_seq_no) {
            for block in potential_descendants.iter() {
                let potential_descendant =
                    self.repository.get_block(block)?.expect("Failed to load unprocessed block");
                if potential_descendant.data().parent() == parent_id {
                    if child_block.is_some() {
                        let id = child_block.clone().unwrap().data().identifier();
                        if id != potential_descendant.data().identifier() {
                            // unimplemented!(
                            //     "There are several blocks with the same parent saved in cache"
                            // );
                            continue;
                        }
                    }
                    child_block = Some(potential_descendant);
                }
            }
            if let Some(block) = &child_block {
                potential_descendants.remove(&block.data().identifier());
            }
        }
        if let Some(true) = self.unprocessed_blocks_cache.get(&child_seq_no).map(|v| v.is_empty()) {
            self.unprocessed_blocks_cache.remove(&child_seq_no);
        }
        Ok(child_block)
    }

    // Clear unprocessed blocks older than last_finalized_seq_no (exclusively) from
    // cache and from repo. We do not clear the last finalized because its state
    // needed for later blocks apply.
    pub(crate) fn clear_unprocessed_till(
        &mut self,
        last_finalized_seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        if self.unprocessed_blocks_cache.is_empty() {
            return Ok(());
        }
        tracing::trace!(
            "Clear unprocessed before {:?}: {:?}",
            last_finalized_seq_no,
            self.unprocessed_blocks_cache
        );
        // take keys and cut them
        let keys: Vec<BlockSeqNoFor<TBlockProducerProcess>> =
            self.unprocessed_blocks_cache.keys().copied().collect();
        for key in keys {
            if &key < last_finalized_seq_no {
                let blocks = self.unprocessed_blocks_cache.remove(&key).unwrap();
                for block_id in blocks {
                    self.repository.erase_block(&block_id)?;
                }
            } else {
                break;
            }
        }
        tracing::trace!("Clear unprocessed finished");
        Ok(())
    }

    pub(crate) fn remove_from_unprocessed(
        &mut self,
        block_seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
        block_id: &BlockIdentifierFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        if self.unprocessed_blocks_cache.is_empty() {
            return Ok(());
        }
        tracing::trace!(
            "Remove from unprocessed {:?} {:?}: {:?}",
            block_seq_no,
            block_id,
            self.unprocessed_blocks_cache
        );
        if let Some(blocks) = self.unprocessed_blocks_cache.get_mut(block_seq_no) {
            blocks.remove(block_id);
            tracing::trace!("Remove from unprocessed finished");
        }
        Ok(())
    }

    pub(crate) fn on_incoming_block_request(
        &self,
        mut from: BlockSeqNoFor<TBlockProducerProcess>,
        to: BlockSeqNoFor<TBlockProducerProcess>,
        node_id: NodeIdentifier,
    ) -> anyhow::Result<()> {
        tracing::trace!("on_incoming_block_request from {node_id}: {:?} - {:?}", from, to);
        while from != to {
            let blocks = self
                .repository
                .get_block_from_repo_or_archive_by_seq_no(&from)?;
            if blocks.is_empty() {
                tracing::trace!("Failed to find required block by seq_no: {:?}", from);
                break;
            }
            let mut found_block_to_send = false;
            for block in blocks {
                if self.is_candidate_block_signed_by_this_node(&block)? {
                    self.send_candidate_block(block, node_id)?;
                    found_block_to_send = true;
                    break;
                }
            }
            if !found_block_to_send {
                break;
            }
            from = from.next();
        }
        Ok(())
    }

    pub(crate) fn store_and_accept_candidate_block(
        &mut self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<BlockStatus> {
        tracing::trace!("store_and_accept_candidate_block {} {:?}", candidate_block.data(), candidate_block.clone_signature_occurrences());
        let block_seq_no = candidate_block.data().seq_no();
        let min_broadcast_acceptance =
            self.min_signatures_count_to_accept_broadcasted_state(block_seq_no);

        let stored_block = self
            .repository
            .get_block(&candidate_block.data().identifier())?
            .ok_or(anyhow::format_err!("Failed to load block"))?;
        let mut merged_signatures_occurrences = stored_block.clone_signature_occurrences();
        let block_signature_occurrences = candidate_block.clone_signature_occurrences();
        let stored_block_signatures_count = stored_block.clone_signature_occurrences().len();
        for signer_index in block_signature_occurrences.keys() {
            let new_count = (*merged_signatures_occurrences.get(signer_index).unwrap_or(&0))
                + (*block_signature_occurrences.get(signer_index).unwrap());
            merged_signatures_occurrences.insert(*signer_index, new_count);
        }
        merged_signatures_occurrences.retain(|_k, count| *count > 0);
        let merged_block_broadcast_signatures_count = merged_signatures_occurrences.len();

        if merged_block_broadcast_signatures_count > stored_block_signatures_count {
            let stored_aggregated_signature = stored_block.aggregated_signature();
            let merged_aggregated_signature = TBLSSignatureScheme::merge(
                stored_aggregated_signature,
                candidate_block.aggregated_signature(),
            )?;
            let merged_envelope = <TRepository as Repository>::CandidateBlock::create(
                merged_aggregated_signature,
                merged_signatures_occurrences,
                candidate_block.data().clone(),
            );
            self.repository.store_block(merged_envelope)?;
        }

        if merged_block_broadcast_signatures_count >= min_broadcast_acceptance
            && !self
            .repository
            .is_block_accepted_as_main_candidate(&candidate_block.data().identifier())?
            .unwrap_or(false)
        {
            let accept_res = self.on_candidate_block_is_accepted_by_majority(candidate_block.data().clone())?;
            if accept_res != BlockStatus::Ok {
                return Ok(accept_res);
            }
        }
        Ok(BlockStatus::Ok)
    }
}
