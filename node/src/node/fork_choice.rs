// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::AddAssign;
use std::ops::Sub;

use num_bigint::BigUint;
use num_traits::Zero;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::compare_hashes;
use crate::types::AckiNackiBlock;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

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
    pub(crate) fn fork_choice_rule_for_attestations(
        &self,
        block_attestations: &Vec<Envelope<TBLSSignatureScheme, AttestationData>>,
    ) -> anyhow::Result<Option<Envelope<TBLSSignatureScheme, AttestationData>>> {
        let mut blocks = vec![];
        let mut block_seq_no = None;
        for attestation in block_attestations {
            let block_id = &attestation.data().block_id;
            let block = self.repository.get_block_from_repo_or_archive(block_id)?;
            if block_seq_no.is_none() {
                block_seq_no = Some(block.data().seq_no());
            } else if &block.data().seq_no() != block_seq_no.as_ref().unwrap() {
                anyhow::bail!("Blocks have different seq_no");
            }
            blocks.push(block);
        }
        Ok(if let Some(block) = self.fork_choice_rule(blocks)? {
            block_attestations.iter().find(|attestation| attestation.data().block_id == block.data().identifier()).cloned()
        } else {
            None
        })
    }

    // Fork choice rule allows to choose one block of the particular height
    pub(crate) fn fork_choice_rule(
        &self,
        mut blocks_with_the_same_seq_no: Vec<<Self as NodeAssociatedTypes>::CandidateBlock>,
    ) -> anyhow::Result<Option<<Self as NodeAssociatedTypes>::CandidateBlock>> {
        tracing::trace!("fork_choice_rule start");
        if blocks_with_the_same_seq_no.is_empty() {
            tracing::trace!("fork_choice_rule list is empty");
            return Ok(None);
        }
        if blocks_with_the_same_seq_no.len() == 1 {
            let block = blocks_with_the_same_seq_no.pop();
            tracing::trace!("fork_choice_rule list has only one block: {:?}", block.as_ref().unwrap().data().identifier());
            return Ok(block);
        }

        let common_parent_block_id = blocks_with_the_same_seq_no[0].data().parent();
        for block in blocks_with_the_same_seq_no.iter().skip(1) {
            tracing::trace!("fork_choice_rule block: {block}");
            if block.data().parent() != common_parent_block_id {
                anyhow::bail!("Blocks with the same seq_no have different parents");
            }
        }

        // Check if we already have a single votes leader
        let block_seq_no = blocks_with_the_same_seq_no.first().unwrap().data().seq_no();
        let number_of_signatures_for_acceptance = self.min_signatures_count_to_accept_broadcasted_state(block_seq_no);
        let mut widely_accepted_blocks_indexes = vec![];
        for (index, block) in blocks_with_the_same_seq_no.iter().enumerate() {
            let votes = block.clone_signature_occurrences().len();
            if votes >= number_of_signatures_for_acceptance {
                widely_accepted_blocks_indexes.push(index);
            }
        }
        if widely_accepted_blocks_indexes.len() == 1 {
            let index = widely_accepted_blocks_indexes.pop().unwrap();
            tracing::trace!("fork_choice_rule block node has one widely accepted block: {}", blocks_with_the_same_seq_no.get(index).as_ref().unwrap());
            return Ok(blocks_with_the_same_seq_no.get(index).cloned());
        }

        // We don't have a single leader, check if we have a block from parent block producer.
        let block_keeper_set = self.block_keeper_set_for(&block_seq_no, &self.get_block_thread_id(&blocks_with_the_same_seq_no[0])?);
        let parent_block = self.repository.get_block_from_repo_or_archive(&common_parent_block_id)?;
        let parent_producer_id = parent_block.data().get_common_section().producer_id as SignerIndex;
        // Check that parent block producer is still in block keeper set
        if block_keeper_set.contains_key(&parent_producer_id) && !block_keeper_set.get(&parent_producer_id).unwrap().stake.is_zero() {
            for index in widely_accepted_blocks_indexes {
                if blocks_with_the_same_seq_no[index].data().get_common_section().producer_id == parent_producer_id as NodeIdentifier {
                    tracing::trace!("fork_choice_rule block node has block from previous BP: {}", blocks_with_the_same_seq_no.get(index).as_ref().unwrap());
                    return Ok(blocks_with_the_same_seq_no.get(index).cloned());
                }
            }
        } else {
            // If previous BP was slashed remove it from the list
            for i in 0..widely_accepted_blocks_indexes.len() {
                let index = widely_accepted_blocks_indexes[i];
                if blocks_with_the_same_seq_no[index].data().get_common_section().producer_id == parent_producer_id as NodeIdentifier {
                    widely_accepted_blocks_indexes.remove(i);
                    break;
                }
            }
            if widely_accepted_blocks_indexes.len() == 1 {
                let index = widely_accepted_blocks_indexes[0];
                tracing::trace!("fork_choice_rule block node has block except onr from slashed previous BP: {}", blocks_with_the_same_seq_no.get(index).as_ref().unwrap());
                return Ok(blocks_with_the_same_seq_no.get(index).cloned())
            }
        }

        // We don't have a single good block, let's count the stakes
        // Calculate U
        let mut conflicting_blocks_stakes = vec![];
        for candidate_block in &blocks_with_the_same_seq_no {
            let mut total_stake = BigUint::zero();
            for signer_index in candidate_block.clone_signature_occurrences().keys() {
                total_stake.add_assign(&block_keeper_set.get(signer_index).expect("Failed to get block keeper data").stake);
            }
            conflicting_blocks_stakes.push(total_stake);
        }

        // Find maximums stake amounts M
        let mut maximum_stakes = HashSet::new();
        let mut max_stake = BigUint::zero();
        for (index, stake) in conflicting_blocks_stakes.iter().enumerate() {
            let stake = stake.clone();
            match stake.cmp(&max_stake) {
                Ordering::Equal => {
                    maximum_stakes.insert(index);
                },
                Ordering::Greater => {
                    max_stake = stake.clone();
                    maximum_stakes.clear();
                    maximum_stakes.insert(index);
                },
                _ => {}
            }
        }

        // Calculate min add stake for condition change
        let mut diffs = vec![]; // D vector
        for (index, stake) in conflicting_blocks_stakes.iter().enumerate() {
            if !maximum_stakes.contains(&index) {
                diffs.push(max_stake.clone().sub(stake));
            }
        }
        let min_add_stake = diffs.into_iter().min().unwrap_or(BigUint::zero());

        // Collect signer indexes that have already attested blocks
        let mut attested_signers = HashSet::new();
        for candidate_block in &blocks_with_the_same_seq_no {
            for signer_index in candidate_block.clone_signature_occurrences().keys() {
                attested_signers.insert(*signer_index);
            }
        }

        // Calculate stake amount not yet confirmed blocks
        let mut undistributed_stake = BigUint::zero();
        for (signer, block_keeper_data) in &block_keeper_set {
            if !attested_signers.contains(signer) {
                undistributed_stake.add_assign(&block_keeper_data.stake);
            }
        }

        // Check border conditions
        if min_add_stake < undistributed_stake { // Add condition sigma(Ai) < A
            tracing::trace!("fork_choice_rule there is no single leader and there are still undistributed votes");
            return Ok(None);
        }

        // If there is only one maximum stake sum return it
        if maximum_stakes.len() == 1 {
            let chosen_block = blocks_with_the_same_seq_no.get(maximum_stakes.into_iter().next().unwrap());
            tracing::trace!("fork_choice_rule there is a leader with maximum stake: {}", chosen_block.as_ref().unwrap());
            return Ok(chosen_block.cloned());
        } else if !maximum_stakes.is_empty() {
            // Else if there are several blocks with the same stake sum, return block with the
            // least hash
            let mut block_hashes = vec![];
            for index in maximum_stakes {
                let block_hash = blocks_with_the_same_seq_no.get(index).unwrap().data().get_hash();
                block_hashes.push((index, block_hash));
            }
            block_hashes.sort_by(|l, r| compare_hashes(&l.1, &r.1));
            let index = block_hashes.pop().unwrap().0;
            tracing::trace!("fork_choice_rule return block with the least hash: {}", blocks_with_the_same_seq_no.get(index).as_ref().unwrap());
            return Ok(blocks_with_the_same_seq_no.get(index).cloned());
        }
        tracing::trace!("fork_choice_rule no block was chosen");
        Ok(None)
    }

    pub fn check_if_block_should_be_finalized(
        &mut self,
        thread_id: &ThreadIdentifier,
        block_seq_no: &BlockSeqNo
    ) -> anyhow::Result<bool> {
        tracing::trace!("check_if_block_should_be_finalized start");
        let mut distributed_votes_cnt = 0;
        let blocks_with_the_same_seq_no = self.repository.list_blocks_with_seq_no(block_seq_no, &self.thread_id)?;
        for candidate in &blocks_with_the_same_seq_no {
            distributed_votes_cnt += candidate.clone_signature_occurrences().len();
        }
        let total_bk_cnt = self.get_block_keeper_set(block_seq_no, thread_id).len();
        tracing::trace!("check_if_block_should_be_finalized distributed_votes_cnt: {distributed_votes_cnt}, total_bk_cnt: {total_bk_cnt}");
        let (_last_signed_block_id, last_signed_block_seq_no) = self.find_thread_last_block_id_this_node_can_continue(thread_id)?;
        let block_gap: <BlockSeqNo as std::ops::Sub>::Output = last_signed_block_seq_no - block_seq_no;
        tracing::trace!("check_if_block_should_be_finalized block_gap: {block_gap}");
        if distributed_votes_cnt >= total_bk_cnt || block_gap > self.config.global.attestation_validity_block_gap {
            tracing::trace!("check_if_block_should_be_finalized choose block");
            let chosen = self.fork_choice_rule(blocks_with_the_same_seq_no)?.expect("After all votes were distributed one block must be chosen");
            self.on_candidate_block_is_accepted_by_majority(chosen.data().clone())?;
            self.on_block_finalized(&chosen)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
