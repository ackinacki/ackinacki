// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::AddAssign;
use std::ops::Sub;

use num_bigint::BigUint;
use num_traits::Zero;
use serde::Deserialize;
use serde::Serialize;

use crate::block::compare_hashes;
use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
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
    pub(crate) fn fork_choice_rule_for_attestations(
        &self,
        block_attestations: &Vec<Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>>,
    ) -> anyhow::Result<Option<Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>>> {
        let mut blocks = vec![];
        let mut block_seq_no = None;
        for attestation in block_attestations {
            let block_id = &attestation.data().block_id;
            let block = self.repository.get_block(block_id)?
                .ok_or(anyhow::format_err!("Failed to load block from repo: {block_id:?}"))?;
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
        if blocks_with_the_same_seq_no.is_empty() {
            return Ok(None);
        }
        if blocks_with_the_same_seq_no.len() == 1 {
            return Ok(blocks_with_the_same_seq_no.pop());
        }

        let common_parent_block_id = blocks_with_the_same_seq_no[0].data().parent();
        for block in blocks_with_the_same_seq_no.iter().skip(1) {
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
            return Ok(blocks_with_the_same_seq_no.get(index).cloned());
        }

        // We don't have a single leader, check if we have a block from parent block producer.
        let block_keeper_set = self.block_keeper_set_for(&block_seq_no);
        let parent_block = self.repository.get_block_from_repo_or_archive(&common_parent_block_id)?;
        let parent_producer_id = parent_block.data().get_common_section().producer_id as SignerIndex;
        // Check that parent block producer is still in block keeper set
        if block_keeper_set.contains_key(&parent_producer_id) && !block_keeper_set.get(&parent_producer_id).unwrap().stake.is_zero() {
            for index in widely_accepted_blocks_indexes {
                if blocks_with_the_same_seq_no[index].data().get_common_section().producer_id == parent_producer_id as NodeIdentifier {
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
            return Ok(None);
        }

        // If there is only one maximum stake sum return it
        if maximum_stakes.len() == 1 {
            return Ok(blocks_with_the_same_seq_no.get(maximum_stakes.into_iter().next().unwrap()).cloned());
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
            return Ok(blocks_with_the_same_seq_no.get(index).cloned());
        }
        Ok(None)
    }
}
