// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::AckiNackiBlock;

const _RESEND_ATTESTATION_BLOCK_DIFF: u32 = 10;
const _RESEND_ATTESTATION_TIMEOUT_MS: u128 = 2000;

impl<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = RepositoryImpl>,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateImpl,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = RepositoryImpl
        >,
        TRandomGenerator: rand::Rng,
{
    pub(crate) fn _parse_block_attestations(
        &mut self,
        _candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<BlockStatus> {
        // tracing::trace!("parse_block_attestations");
        // let thread_id = self.get_block_thread_id(candidate_block)?;
        // let mut sent_attestations = self.sent_attestations.entry(thread_id).or_default().clone();
        // tracing::trace!("sent_attestations: {:?}", sent_attestations);
        // let block_attestations =
        //     &candidate_block.data().get_common_section()
        //         .block_attestations;
        // tracing::trace!("block_attestations(len): {:?}", block_attestations.len());
        // let attestation_limit_seq_no = self.get_attestation_limit_seq_no()?;
        // let mut block_state = self.blocks_states.get(&candidate_block.data().identifier())?;
        // for attestation in block_attestations {
        //     if !self.check_attestation(attestation)? {
        //         return Ok(BlockStatus::BadBlock);
        //     }
        //     let attestation_target: BlockIdentifier = attestation.data().block_id.clone();
        //     let attestation_signers: HashSet<SignerIndex> = HashSet::from_iter(
        //         attestation
        //             .clone_signature_occurrences()
        //             .keys()
        //             .cloned()
        //             .into_iter()
        //     );
        //     block_state.guarded_mut(|e| {
        //         if let Some(existing_attestations) = e.verified_attestations_for(&attestation_target) {
        //             assert!(existing_attestations == attestation_signers);
        //             Ok(())
        //         } else {
        //             e.add_verified_attestations_for(attestation_target, attestation_signers)
        //         }
        //     })?;
        // }
        //
        // // clean old sent attestations
        // for i in (0..sent_attestations.len()).rev() {
        //     let sent_attestation = &sent_attestations[i].1;
        //     if sent_attestation.data().block_seq_no <= attestation_limit_seq_no {
        //         let (_, removed) = sent_attestations.remove(i);
        //         tracing::trace!("Removed attestation for old block (older than finalized): {:?}", removed.data());
        //     }
        // }
        //
        // for block_attestation in block_attestations.iter() {
        //     tracing::trace!("Processing block attestation: {:?}", block_attestation);
        //     for i in (0..sent_attestations.len()).rev() {
        //         let sent_attestation = &sent_attestations[i].1;
        //         if sent_attestation.data() == block_attestation.data() {
        //             if let Some(signer_index) = self.get_node_signer_index_for_block_id(block_attestation.data().block_id.clone()) {
        //                 let block_attestation_occurrences = block_attestation.clone_signature_occurrences();
        //                 let incoming_attestation_contains_this_node_signature =
        //                     block_attestation_occurrences.get(
        //                         &signer_index
        //                     ).unwrap_or(&0) > &0;
        //
        //                 if incoming_attestation_contains_this_node_signature {
        //                     let (_, removed) = sent_attestations.remove(i);
        //                     tracing::trace!("Removed attestation because it is present in the incoming block: {:?}", removed);
        //                 }
        //             }
        //         }
        //     }
        //     self.attestation_processor.process_block_attestation(block_attestation.clone());
        //     // self.on_incoming_block_attestation(block_attestation)?;
        // }
        // let current_bp_id = self.get_latest_block_producer(&thread_id);
        // tracing::trace!("sent_attestations: {:?}", sent_attestations);
        // let limit_seq_no = candidate_block.data().seq_no().saturating_sub(RESEND_ATTESTATION_BLOCK_DIFF);
        // for sent_attestation in sent_attestations.iter_mut().rev() {
        //     let attestation_seq_no = sent_attestation.0;
        //     if limit_seq_no > attestation_seq_no {
        //         let attestation = sent_attestation.1.clone();
        //         tracing::trace!("Resend attestation: {:?}", attestation);
        //         self.send_block_attestation(current_bp_id, attestation)?;
        //         sent_attestation.0 = candidate_block.data().seq_no();
        //     }
        // }
        // let sent = self.sent_attestations.entry(thread_id).or_default();
        // *sent = sent_attestations;
        Ok(BlockStatus::Ok)
    }

    pub(crate) fn check_attestation(&self, attestation: &<Self as NodeAssociatedTypes>::BlockAttestation) -> anyhow::Result<bool> {
        tracing::trace!("Check attestation: {:?}", attestation);
        let AttestationData{ block_id, block_seq_no: _ } = attestation.data().clone();
        let keys_ring = match self.get_block_keeper_pubkeys(block_id) {
            Some(value) => value,
            None => return Ok(true),
        };
        let valid = attestation.verify_signatures(&keys_ring)?;
        if !valid {
            tracing::trace!("Bad attestation");
        }
        Ok(valid)
    }

    pub(crate) fn send_attestations(&mut self) -> anyhow::Result<()> {
        let unprocessed_blocks = {
            self.unprocessed_blocks_cache.lock().clone()
        };
        self.attestation_sender_service.evaluate(&unprocessed_blocks, &mut self.last_block_attestations, &self.repository);
        Ok(())
    }
}
