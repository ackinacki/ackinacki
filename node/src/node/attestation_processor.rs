// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::Mutex;

use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::SignerIndex;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

pub const LOOP_PAUSE_DURATION: Duration = Duration::from_millis(10);

pub trait AttestationProcessor {
    type BlockAttestation: BLSSignedEnvelope;
    type CandidateBlock: BLSSignedEnvelope;
    type PubKey;
    type Repository: Repository;
    type SignerIndex;

    fn process_block_attestation(&self, attestation: Self::BlockAttestation);

    fn get_processed_blocks(&self) -> Vec<Self::CandidateBlock>;
}

pub struct AttestationProcessorImpl {
    attestations_handler: JoinHandle<()>,
    attestations_queue: Arc<Mutex<Vec<<Self as AttestationProcessor>::BlockAttestation>>>,
    processed_blocks: Arc<Mutex<Vec<<Self as AttestationProcessor>::CandidateBlock>>>,
}

impl AttestationProcessorImpl {
    pub fn new(
        repository: <Self as AttestationProcessor>::Repository,
        block_keeper_ring: Arc<
            Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>>,
        >,
        thread_identifier: ThreadIdentifier,
    ) -> Self {
        let attestations_queue =
            Arc::new(Mutex::new(Vec::<<Self as AttestationProcessor>::BlockAttestation>::new()));
        let queue_clone = attestations_queue.clone();
        let processed_blocks = Arc::new(Mutex::new(Vec::new()));
        let processed_blocks_clone = processed_blocks.clone();
        let attestations_handler = std::thread::Builder::new()
            .name("Attestations processor".to_string())
            .spawn(move || {
                loop {
                    let attestations = {
                        let mut queue = queue_clone.lock();
                        let mut attestations = vec![];
                        if !queue.is_empty() {
                            let first_block_id = queue.first().unwrap().data().clone();
                            for i in (0..queue.len()).rev() {
                                if queue[i].data().clone() == first_block_id {
                                    attestations.push(queue.remove(i));
                                }
                            }
                        }
                        attestations
                    };
                    if attestations.is_empty() {
                        std::thread::sleep(LOOP_PAUSE_DURATION);
                        continue;
                    }

                    let block_id = attestations[0].data().block_id.clone();
                    tracing::info!(
                        "Processing block attestations({}) for: {:?}",
                        attestations.len(),
                        block_id
                    );
                    let mut stored_block = match repository.get_block_from_repo_or_archive(&block_id) {
                        Ok(block) => block,
                        Err(e) => {
                            tracing::trace!(
                                "Incoming block attestation error: block with id not found: {:?} {e}",
                                block_id
                            );
                            continue;
                        }
                    };
                    if stored_block.data().get_common_section().thread_id != thread_identifier {
                        continue;
                    }
                    let mut block_was_updated = false;
                    // let mut merged_signatures_cnt = 0;
                    for attestation in attestations {
                        tracing::info!("Incoming block attestation: {:?}", attestation,);
                        let envelope_with_incoming_signatures =
                            <Self as AttestationProcessor>::CandidateBlock::create(
                                attestation.aggregated_signature().clone(),
                                attestation.clone_signature_occurrences(),
                                stored_block.data().clone(),
                            );

                        if !check_block_signature(
                            block_keeper_ring.clone(),
                            &envelope_with_incoming_signatures,
                        ) {
                            // TODO: seems like we have bad attestation here, need to punish it's
                            // source
                            tracing::trace!("Attestation signatures are invalid");
                            continue;
                        }

                        let stored_block_broadcast_signatures_count = stored_block
                            .clone_signature_occurrences()
                            .iter()
                            .filter(|e| *e.1 > 0)
                            .count();

                        let mut merged_signatures_occurences =
                            stored_block.clone_signature_occurrences();
                        let block_signature_occurences =
                            envelope_with_incoming_signatures.clone_signature_occurrences();
                        for signer_index in block_signature_occurences.keys() {
                            let new_count =
                                (*merged_signatures_occurences.get(signer_index).unwrap_or(&0))
                                    + (*block_signature_occurences.get(signer_index).unwrap());
                            merged_signatures_occurences.insert(*signer_index, new_count);
                        }
                        merged_signatures_occurences.retain(|_k, count| *count > 0);

                        if merged_signatures_occurences.len() > stored_block_broadcast_signatures_count {
                            tracing::trace!("Save merged");
                            let stored_aggregated_signature = stored_block.aggregated_signature();
                            let merged_aggregated_signature = GoshBLS::merge(
                                stored_aggregated_signature,
                                envelope_with_incoming_signatures.aggregated_signature(),
                            )
                            .expect("Failed to merge signatures");
                            let merged_envelope =
                                <Self as AttestationProcessor>::CandidateBlock::create(
                                    merged_aggregated_signature,
                                    merged_signatures_occurences,
                                    stored_block.data().clone(),
                                );
                            block_was_updated = true;
                            stored_block = merged_envelope;
                        }
                    }
                    if block_was_updated {
                        let mut processed = processed_blocks_clone.lock();
                        processed.push(stored_block);
                    }
                }
            })
            .expect("Failed to spawn attestations processor");
        Self { attestations_handler, attestations_queue, processed_blocks }
    }
}

impl AttestationProcessor for AttestationProcessorImpl {
    type BlockAttestation = Envelope<GoshBLS, AttestationData>;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock<GoshBLS>>;
    type PubKey = PubKey;
    type Repository = RepositoryImpl;
    type SignerIndex = SignerIndex;

    fn process_block_attestation(&self, attestation: Self::BlockAttestation) {
        assert!(!self.attestations_handler.is_finished());
        let mut queue = self.attestations_queue.lock();
        queue.push(attestation);
    }

    fn get_processed_blocks(&self) -> Vec<Self::CandidateBlock> {
        let mut processed = self.processed_blocks.lock();
        let processed_clone = processed.clone();
        processed.clear();
        processed_clone
    }
}

pub fn get_block_keeper_pubkeys(
    block_keeper_sets: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>>>,
    block_seq_no: &BlockSeqNo,
    thread_id: &ThreadIdentifier,
) -> HashMap<SignerIndex, PubKey> {
    let thread_bk_sets =
        block_keeper_sets.lock().get(thread_id).expect("Failed to get BK set for thread").clone();
    for (seq_no, bk_set) in thread_bk_sets.iter().rev() {
        if seq_no > block_seq_no {
            continue;
        }
        return bk_set.iter().map(|(k, v)| (*k, v.pubkey.clone())).collect();
    }
    panic!("Failed to find BK set for block with seq_no: {block_seq_no:?}")
}

fn check_block_signature(
    block_keeper_ring: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>>>,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock<GoshBLS>>,
) -> bool {
    let block_keeper_ring = get_block_keeper_pubkeys(
        block_keeper_ring,
        &candidate_block.data().seq_no(),
        &candidate_block.data().get_common_section().thread_id,
    );
    let is_valid = candidate_block
        .verify_signatures(&block_keeper_ring)
        .expect("Signatures verification should not crash.");
    if !is_valid {
        tracing::trace!("Signature verification failed: {}", candidate_block);
    }
    is_valid
}
