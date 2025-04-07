use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use tracing::instrument;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;

mod compacted_attestation;
mod compacted_map_key;
use compacted_attestation::CompactedAttestation;

use crate::block_keeper_system::BlockKeeperSet;
use crate::utilities::guarded::AllowGuardedMut;

type CompactedMap<T> = BTreeMap<compacted_map_key::CompactedMapKey, T>;

#[derive(Default, Clone)]
pub struct CollectedAttestations {
    compacted_unverified_attestations: CompactedMap<HashSet<CompactedAttestation>>,
    folded_attestations: CompactedMap<Envelope<GoshBLS, AttestationData>>,
    cutoff: BlockSeqNo,
}

impl AllowGuardedMut for CollectedAttestations {}

impl CollectedAttestations {
    pub fn add(
        &mut self,
        attestation: Envelope<GoshBLS, AttestationData>,
        get_bk_set: impl Fn(&BlockIdentifier) -> Option<Arc<BlockKeeperSet>>,
    ) -> anyhow::Result<bool> {
        if &self.cutoff >= attestation.data().block_seq_no() {
            return Ok(false);
        }
        let key =
            (attestation.data().block_id().clone(), *attestation.data().block_seq_no()).into();
        // check if got new sigs
        if let Some(folded) = self.folded_attestations.get(&key) {
            if attestation
                .clone_signature_occurrences()
                .into_iter()
                .filter_map(
                    |(signer_index, count)| if count > 0 { Some(signer_index) } else { None },
                )
                .all(|e| folded.has_signer_index(e))
            {
                return Ok(false);
            }
        }
        self.compacted_unverified_attestations
            .entry(key)
            .and_modify(|e| {
                e.insert((&attestation).into());
            })
            .or_insert(HashSet::from_iter([(&attestation).into()]));
        self.aggregate(
            HashSet::from_iter([attestation.data().block_id().clone()].into_iter()),
            get_bk_set,
        )?;
        Ok(true)
    }

    #[instrument(skip_all)]
    // delete al keys that less or equal then stop
    pub fn move_cutoff(&mut self, block_seq_no: BlockSeqNo, block_id: BlockIdentifier) {
        tracing::trace!(?block_seq_no, "CollectedAttestations: cleared old attestations");
        self.cutoff = block_seq_no;
        let key: compacted_map_key::CompactedMapKey = (block_id, block_seq_no).into();
        self.compacted_unverified_attestations =
            self.compacted_unverified_attestations.split_off(&key);
        self.compacted_unverified_attestations.remove(&key);
        self.folded_attestations = self.folded_attestations.split_off(&key);
        self.folded_attestations.remove(&key);
    }

    #[instrument(skip_all)]
    // Add arg fn to check attestation or bk set
    pub fn aggregate(
        &mut self,
        required: HashSet<BlockIdentifier>,
        get_bk_set: impl Fn(&BlockIdentifier) -> Option<Arc<BlockKeeperSet>>,
    ) -> anyhow::Result<Vec<Envelope<GoshBLS, AttestationData>>> {
        let mut result = vec![];

        for block_id in &required {
            // 1) verify all unverified atts for block
            // 2) retain all successfully verified
            // 3) merge them to folded
            // 4) remove from unverified
            // 5) return folded

            // If we have unverified attestations for the block
            if let Some(key) = self
                .compacted_unverified_attestations
                .keys()
                .find(|e| e.block_identifier() == block_id)
                .cloned()
            {
                let Some(bk_set) = get_bk_set(block_id) else {
                    tracing::trace!("CollectedAttestations: aggregate: Can't verify block attestations, skip it {block_id}");
                    continue;
                };

                let attestations = self
                    .compacted_unverified_attestations
                    .remove(&key)
                    .expect("we have already checked that key present");
                for compacted_attestation in attestations {
                    let attestation: Envelope<GoshBLS, AttestationData> =
                        (compacted_attestation, &key).into();
                    if !attestation.verify_signatures(&bk_set.get_pubkeys_by_signers())? {
                        tracing::trace!("CollectedAttestations: aggregate: attestation verification failed: {attestation:?}");
                        continue;
                    }
                    tracing::trace!("Aggregate attestations block id: {:?}", block_id);
                    self.folded_attestations
                        .entry(key.clone())
                        .and_modify(|envelope: &mut Envelope<GoshBLS, AttestationData>| {
                            let mut merged_signatures_occurences =
                                envelope.clone_signature_occurrences();
                            let initial_signatures_count = merged_signatures_occurences.len();
                            let incoming_signature_occurrences =
                                attestation.clone_signature_occurrences();
                            for signer_index in incoming_signature_occurrences.keys() {
                                let new_count = (*merged_signatures_occurences
                                    .get(signer_index)
                                    .unwrap_or(&0))
                                    + (*incoming_signature_occurrences.get(signer_index).unwrap());
                                merged_signatures_occurences.insert(*signer_index, new_count);
                            }
                            merged_signatures_occurences.retain(|_k, count| *count > 0);

                            if merged_signatures_occurences.len() > initial_signatures_count {
                                let aggregated_signature = envelope.aggregated_signature();
                                let merged_aggregated_signature = GoshBLS::merge(
                                    aggregated_signature,
                                    attestation.aggregated_signature(),
                                )
                                .expect("Failed to merge attestations");
                                *envelope = Envelope::<GoshBLS, AttestationData>::create(
                                    merged_aggregated_signature,
                                    merged_signatures_occurences,
                                    envelope.data().clone(),
                                );
                            }
                        })
                        .or_insert(attestation.clone());
                }
            }
            if let Some(key) =
                self.folded_attestations.keys().find(|e| e.block_identifier() == block_id).cloned()
            {
                result.push(self.folded_attestations.get(&key).unwrap().clone());
            }
        }
        Ok(result)
    }
}
