use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use derive_getters::Getters;
use node_types::BlockIdentifier;
use parking_lot::Mutex;
use tracing::instrument;
use typed_builder::TypedBuilder;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::AttestationTargetType;
use crate::node::block_state::attestation_target_checkpoints::AttestationTargetCheckpoint;
use crate::node::block_state::repository::BlockState;
use crate::node::SignerIndex;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;
use crate::utilities::FixedSizeHashMap;

mod compacted_attestation;
mod compacted_map_key;
mod fold;
use compacted_attestation::CompactedAttestation;
use fold::try_fold;

use crate::types::attestation::compacted_map_key::CompactedMapKey;
use crate::utilities::guarded::AllowGuardedMut;

type CompactedMap<T> = BTreeMap<compacted_map_key::CompactedMapKey, T>;

pub const AGGREGATED_ATTESTATIONS_CACHE_SIZE: usize = 128;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AttestationCacheKey {
    pub block_id: BlockIdentifier,
    pub target_type: AttestationTargetType,
}

impl From<&Envelope<AttestationData>> for AttestationCacheKey {
    fn from(value: &Envelope<AttestationData>) -> Self {
        Self { block_id: *value.data().block_id(), target_type: *value.data().target_type() }
    }
}

pub type AggregatedAttestationsCache =
    Arc<Mutex<FixedSizeHashMap<AttestationCacheKey, Envelope<AttestationData>>>>;

pub fn new_aggregated_attestations_cache() -> AggregatedAttestationsCache {
    Arc::new(Mutex::new(FixedSizeHashMap::new(AGGREGATED_ATTESTATIONS_CACHE_SIZE)))
}

pub fn add_to_aggregated_attestations_cache(
    cache: &AggregatedAttestationsCache,
    attestation: Envelope<AttestationData>,
) {
    let key = AttestationCacheKey::from(&attestation);
    let mut cache = cache.lock();
    let Some(existing) = cache.get_mut(&key) else {
        cache.insert(key, attestation);
        return;
    };

    if existing.data().parent_block_id() != attestation.data().parent_block_id()
        || existing.data().block_seq_no() != attestation.data().block_seq_no()
        || existing.data().envelope_hash() != attestation.data().envelope_hash()
        || existing.data().block_id() != attestation.data().block_id()
        || existing.data().target_type() != attestation.data().target_type()
    {
        tracing::warn!(
            ?key,
            existing = ?existing.data(),
            incoming = ?attestation.data(),
            "Conflicting attestation was not stored in aggregated attestations cache"
        );
        return;
    }

    let existing_signers: HashSet<SignerIndex> = existing.signers().copied().collect();
    let incoming_signers: HashSet<SignerIndex> = attestation.signers().copied().collect();
    if incoming_signers.is_subset(&existing_signers) {
        tracing::trace!(
            ?key,
            "Incoming attestation does not add unique signers to aggregated attestations cache"
        );
        return;
    }

    let mut signature_occurrences = existing.clone_signature_occurrences();
    for (signer_index, count) in attestation.clone_signature_occurrences() {
        let Some(new_count) = signature_occurrences
            .get(&signer_index)
            .copied()
            .unwrap_or_default()
            .checked_add(count)
        else {
            tracing::trace!(
                ?key,
                signer_index,
                "Attestation signature occurrence count overflow in aggregated attestations cache"
            );
            return;
        };
        signature_occurrences.insert(signer_index, new_count);
    }
    signature_occurrences.retain(|_, count| *count > 0);

    let Ok(aggregated_signature) =
        GoshBLS::merge(existing.aggregated_signature(), attestation.aggregated_signature())
    else {
        tracing::trace!(
            ?key,
            "Failed to merge attestation signatures for aggregated attestations cache"
        );
        return;
    };
    *existing = Envelope::<AttestationData>::create(
        aggregated_signature,
        signature_occurrences,
        existing.data().clone(),
    );
}

#[derive(Default, Clone)]
#[allow(clippy::disallowed_types)]
pub struct CollectedAttestations {
    compacted_unverified_attestations: CompactedMap<HashSet<CompactedAttestation>>,
    folded_attestations: CompactedMap<Envelope<AttestationData>>,
    cutoff: BlockSeqNo,
    notifications: Arc<(parking_lot::Mutex<u32>, parking_lot::Condvar)>,
}

impl std::fmt::Debug for CollectedAttestations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CollectedAttestations(cutoff={})", self.cutoff)
    }
}

impl AllowGuardedMut for CollectedAttestations {}

#[derive(Default, Debug)]
pub enum AggregateActionOnConditionMissed {
    #[default]
    Skip,
}

#[derive(TypedBuilder, Getters, Debug)]
pub struct AggregateFilter {
    attestation_type: AttestationTargetType,
    min_signatures_inclusive: usize,

    #[builder(default)]
    action_on_condition_missed: AggregateActionOnConditionMissed,
}

impl From<&AttestationTargetCheckpoint> for AggregateFilter {
    fn from(checkpoint: &AttestationTargetCheckpoint) -> Self {
        Self {
            attestation_type: *checkpoint.attestation_target_type(),
            min_signatures_inclusive: *checkpoint.required_attestation_count(),
            action_on_condition_missed: Default::default(),
        }
    }
}

impl CollectedAttestations {
    pub fn notifications(&self) -> &Arc<(parking_lot::Mutex<u32>, parking_lot::Condvar)> {
        &self.notifications
    }

    pub fn touch(&self) {
        let mut e = self.notifications.0.lock();
        *e = e.wrapping_add(1);
        self.notifications.1.notify_all();
    }

    // Note: we've decided that Attestation will be authenticated on the network
    // layer.
    // Individual signature check is a fallback scenario that will trigger a NACK.
    pub fn add(
        &mut self,
        attestation: Envelope<AttestationData>,
        enable_cutoff: bool,
    ) -> anyhow::Result<bool> {
        if enable_cutoff && &self.cutoff >= attestation.data().block_seq_no() {
            return Ok(false);
        }
        let key = CompactedMapKey::builder()
            .block_seq_no(*attestation.data().block_seq_no())
            .block_identifier(*attestation.data().block_id())
            .attestation_target_type(*attestation.data().target_type())
            .build();
        // default for the <or_insert> case
        let mut is_modified = true;
        self.compacted_unverified_attestations
            .entry(key)
            .and_modify(|e| {
                if !e.insert((&attestation).into()) {
                    is_modified = false;
                }
            })
            .or_insert(HashSet::from_iter([(&attestation).into()]));
        if is_modified {
            self.touch();
        }
        Ok(is_modified)
    }

    pub fn add_bunch(
        &mut self,
        attestations: Vec<Envelope<AttestationData>>,
        enable_cutoff: bool,
    ) -> anyhow::Result<bool> {
        let attestations = if enable_cutoff {
            attestations.into_iter().filter(|e| e.data().block_seq_no() > &self.cutoff).collect()
        } else {
            attestations
        };
        if attestations.is_empty() {
            return Ok(false);
        }
        let mut added_smth = false;
        for attestation in attestations {
            let key = CompactedMapKey::builder()
                .block_seq_no(*attestation.data().block_seq_no())
                .block_identifier(*attestation.data().block_id())
                .attestation_target_type(*attestation.data().target_type())
                .build();
            // default for the <or_insert> case
            let mut is_modified = true;
            self.compacted_unverified_attestations
                .entry(key)
                .and_modify(|e| {
                    if !e.insert((&attestation).into()) {
                        is_modified = false;
                    }
                })
                .or_insert(HashSet::from_iter([(&attestation).into()]));
            if is_modified {
                added_smth = true;
            }
        }
        if added_smth {
            self.touch();
        }
        Ok(added_smth)
    }

    #[instrument(skip_all)]
    // delete al keys that less or equal then stop
    pub fn move_cutoff(&mut self, block_seq_no: BlockSeqNo, _block_id: BlockIdentifier) {
        tracing::trace!(?block_seq_no, "CollectedAttestations: cleared old attestations");
        self.cutoff = block_seq_no;
        self.compacted_unverified_attestations.retain(|k, _| *k.block_seq_no() > block_seq_no);
        self.folded_attestations.retain(|k, _| *k.block_seq_no() > block_seq_no);
    }

    // Aggregate attestations.
    #[instrument(skip_all)]
    pub fn aggregate(
        &mut self,
        required: &[(BlockState, AggregateFilter)],
    ) -> anyhow::Result<Vec<Envelope<AttestationData>>> {
        let mut result = vec![];
        tracing::trace!(?required, "CollectedAttestations: aggregate");

        for (block_state, aggregate_filter) in required.iter() {
            let check_stored_in_state = match aggregate_filter.attestation_type() {
                AttestationTargetType::Primary => block_state.guarded(|e| {
                    e.primary_finalization_proof().clone().or(e.prefinalization_proof().clone())
                }),
                AttestationTargetType::Fallback => {
                    block_state.guarded(|e| e.fallback_finalization_proof().clone())
                }
            };
            if let Some(stored) = check_stored_in_state {
                if stored.signatures_count() >= aggregate_filter.min_signatures_inclusive {
                    result.push(stored.clone());
                    continue;
                }
            }

            // Check if already folded is good enough
            let block_identifier = *block_state.block_identifier();
            let Some(block_seq_no) = block_state.guarded(|e| *e.block_seq_no()) else {
                tracing::trace!("CollectedAttestations: aggregate: failed to get block_seq_no. Block id: {block_identifier}");
                anyhow::bail!("BlockSeqNo is missing");
            };
            let Some(parent_block_identifier) =
                block_state.guarded(|e| *e.parent_block_identifier())
            else {
                tracing::trace!("CollectedAttestations: aggregate: failed to get parent_block_identifier. Block id: {block_identifier}");
                anyhow::bail!("Parent block id is missing");
            };
            // let Some(envelope_hash) =  block_state.guarded(|e| e.envelope_hash().clone())
            // else {
            //     tracing::trace!("CollectedAttestations: aggregate: failed to get envelope_hash. Block id: {block_identifier}");
            //     anyhow::bail!("Block envelope_hash is missing");
            // };
            let key = CompactedMapKey::builder()
                .block_seq_no(block_seq_no)
                .block_identifier(block_identifier)
                .attestation_target_type(*aggregate_filter.attestation_type())
                .build();
            let folded_attestation = self.folded_attestations.get(&key);
            if let Some(folded_attestation) = folded_attestation {
                if folded_attestation.signatures_count()
                    >= aggregate_filter.min_signatures_inclusive
                {
                    result.push(folded_attestation.clone());
                    continue;
                }
            }

            // Check if it will be enough if we combine folded and unverified.
            tracing::trace!(?block_identifier, "CollectedAttestations: aggregate");
            let Some(bk_set) = block_state.guarded(|e| e.bk_set().clone()) else {
                tracing::trace!("CollectedAttestations: aggregate: Can't verify block attestations. Missing bk set. Block id: {block_identifier}");
                anyhow::bail!("Missing bk set");
            };
            let Some(envelope_hash) = block_state.guarded(|e| e.envelope_hash().clone()) else {
                tracing::trace!("CollectedAttestations: aggregate: failed to get an envelope hash. Block id: {block_identifier}");
                anyhow::bail!("Envelope hash is missing");
            };
            let folded_attestation_signers: HashSet<SignerIndex> = folded_attestation
                .map_or(Default::default(), |e: &Envelope<AttestationData>| {
                    e.signers().cloned().collect()
                });
            let mut combined_signers = folded_attestation_signers;
            let mut to_combine = vec![];
            if let Some(folded_attestation) = folded_attestation {
                to_combine.push((
                    folded_attestation.clone_signature_occurrences(),
                    folded_attestation.aggregated_signature().clone(),
                ));
            }
            if let Some(unverified_attestations) = self.compacted_unverified_attestations.get(&key)
            {
                for attestation in unverified_attestations.iter() {
                    if attestation.parent_block_id() != &parent_block_identifier
                        || attestation.envelope_hash() != &envelope_hash
                    {
                        continue;
                    }
                    if attestation.signers().all(|e| bk_set.contains_signer(e)) {
                        combined_signers.extend(attestation.signers());
                        to_combine.push((
                            attestation.signature_occurrences().clone().into_iter().collect(),
                            attestation.aggregated_signature().clone(),
                        ));
                    }
                }
            }
            if combined_signers.len() >= aggregate_filter.min_signatures_inclusive {
                let attestation_data = AttestationData::builder()
                    .block_id(block_identifier)
                    .block_seq_no(block_seq_no)
                    .parent_block_id(parent_block_identifier)
                    .envelope_hash(envelope_hash)
                    .target_type(*aggregate_filter.attestation_type())
                    .build();
                let fold_result = try_fold(attestation_data, to_combine, &bk_set);
                let Some(new_fold) = fold_result.folded else {
                    match aggregate_filter.action_on_condition_missed {
                        AggregateActionOnConditionMissed::Skip => {
                            tracing::trace!("Failed to fold any attestation. Count missed {block_identifier}. Using skip action");
                            continue;
                        }
                    }
                };
                self.folded_attestations.insert(key, new_fold.clone());
                if !fold_result.poisoned.is_empty() {
                    // TODO: send NACKS.
                }
                // another check is required. try_fold may skip some attestations in case of poisoning.
                // Note: should trigger NACK in the next release.
                if new_fold.signatures_count() >= aggregate_filter.min_signatures_inclusive {
                    result.push(new_fold);
                    continue;
                }
            }
            match aggregate_filter.action_on_condition_missed {
                AggregateActionOnConditionMissed::Skip => {
                    tracing::trace!(
                        "Attestations count missed {block_identifier}. Using skip action"
                    );
                    continue;
                }
            }
            /*
            // 1) verify all unverified atts for block
            // 2) retain all successfully verified
            // 3) merge them to folded
            // 4) remove from unverified
            // 5) return folded

            // If we have unverified attestations for the block
            for key in self
                .compacted_unverified_attestations
                .clone()
                .keys()
                .filter(|e| e.block_identifier() == block_id)
            {
                let attestations = self
                    .compacted_unverified_attestations
                    .remove(key)
                    .expect("we have already checked that key present");
                for compacted_attestation in attestations {
                    let attestation: Envelope<GoshBLS, AttestationData> =
                        (compacted_attestation, key).into();
                    if !attestation.verify_signatures(bk_set.get_pubkeys_by_signers())? {
                        tracing::trace!("CollectedAttestations: aggregate: attestation verification failed: {attestation:?}");
                        continue;
                    }

                    if attestation.data().envelope_hash() != &envelope_hash {
                        tracing::warn!("CollectedAttestations: double signatures detected. Two envelopes for the same block id");
                        // TODO: send Nack.
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
            for key in self.folded_attestations.keys().filter(|e| e.block_identifier() == block_id)
            {
                result.push(self.folded_attestations.get(key).unwrap().clone());
            }
            */
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use node_types::BlockIdentifier;

    use super::*;
    use crate::bls::gosh_bls::Signature;
    use crate::types::envelope_hash::AckiNackiEnvelopeHash;

    fn test_attestation(
        parent_block_id: BlockIdentifier,
        block_id: BlockIdentifier,
        envelope_hash: AckiNackiEnvelopeHash,
        target_type: AttestationTargetType,
        signer_indices: impl IntoIterator<Item = SignerIndex>,
    ) -> Envelope<AttestationData> {
        let signature_occurrences =
            HashMap::from_iter(signer_indices.into_iter().map(|signer_index| (signer_index, 1)));
        Envelope::create(
            Signature::default(),
            signature_occurrences,
            AttestationData::builder()
                .parent_block_id(parent_block_id)
                .block_id(block_id)
                .block_seq_no(BlockSeqNo::from(1))
                .envelope_hash(envelope_hash)
                .target_type(target_type)
                .build(),
        )
    }

    #[test]
    fn aggregated_cache_merges_attestations_for_same_key() {
        let cache = new_aggregated_attestations_cache();
        let parent_block_id = BlockIdentifier::new([1; 32]);
        let block_id = BlockIdentifier::new([2; 32]);
        let envelope_hash = AckiNackiEnvelopeHash([3; 32]);

        add_to_aggregated_attestations_cache(
            &cache,
            test_attestation(
                parent_block_id,
                block_id,
                envelope_hash.clone(),
                AttestationTargetType::Primary,
                [1],
            ),
        );
        add_to_aggregated_attestations_cache(
            &cache,
            test_attestation(
                parent_block_id,
                block_id,
                envelope_hash,
                AttestationTargetType::Primary,
                [1, 2],
            ),
        );

        let cache = cache.lock();
        let cached = cache
            .get(&AttestationCacheKey { block_id, target_type: AttestationTargetType::Primary })
            .unwrap();
        assert_eq!(cached.signatures_count(), 2);
        assert!(cached.has_signer_index(1));
        assert!(cached.has_signer_index(2));
    }

    #[test]
    fn aggregated_cache_keeps_existing_attestation_on_conflict() {
        let cache = new_aggregated_attestations_cache();
        let parent_block_id = BlockIdentifier::new([1; 32]);
        let block_id = BlockIdentifier::new([2; 32]);
        let original_hash = AckiNackiEnvelopeHash([3; 32]);

        add_to_aggregated_attestations_cache(
            &cache,
            test_attestation(
                parent_block_id,
                block_id,
                original_hash.clone(),
                AttestationTargetType::Primary,
                [1],
            ),
        );
        add_to_aggregated_attestations_cache(
            &cache,
            test_attestation(
                parent_block_id,
                block_id,
                AckiNackiEnvelopeHash([4; 32]),
                AttestationTargetType::Primary,
                [2],
            ),
        );

        let cache = cache.lock();
        let cached = cache
            .get(&AttestationCacheKey { block_id, target_type: AttestationTargetType::Primary })
            .unwrap();
        assert_eq!(cached.data().envelope_hash(), &original_hash);
        assert_eq!(cached.signatures_count(), 1);
        assert!(cached.has_signer_index(1));
        assert!(!cached.has_signer_index(2));
    }
}
