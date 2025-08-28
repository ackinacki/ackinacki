use std::collections::HashMap;

use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::gosh_bls::Signature;
use crate::bls::BLSSignatureScheme;
use crate::types::attestation::AttestationData;
use crate::types::attestation::Envelope;
use crate::types::attestation::GoshBLS;
use crate::types::attestation::SignerIndex;

pub struct TryFoldResult {
    pub folded: Option<Envelope<GoshBLS, AttestationData>>,

    pub poisoned: Vec<Envelope<GoshBLS, AttestationData>>,
}

pub fn try_fold(
    attestation_data: AttestationData,
    to_combine: Vec<(HashMap<SignerIndex, u16>, Signature)>,
    bk_set: &BlockKeeperSet,
) -> TryFoldResult {
    if let Some(folded) = try_fold_all_optimistic(attestation_data.clone(), &to_combine, bk_set) {
        return TryFoldResult { folded: Some(folded), poisoned: vec![] };
    }
    try_fold_one_by_one_checked(attestation_data, to_combine, bk_set)
}

fn try_fold_all_optimistic(
    attestation_data: AttestationData,
    to_combine: &[(HashMap<SignerIndex, u16>, Signature)],
    bk_set: &BlockKeeperSet,
) -> Option<Envelope<GoshBLS, AttestationData>> {
    let mut signatures = vec![];
    let mut aggregated_signature_occurences = HashMap::<SignerIndex, u16>::new();
    for (signature_occurences, signature) in to_combine.iter() {
        for (k, v) in signature_occurences {
            aggregated_signature_occurences.entry(*k).and_modify(|e| *e += v).or_insert(*v);
        }
        signatures.push(signature.clone());
    }
    let Ok(aggregated_signature) = GoshBLS::merge_all(&signatures) else {
        return None;
    };
    let envelope =
        Envelope::create(aggregated_signature, aggregated_signature_occurences, attestation_data);
    if let Ok(true) = envelope.verify_signatures(bk_set.get_pubkeys_by_signers()) {
        Some(envelope)
    } else {
        None
    }
}

fn try_fold_one_by_one_checked(
    attestation_data: AttestationData,
    to_combine: Vec<(HashMap<SignerIndex, u16>, Signature)>,
    bk_set: &BlockKeeperSet,
) -> TryFoldResult {
    let mut folded: Option<Envelope<GoshBLS, AttestationData>> = None;
    let mut poisoned = vec![];
    for (signature_occurences, signature) in to_combine.into_iter() {
        let envelope = Envelope::create(
            signature.clone(),
            signature_occurences.clone(),
            attestation_data.clone(),
        );
        match envelope.verify_signatures(bk_set.get_pubkeys_by_signers()) {
            Ok(true) => {
                if let Some(e) = folded.as_mut() {
                    let Ok(aggregated_signature) =
                        GoshBLS::merge(e.aggregated_signature(), &signature)
                    else {
                        tracing::trace!("Signature merge failed");
                        poisoned.push(envelope);
                        continue;
                    };
                    let mut aggregated_signature_occurences = e.clone_signature_occurrences();
                    for (k, v) in signature_occurences {
                        aggregated_signature_occurences
                            .entry(k)
                            .and_modify(|e| *e += v)
                            .or_insert(v);
                    }
                    *e = Envelope::create(
                        aggregated_signature,
                        aggregated_signature_occurences,
                        attestation_data.clone(),
                    );
                } else {
                    folded = Some(envelope);
                }
            }
            Ok(false) => {
                poisoned.push(envelope);
            }
            Err(e) => {
                tracing::trace!("Was not able to verify an attestation: {envelope:?}, error: {e}");
                poisoned.push(envelope);
            }
        }
    }
    TryFoldResult { folded, poisoned }
}
