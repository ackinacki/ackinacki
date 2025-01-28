use std::collections::HashMap;
use std::collections::HashSet;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::types::ackinacki_block::SignerIndex;
use crate::types::BlockIdentifier;

pub trait AsSignaturesMap {
    fn as_signatures_map(&self) -> HashMap<BlockIdentifier, HashSet<SignerIndex>>;
}

impl AsSignaturesMap for Vec<Envelope<GoshBLS, AttestationData>> {
    fn as_signatures_map(&self) -> HashMap<BlockIdentifier, HashSet<SignerIndex>> {
        let mut attestations_map = HashMap::<BlockIdentifier, HashSet<SignerIndex>>::new();
        for attestation in self.iter() {
            let attestation_target = attestation.data().block_id.clone();
            let attestation_signers =
                HashSet::from_iter(attestation.clone_signature_occurrences().keys().cloned());
            attestations_map
                .entry(attestation_target)
                .and_modify(|e| e.extend(attestation_signers.clone().into_iter()))
                .or_insert(attestation_signers);
        }
        attestations_map
    }
}
