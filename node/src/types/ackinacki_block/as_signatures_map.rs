use std::collections::HashMap;
use std::collections::HashSet;

use node_types::BlockIdentifier;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::AttestationTargetType;
use crate::types::ackinacki_block::SignerIndex;

pub trait AsSignaturesMap {
    fn as_signatures_map(
        &self,
    ) -> HashMap<(BlockIdentifier, AttestationTargetType), HashSet<SignerIndex>>;
}

impl AsSignaturesMap for Vec<Envelope<AttestationData>> {
    fn as_signatures_map(
        &self,
    ) -> HashMap<(BlockIdentifier, AttestationTargetType), HashSet<SignerIndex>> {
        let mut attestations_map =
            HashMap::<(BlockIdentifier, AttestationTargetType), HashSet<SignerIndex>>::new();
        for attestation in self.iter() {
            let attestation_target =
                (*attestation.data().block_id(), *attestation.data().target_type());
            let attestation_signers =
                HashSet::from_iter(attestation.clone_signature_occurrences().keys().cloned());
            attestations_map
                .entry(attestation_target)
                .and_modify(|e| e.extend(attestation_signers.clone()))
                .or_insert(attestation_signers);
        }
        attestations_map
    }
}
