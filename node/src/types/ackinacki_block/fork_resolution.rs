use std::sync::Arc;

use derive_getters::Getters;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;

// Note: Single fork resolition contains information required to resolve a single fork.
// It can also be empty in case of a BP could not gather all required number of attestations
// for block finalization, yet it had enougth attestations to declare victory in any fork.
#[derive(Serialize, Deserialize, Clone, Debug, Getters, TypedBuilder, PartialEq)]
pub struct ForkResolution {
    parent_block_identifier: BlockIdentifier,
    winner: BlockIdentifier,
    winner_attestations: Envelope<GoshBLS, AttestationData>,
    // lost candidates
    other_forks_block_envelopes: Vec<Arc<Envelope<GoshBLS, AckiNackiBlock>>>,
    // attestations for the lost candidates
    lost_attestations: Vec<Envelope<GoshBLS, AttestationData>>,
    // nacks for lost candidates
    nacked_forks: Vec<Envelope<GoshBLS, NackData>>,
}
