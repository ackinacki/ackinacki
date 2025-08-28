use derive_getters::Getters;
use typed_builder::TypedBuilder;

use crate::node::associated_types::AttestationTargetType;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;

#[derive(Clone, Debug, Getters, TypedBuilder, Eq, PartialEq, Ord, PartialOrd)]
pub struct CompactedMapKey {
    block_seq_no: BlockSeqNo,
    block_identifier: BlockIdentifier,
    attestation_target_type: AttestationTargetType,
}
