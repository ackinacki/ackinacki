use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::types::AckiNackiBlock;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(transparent)]
pub struct AckiNackiEnvelopeHash(pub [u8; 32]);

impl Debug for AckiNackiEnvelopeHash {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "AckiNackiEnvelopeHash<{}>", hex::encode(self.0))
    }
}

pub fn envelope_hash(envelope: &Envelope<GoshBLS, AckiNackiBlock>) -> AckiNackiEnvelopeHash {
    let mut hasher = Sha256::new();
    let data = bincode::serialize(envelope).unwrap();

    hasher.update(data);
    let res = hasher.finalize().into();
    AckiNackiEnvelopeHash(res)
}
