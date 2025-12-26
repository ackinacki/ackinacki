use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;

use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use super::canonical_config_hash::CanonicalConfigHash;

#[derive(TypedBuilder)]
#[builder(
    build_method(vis="pub", into=ProtocolVersion),
    builder_method(vis=""),
    builder_type(vis="pub", name=ProtocolVersionBuilder)
)]
pub struct ProtocolVersionBase {
    #[builder(setter(into))]
    canonical_config_hash: CanonicalConfigHash,
    tvm_engine_version: semver::Version,
    gossip_version: u16,
    // TODO:
    // node_software_version: (Major, Minor)
}

impl Display for ProtocolVersionBase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.canonical_config_hash, self.tvm_engine_version, self.gossip_version
        )
    }
}

// Note:
// It is intentionally has no fields as it can be anything.
// The only intended way to use it is to compare it with
// the protocol version supported by the node itself.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProtocolVersion(String);

impl ProtocolVersion {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        Ok(ProtocolVersion(text.to_string()))
    }

    pub fn builder() -> ProtocolVersionBuilder {
        ProtocolVersionBase::builder()
    }

    pub fn hash(&self) -> ProtocolVersionHash {
        use md5::Digest;
        let mut hasher = md5::Md5::new();
        hasher.update(self.0.as_bytes());
        hasher.finalize().to_vec().into()
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<ProtocolVersionBase> for ProtocolVersion {
    fn from(base: ProtocolVersionBase) -> Self {
        ProtocolVersion(format!("{base}"))
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ProtocolVersionHash(Vec<u8>);

impl From<Vec<u8>> for ProtocolVersionHash {
    fn from(v: Vec<u8>) -> Self {
        ProtocolVersionHash(v)
    }
}

impl Display for ProtocolVersionHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl Debug for ProtocolVersionHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}
