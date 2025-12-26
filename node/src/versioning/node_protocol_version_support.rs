use std::fmt;
use std::str::FromStr;

use derive_getters::Getters;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::ProtocolVersion;
use crate::versioning::protocol_version::ProtocolVersionHash;

// Each node running MUST support at least one version of a protocol.

#[derive(Debug, Clone, PartialEq, Eq, Hash, Getters)]
pub struct ProtocolVersionSupport {
    base: ProtocolVersion,
    status: ProtocolVersionSupportStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProtocolVersionSupportStatus {
    Current,
    TransitioningTo(ProtocolVersion),
}

impl ProtocolVersionSupport {
    pub fn new(base: ProtocolVersion) -> Self {
        Self { base, status: ProtocolVersionSupportStatus::Current }
    }

    pub fn transitioning_to(mut self, target: ProtocolVersion) -> Self {
        self.status = ProtocolVersionSupportStatus::TransitioningTo(target);
        self
    }

    /// Whether both refer to the same base version (ignoring status).
    pub fn same_base(&self, other: &Self) -> bool {
        self.base == other.base
    }

    /// Convenience: returns Some(target) if TransitioningTo, else None.
    pub fn target(&self) -> Option<&ProtocolVersion> {
        match &self.status {
            ProtocolVersionSupportStatus::Current => None,
            ProtocolVersionSupportStatus::TransitioningTo(v) => Some(v),
        }
    }

    pub fn is_transitioning(&self) -> bool {
        match &self.status {
            ProtocolVersionSupportStatus::Current => false,
            ProtocolVersionSupportStatus::TransitioningTo(_) => true,
        }
    }

    pub fn supports_version(&self, protocol_version_hash: &ProtocolVersionHash) -> bool {
        let bash_hash = self.base.hash();
        let next_version_hash = match &self.status {
            ProtocolVersionSupportStatus::Current => None,
            ProtocolVersionSupportStatus::TransitioningTo(v) => Some(v.hash()),
        };
        &bash_hash == protocol_version_hash
            || next_version_hash
                .is_some_and(|next_version_hash| &next_version_hash == protocol_version_hash)
    }

    pub fn is_none(&self) -> bool {
        match &self.status {
            ProtocolVersionSupportStatus::Current => {
                self.base == ProtocolVersion::parse("None").unwrap()
            }
            ProtocolVersionSupportStatus::TransitioningTo(_) => false,
        }
    }

    pub fn as_tuple(&self) -> (&ProtocolVersion, Option<&ProtocolVersion>) {
        match &self.status {
            ProtocolVersionSupportStatus::Current => (&self.base, None),
            ProtocolVersionSupportStatus::TransitioningTo(target) => (&self.base, Some(target)),
        }
    }
}

impl fmt::Display for ProtocolVersionSupport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.status {
            ProtocolVersionSupportStatus::Current => write!(f, "{}", self.base),
            ProtocolVersionSupportStatus::TransitioningTo(target) => {
                write!(f, "{}->{}", self.base, target)
            }
        }
    }
}

impl FromStr for ProtocolVersionSupport {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((base_str, target_str)) = s.split_once("->") {
            let base = ProtocolVersion::parse(base_str.trim())
                .map_err(|e| anyhow::format_err!("invalid base semver: {e}"))?;
            let target = ProtocolVersion::parse(target_str.trim())
                .map_err(|e| anyhow::format_err!("invalid target semver: {e}"))?;
            Ok(ProtocolVersionSupport {
                base,
                status: ProtocolVersionSupportStatus::TransitioningTo(target),
            })
        } else {
            let base = ProtocolVersion::parse(s.trim())
                .map_err(|e| anyhow::format_err!("invalid protocol version: {e}"))?;
            Ok(ProtocolVersionSupport { base, status: ProtocolVersionSupportStatus::Current })
        }
    }
}

// Serde via the same compact string form as Display/FromStr
impl Serialize for ProtocolVersionSupport {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for ProtocolVersionSupport {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        ProtocolVersionSupport::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_matrix;

    use super::*;

    #[test_matrix([
        "0.12.0",
        "0.12.0->0.14.0",
        "0x123.0->oadh01c.1"
    ])]
    fn ensure_protocol_version_to_string_and_parse_result_in_the_same_value(version: &str) {
        let support = ProtocolVersionSupport::from_str(version).unwrap();
        assert_eq!(version, support.to_string());
    }
}
