pub mod block_protocol_version_state;
pub mod canonical_config_hash;
pub mod node_protocol_version_support;
pub mod protocol_version;
pub use node_protocol_version_support::ProtocolVersionSupport;
pub use node_protocol_version_support::ProtocolVersionSupportStatus;
pub use protocol_version::ProtocolVersion;

#[cfg(test)]
mod tests {
    use std::str::FromStr;

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
// ----------
// Examples / quick tests
// ----------
// use ProtocolVersion;
// use std::str::FromStr;
//
// let v = ProtocolVersionSupport::new(Version::new(1,2,3));
// assert_eq!(v.to_string(), "1.2.3");
// let t = ProtocolVersionSupport::new(Version::new(1,2,3)).transitioning_to(Version::new(2,0,0));
// assert_eq!(t.to_string(), "1.2.3->2.0.0");
//
// let parsed = "1.2.3->2.0.0".parse::<ProtocolVersionSupport>().unwrap();
// assert!(matches!(parsed.status(), ProtocolVersionSupportStatus::TransitioningTo(_)));
// assert_eq!(parsed.target().unwrap(), &Version::new(2,0,0));
//
// let json = serde_json::to_string(&parsed).unwrap();
// assert_eq!(json, "\"1.2.3->2.0.0\"");
// let back: ProtocolVersionSupport = serde_json::from_str(&json).unwrap();
// assert_eq!(back, parsed);
