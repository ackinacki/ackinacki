//! Stable operation tags; retired tag 2 is rejected rather than reused.
use serde::de::EnumAccess;
use serde::de::VariantAccess;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::BlockAccountOperation;

struct OperationTag(u32);

impl<'de> Deserialize<'de> for OperationTag {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TagVisitor;
        impl serde::de::Visitor<'_> for TagVisitor {
            type Value = OperationTag;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an account operation name or tag")
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
                u32::try_from(value).map(OperationTag).map_err(E::custom)
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                match value {
                    "UpdateOrInsert" => Ok(OperationTag(0)),
                    "Remove" => Ok(OperationTag(1)),
                    "AccountMerkleUpdate" => Ok(OperationTag(3)),
                    _ => Err(E::unknown_variant(
                        value,
                        &["UpdateOrInsert", "Remove", "AccountMerkleUpdate"],
                    )),
                }
            }
        }
        deserializer.deserialize_identifier(TagVisitor)
    }
}

impl Serialize for BlockAccountOperation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::UpdateOrInsert(account) => serializer.serialize_newtype_variant(
                "BlockAccountOperation",
                0,
                "UpdateOrInsert",
                account,
            ),
            Self::Remove => serializer.serialize_unit_variant("BlockAccountOperation", 1, "Remove"),
            Self::AccountMerkleUpdate(bytes) => serializer.serialize_newtype_variant(
                "BlockAccountOperation",
                3,
                "AccountMerkleUpdate",
                bytes,
            ),
        }
    }
}

impl<'de> Deserialize<'de> for BlockAccountOperation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = BlockAccountOperation;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an account operation (tag 0, 1 or 3)")
            }

            fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
                let (OperationTag(tag), value) = data.variant()?;
                match (tag, value) {
                    (0, value) => {
                        value.newtype_variant().map(BlockAccountOperation::UpdateOrInsert)
                    }
                    (1, value) => {
                        value.unit_variant()?;
                        Ok(BlockAccountOperation::Remove)
                    }
                    (3, value) => {
                        value.newtype_variant().map(BlockAccountOperation::AccountMerkleUpdate)
                    }
                    (tag, _) => Err(serde::de::Error::custom(format!(
                        "unsupported account operation tag {tag}"
                    ))),
                }
            }
        }
        deserializer.deserialize_enum(
            "BlockAccountOperation",
            &["UpdateOrInsert", "Remove", "AccountMerkleUpdate"],
            Visitor,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_operation_is_rejected_and_merkle_update_keeps_tag() {
        assert!(bincode::deserialize::<BlockAccountOperation>(&2u32.to_le_bytes()).is_err());
        let op = BlockAccountOperation::AccountMerkleUpdate(vec![1, 2]);
        let wire = bincode::serialize(&op).unwrap();
        assert_eq!(&wire[..4], &3u32.to_le_bytes());
        assert_eq!(bincode::deserialize::<BlockAccountOperation>(&wire).unwrap(), op);
    }
}
