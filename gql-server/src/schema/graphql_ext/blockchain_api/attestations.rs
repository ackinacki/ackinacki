// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use async_graphql::Enum;
use async_graphql::Json;
use async_graphql::SimpleObject;
use faster_hex::hex_string;
use serde_json::Map;
use serde_json::Value;

use crate::schema::db;

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum AttestationTargetType {
    Primary,
    Fallback,
}

impl TryFrom<i64> for AttestationTargetType {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Primary),
            1 => Ok(Self::Fallback),
            _ => anyhow::bail!("unknown attestation target_type: {value}"),
        }
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct BlockAttestation {
    pub block_id: String,
    pub parent_block_id: String,
    pub envelope_hash: String,
    pub target_type: AttestationTargetType,
    pub aggregated_signature: String,
    pub signature_occurrences: Json<Value>,
}

impl BlockAttestation {
    pub fn try_from_db(value: db::attestation::Attestation) -> anyhow::Result<Self> {
        let signature_occurrences: HashMap<u16, u16> =
            bincode::deserialize(&value.signature_occurrences)?;
        let mut map = Map::new();
        for (signer_id, position) in signature_occurrences {
            map.insert(signer_id.to_string(), Value::from(position));
        }

        Ok(Self {
            block_id: value.block_id,
            parent_block_id: value.parent_block_id,
            envelope_hash: hex_string(&value.envelope_hash),
            target_type: value.target_type.try_into()?,
            aggregated_signature: hex_string(&value.aggregated_signature),
            signature_occurrences: Json(Value::Object(map)),
        })
    }
}
