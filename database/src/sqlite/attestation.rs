// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ArchAttestation {
    pub block_id: String,
    pub parent_block_id: String,
    pub envelope_hash: [u8; 32],
    pub target_type: u8,
    pub aggregated_signature: Vec<u8>,
    pub signature_occurrences: Vec<u8>,
    pub source_block_id: String,
    pub source_chain_order: String,
}
