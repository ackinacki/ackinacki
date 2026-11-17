// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::SimpleObject;
use faster_hex::hex_string;

use crate::schema::db::latest_bk_set::LatestBkSetSnapshot;

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct BlockchainBkSetSigner {
    pub signer_index: u32,
    /// BLS public key, lowercase hex (96 characters).
    pub pubkey: String,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct BlockchainLatestBkSet {
    pub thread_id: String,
    pub seq_no: u64,
    pub block_id: String,
    pub height: u64,
    pub chain_order: String,
    pub updated_at: u64,
    pub signers: Vec<BlockchainBkSetSigner>,
}

impl From<LatestBkSetSnapshot> for BlockchainLatestBkSet {
    fn from(value: LatestBkSetSnapshot) -> Self {
        Self {
            thread_id: value.thread_id,
            seq_no: value.seq_no,
            block_id: value.block_id,
            height: value.height,
            chain_order: value.chain_order,
            updated_at: value.updated_at,
            signers: value
                .signers
                .into_iter()
                .map(|(signer_index, pubkey)| BlockchainBkSetSigner {
                    signer_index,
                    pubkey: hex_string(&pubkey),
                })
                .collect(),
        }
    }
}
