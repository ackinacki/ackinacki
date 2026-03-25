// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ArchBkSetUpdate {
    pub block_id: String,
    pub thread_id: String,
    pub height: [u8; 8],
    pub chain_order: String,
    pub bk_set_update: Vec<u8>,
}
