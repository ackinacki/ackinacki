// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use num_bigint::BigUint;
use serde::Deserialize;
use serde::Serialize;

use crate::bls::gosh_bls::PubKey;
use crate::node::SignerIndex;

mod abi;
pub mod epoch;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum BlockKeeperSetChange {
    BlockKeeperAdded((SignerIndex, BlockKeeperData)),
    BlockKeeperRemoved((SignerIndex, BlockKeeperData)),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum BlockKeeperStatus {
    Active,
    CalledToFinish,
    Expired,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BlockKeeperData {
    pub index: SignerIndex,
    pub pubkey: PubKey,
    pub epoch_finish_timestamp: u32,
    pub status: BlockKeeperStatus,
    pub address: String,
    pub stake: BigUint,
}

pub type BlockKeeperSet = HashMap<SignerIndex, BlockKeeperData>;

impl Debug for BlockKeeperData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockKeeperData")
            .field("index", &self.index)
            .field("pubkey", &hex::encode(&self.pubkey))
            .field("epoch_finish_timestamp", &self.epoch_finish_timestamp)
            .field("status", &self.status)
            .field("address", &self.address)
            .field("stake", &self.stake)
            .finish()
    }
}

pub fn get_block_keeper_ring_pubkeys(
    block_keeper_set: &BlockKeeperSet,
) -> HashMap<SignerIndex, PubKey> {
    block_keeper_set.clone().into_iter().map(|(k, v)| (k, v.pubkey)).collect()
}
