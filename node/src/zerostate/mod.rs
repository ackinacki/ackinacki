// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod parse;
pub mod serialize;
mod update;

use std::collections::HashMap;

use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_block::ShardStateUnsplit;

use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::gosh_bls::PubKey;
use crate::node::SignerIndex;
use crate::zerostate::serialize::WrappedZeroState;

#[derive(Debug, Default)]
pub struct ZeroState {
    pub shard_state: ShardStateUnsplit,
    pub block_keeper_set: BlockKeeperSet,
}

impl ZeroState {
    pub fn get_block_keeper_ring_pubkeys(&self) -> HashMap<SignerIndex, PubKey> {
        self.block_keeper_set.clone().into_iter().map(|(k, v)| (k, v.pubkey)).collect()
    }

    fn wrap_serialize(&self) -> WrappedZeroState {
        WrappedZeroState {
            shard_state_data: self.shard_state.write_to_bytes().unwrap(),
            block_keeper_set: self.block_keeper_set.clone(),
        }
    }

    fn wrap_deserialize(data: WrappedZeroState) -> Self {
        Self {
            shard_state: ShardStateUnsplit::construct_from_bytes(&data.shard_state_data).unwrap(),
            block_keeper_set: data.block_keeper_set,
        }
    }
}
