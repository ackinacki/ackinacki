// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use num_bigint::BigUint;
use serde::Deserialize;
use serde::Serialize;
use tvm_types::AccountId;

use crate::bls::gosh_bls::PubKey;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::AccountAddress;

pub mod abi;
pub mod epoch;
pub mod wallet_config;

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
    pub wallet_index: NodeIdentifier,
    pub pubkey: PubKey,
    pub epoch_finish_timestamp: u32,
    pub status: BlockKeeperStatus,
    pub address: String,
    pub stake: BigUint,
    pub owner_address: AccountAddress,
    pub signer_index: SignerIndex,
}

pub type BlockKeeperSet = HashMap<SignerIndex, BlockKeeperData>;

impl Debug for BlockKeeperData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self.pubkey))
    }
}

impl Display for BlockKeeperData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockKeeperData")
            .field("wallet_index", &self.wallet_index)
            .field("pubkey", &self.pubkey)
            .field("epoch_finish_timestamp", &self.epoch_finish_timestamp)
            .field("address", &self.address)
            .field("stake", &self.stake)
            .field("owner_address", &self.owner_address)
            .field("signer_index", &self.signer_index)
            .finish()
    }
}

#[derive(Clone)]
pub struct BlockKeeperSlashData {
    pub node_id: NodeIdentifier,
    pub bls_pubkey: PubKey,
    pub addr: AccountId,
    pub slash_type: u8,
}

impl Debug for BlockKeeperSlashData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockKeeperSlashData")
            .field("node_id", &self.node_id)
            .field("bls_pubkey", &self.bls_pubkey)
            .field("address", &self.addr)
            .field("slash_type", &self.slash_type)
            .finish()
    }
}
