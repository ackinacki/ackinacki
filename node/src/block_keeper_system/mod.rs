// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::AddAssign;

use num_bigint::BigUint;
use num_traits::Zero;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tvm_types::AccountId;

use crate::bls::gosh_bls::PubKey;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::AccountAddress;

pub mod abi;
pub mod bk_set;
pub mod bm_license;
pub mod epoch;
pub mod wallet_config;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum BlockKeeperSetChange {
    BlockKeeperAdded((SignerIndex, BlockKeeperData)),
    BlockKeeperRemoved((SignerIndex, BlockKeeperData)),
    FutureBlockKeeperAdded((SignerIndex, BlockKeeperData)),
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum BlockKeeperStatus {
    PreEpoch,
    Active,
    CalledToFinish,
    Expired,
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct BlockKeeperData {
    pub pubkey: PubKey,
    pub epoch_finish_seq_no: Option<u64>,
    pub status: BlockKeeperStatus,
    pub address: String,
    pub stake: BigUint,
    /// Address of the block keeper wallet.
    /// Also known as NodeIdentifier.
    pub owner_address: AccountAddress,
    pub signer_index: SignerIndex,
    pub owner_pubkey: [u8; 32],
}

#[cfg(test)]
impl Default for BlockKeeperData {
    fn default() -> Self {
        BlockKeeperData {
            pubkey: PubKey::default(),
            epoch_finish_seq_no: None,
            status: BlockKeeperStatus::Active,
            address: "".to_string(),
            stake: BigUint::zero(),
            owner_address: AccountAddress::default(),
            signer_index: SignerIndex::default(),
            owner_pubkey: [0; 32],
        }
    }
}

impl BlockKeeperData {
    pub fn node_id(&self) -> NodeIdentifier {
        self.owner_address.0.clone().into()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockKeeperSet {
    by_signer: HashMap<SignerIndex, BlockKeeperData>,
    signer_to_pubkey: HashMap<SignerIndex, PubKey>,
    signer_by_node_id: BTreeMap<NodeIdentifier, SignerIndex>,
}

impl BlockKeeperSet {
    pub fn into_values(self) -> impl Iterator<Item = BlockKeeperData> {
        self.by_signer.into_values()
    }
}

impl BlockKeeperSet {
    pub fn new() -> Self {
        Self {
            by_signer: HashMap::new(),
            signer_by_node_id: BTreeMap::new(),
            signer_to_pubkey: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_signer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_signer.is_empty()
    }

    pub fn insert(&mut self, signer_index: SignerIndex, keeper: BlockKeeperData) {
        let node_id = keeper.node_id();
        self.signer_to_pubkey.insert(signer_index, keeper.pubkey.clone());
        self.by_signer.insert(signer_index, keeper);
        self.signer_by_node_id.insert(node_id, signer_index);
    }

    pub fn contains_signer(&self, signer_index: &SignerIndex) -> bool {
        self.by_signer.contains_key(signer_index)
    }

    pub fn get_by_signer(&self, signer_index: &SignerIndex) -> Option<&BlockKeeperData> {
        self.by_signer.get(signer_index)
    }

    pub fn get_by_node_id(&self, node_id: &NodeIdentifier) -> Option<&BlockKeeperData> {
        self.signer_by_node_id.get(node_id).and_then(|x| self.by_signer.get(x))
    }

    pub fn get_pubkeys_by_signers(&self) -> &HashMap<SignerIndex, PubKey> {
        &self.signer_to_pubkey
    }

    pub fn get_undistributed_stake(&self, attested_signers: &HashSet<SignerIndex>) -> BigUint {
        let mut undistributed_stake = BigUint::zero();
        for (signer, keeper) in &self.by_signer {
            if !attested_signers.contains(signer) {
                undistributed_stake.add_assign(&keeper.stake);
            }
        }
        undistributed_stake
    }

    pub fn iter_node_ids(&self) -> impl Iterator<Item = &NodeIdentifier> {
        self.signer_by_node_id.keys()
    }

    pub fn remove_signer(&mut self, signer_index: &SignerIndex) -> Option<BlockKeeperData> {
        let removed = self.by_signer.remove(signer_index);
        self.signer_to_pubkey.remove(signer_index);
        if let Some(keeper) = &removed {
            self.signer_by_node_id.remove(&keeper.node_id());
        }
        removed
    }
}

impl Default for BlockKeeperSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for BlockKeeperSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.by_signer.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BlockKeeperSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let items = HashMap::<SignerIndex, BlockKeeperData>::deserialize(deserializer)?;
        let mut signer_to_pubkey = HashMap::new();
        let mut by_node_id = BTreeMap::new();
        for (signer, keeper) in &items {
            by_node_id.insert(keeper.node_id(), *signer);
            signer_to_pubkey.insert(*signer, keeper.pubkey.clone());
        }

        Ok(Self { by_signer: items, signer_by_node_id: by_node_id, signer_to_pubkey })
    }
}

impl Debug for BlockKeeperData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self.pubkey))
    }
}

impl Display for BlockKeeperData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockKeeperData")
            .field("node_id", &self.node_id())
            .field("pubkey", &self.pubkey)
            .field("epoch_finish_seq_no", &self.epoch_finish_seq_no.unwrap_or_default())
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
