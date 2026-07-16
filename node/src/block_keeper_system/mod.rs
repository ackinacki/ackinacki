// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::AddAssign;

use node_types::AccountIdentifier;
use num_bigint::BigUint;
use num_traits::Zero;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use typed_builder::TypedBuilder;

use crate::bls::gosh_bls::PubKey;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;

pub mod abi;
pub mod bk_set;
pub mod epoch;
pub mod from_api_bk;
pub mod wallet_config;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum BlockKeeperSetChange {
    BlockKeeperAdded((SignerIndex, BlockKeeperData)),
    BlockKeeperRemoved((SignerIndex, BlockKeeperData)),
    FutureBlockKeeperAdded((SignerIndex, BlockKeeperData)),
    #[cfg(feature = "protocol_version_hash_in_block")]
    BlockKeeperChangedVersion((SignerIndex, BlockKeeperData)),
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
    pub wait_step: u64,
    pub status: BlockKeeperStatus,
    pub address: String,
    pub stake: BigUint,
    /// Address of the block keeper wallet.
    /// Also known as NodeIdentifier.
    pub owner_address: AccountIdentifier,
    pub signer_index: SignerIndex,
    pub owner_pubkey: [u8; 32],
    pub protocol_support: crate::versioning::ProtocolVersionSupport,
}

#[cfg(test)]
impl Default for BlockKeeperData {
    fn default() -> Self {
        use std::str::FromStr;
        BlockKeeperData {
            pubkey: PubKey::default(),
            epoch_finish_seq_no: None,
            wait_step: 0,
            status: BlockKeeperStatus::Active,
            address: "".to_string(),
            stake: BigUint::zero(),
            owner_address: AccountIdentifier::default(),
            signer_index: SignerIndex::default(),
            owner_pubkey: [0; 32],
            protocol_support: crate::versioning::ProtocolVersionSupport::from_str("test").unwrap(),
        }
    }
}

impl BlockKeeperData {
    pub fn node_id(&self) -> NodeIdentifier {
        self.owner_address.into()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, TypedBuilder, derive_getters::Getters)]
pub struct BlockKeeperSetTransitionHashes {
    old_bk_set_hash: [u8; 32],
    new_bk_set_hash: [u8; 32],
}

impl Debug for BlockKeeperSetTransitionHashes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}->{}", hex::encode(self.old_bk_set_hash), hex::encode(self.new_bk_set_hash))
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BlockKeeperSet {
    by_signer: HashMap<SignerIndex, BlockKeeperData>,
    signer_to_pubkey: HashMap<SignerIndex, PubKey>,
    signer_by_node_id: BTreeMap<NodeIdentifier, SignerIndex>,
}

impl Display for BlockKeeperSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        for (k, v) in &self.by_signer {
            write!(f, "{k}: {v}, ")?;
        }
        write!(f, "}}")?;
        Ok(())
    }
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

    pub fn insert(
        &mut self,
        signer_index: SignerIndex,
        keeper: BlockKeeperData,
    ) -> Option<BlockKeeperData> {
        let node_id = keeper.node_id();
        self.signer_to_pubkey.insert(signer_index, keeper.pubkey.clone());
        let data = self.by_signer.insert(signer_index, keeper);
        self.signer_by_node_id.insert(node_id, signer_index);
        data
    }

    pub fn contains_signer(&self, signer_index: &SignerIndex) -> bool {
        self.by_signer.contains_key(signer_index)
    }

    pub fn contains_node(&self, node_id: &NodeIdentifier) -> bool {
        self.signer_by_node_id.contains_key(node_id)
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

    pub fn iter(&self) -> impl Iterator<Item = &BlockKeeperData> {
        self.by_signer.values()
    }

    pub fn remove_signer(&mut self, signer_index: &SignerIndex) -> Option<BlockKeeperData> {
        let removed = self.by_signer.remove(signer_index);
        self.signer_to_pubkey.remove(signer_index);
        if let Some(keeper) = &removed {
            self.signer_by_node_id.remove(&keeper.node_id());
        }
        removed
    }

    /// Compute Poseidon commitment to this BK set.
    ///
    /// Algorithm (matching the bridge circuit spec):
    /// 1. Sort signers by index (ascending)
    /// 2. For each signer: extract pubkey x-coordinate → 5 CRT limbs (104-bit each)
    /// 3. Pad to MAX_SIGNERS (300) with sentinel entries (index=0xFFFF, zero x-limbs)
    /// 4. Feed all Fr elements into Poseidon sponge (T=3, RATE=2, R_F=8, R_P=57)
    ///
    /// Returns 32-byte LE Poseidon hash.
    pub fn poseidon_commitment(&self) -> anyhow::Result<[u8; 32]> {
        use tvm_vm::executor::zk_stuff::bn254::poseidon::PoseidonSponge;

        const LIMB_BITS: usize = 104;
        const NUM_LIMBS: usize = 5;
        const MAX_SIGNERS: usize = 300;
        const PADDING_SIGNER_INDEX: u16 = 0xFFFF;

        let mut sorted_keys: Vec<SignerIndex> = self.by_signer.keys().cloned().collect();
        sorted_keys.sort();
        let actual_size = sorted_keys.len();

        let limb_mask = (BigUint::from(1u64) << LIMB_BITS) - 1u64;
        let mut poseidon_input: Vec<Vec<u8>> = Vec::with_capacity(MAX_SIGNERS * (1 + NUM_LIMBS));

        #[allow(clippy::needless_range_loop)]
        for k in 0..MAX_SIGNERS {
            if k < actual_size {
                let idx = sorted_keys[k];
                // signer_index as Fr (32 bytes LE)
                let mut idx_buf = [0u8; 32];
                idx_buf[..2].copy_from_slice(&idx.to_le_bytes());
                poseidon_input.push(idx_buf.to_vec());

                // Extract x-coordinate from compressed BLS pubkey and split into CRT limbs
                let pk_bytes = self.by_signer[&idx].pubkey.as_ref().to_bytes();
                let x_bigint = extract_bls_x_coordinate_be(&pk_bytes);

                for i in 0..NUM_LIMBS {
                    let limb_val = (&x_bigint >> (i * LIMB_BITS)) & &limb_mask;
                    let limb_bytes = limb_val.to_bytes_le();
                    let mut buf = [0u8; 32];
                    let len = limb_bytes.len().min(32);
                    buf[..len].copy_from_slice(&limb_bytes[..len]);
                    poseidon_input.push(buf.to_vec());
                }
            } else {
                // Padding entry: sentinel index + zero x-limbs
                let mut idx_buf = [0u8; 32];
                idx_buf[..2].copy_from_slice(&PADDING_SIGNER_INDEX.to_le_bytes());
                poseidon_input.push(idx_buf.to_vec());
                for _ in 0..NUM_LIMBS {
                    poseidon_input.push(vec![0u8; 32]);
                }
            }
        }

        let sponge = PoseidonSponge::new();
        sponge
            .hash_bytes_axiom(&poseidon_input)
            .map_err(|e| anyhow::anyhow!("Poseidon hash failed: {:?}", e))
    }
}

/// Extract x-coordinate from a 48-byte big-endian compressed BLS12-381 G1 public key.
///
/// BLS12-381 compressed format (48 bytes, big-endian):
/// - Top 3 bits of byte 0 are flags (compressed, infinity, y-sign)
/// - Remaining 381 bits are the x-coordinate in big-endian
///
/// We clear the flag bits and convert to a BigUint (little-endian).
/// This produces the same x value as halo2curves `G1Affine::from_compressed_be`
/// followed by `.x.to_bytes()` (LE), which is what bridge-poseidon does.
fn extract_bls_x_coordinate_be(compressed_pk: &[u8]) -> BigUint {
    assert_eq!(compressed_pk.len(), 48, "BLS public key must be 48 bytes");
    let mut x_bytes_be = [0u8; 48];
    x_bytes_be.copy_from_slice(compressed_pk);
    // Clear the top 3 flag bits (bits 7,6,5 of byte 0)
    x_bytes_be[0] &= 0x1F;
    BigUint::from_bytes_be(&x_bytes_be)
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
        f.write_str(&format!("({:?},{:?})", self.owner_address, self.pubkey))
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
            .field("version", &self.protocol_support)
            .finish()
    }
}

#[derive(Clone)]
pub struct BlockKeeperSlashData {
    pub node_id: NodeIdentifier,
    pub bls_pubkey: PubKey,
    pub addr: AccountIdentifier,
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
