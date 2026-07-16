pub(crate) mod accumulator;
pub mod archive;
pub(crate) mod kv_store;
mod legacy;
mod maps;
pub(crate) mod repository;

use std::fmt::Display;

pub use accumulator::AnchorBlockRef;
pub use accumulator::FlushGuard;
pub use accumulator::PinHandle;
pub use accumulator::PinRequestGuard;
pub use archive::config::ArchiveStoreConfig;
pub use archive::control::ThreadControlState;
pub use archive::control::UpdatePhase;
pub use archive::store::ArchiveStateStore;
pub use archive::store::StagedArchiveEpoch;
pub use archive::update::ThreadSnapshot;
pub use kv_store::KVRecord;
pub use kv_store::KVStore;
pub use legacy::ensure_legacy_durable_snapshot_is_empty;
pub use maps::DurableThreadAccountsIter;
pub use maps::ThreadAccountMap;
pub use maps::ThreadAccountMapRepository;
use multi_map::MultiMapValue;
use node_types::AccountHash;
use node_types::AccountRouting;
use node_types::Blake3Hashable;
use node_types::DAppIdentifier;
pub use repository::AccountHashMismatchError;
pub use repository::BlockAccountOperation;
pub use repository::DurableThreadAccountsRepository;
pub use repository::DurableThreadAccountsState;
pub use repository::DurableThreadAccountsStateDiff;
pub use repository::DurableThreadAccountsStateDiffSerDe;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AccountInfo {
    Redirect(DAppIdentifier),
    VmAccountHash(AccountHash),
}

impl Display for AccountInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountInfo::Redirect(dapp_id) => write!(f, "Redirect({})", dapp_id),
            AccountInfo::VmAccountHash(account_hash) => {
                write!(f, "{}", account_hash.to_hex_string())
            }
        }
    }
}

impl Blake3Hashable for AccountInfo {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        match self {
            AccountInfo::Redirect(dapp_id) => {
                hasher.update(dapp_id.as_slice());
            }
            AccountInfo::VmAccountHash(account_hash) => {
                hasher.update(account_hash.as_slice());
            }
        }
    }
}

/// Blanket impl: every MapValue is also a MultiMapValue (uses bincode for serialization).
impl MultiMapValue for AccountInfo {
    fn write_value<W: std::io::Write>(&self, w: &mut W) -> anyhow::Result<()> {
        match self {
            AccountInfo::Redirect(dapp_id) => {
                w.write_all(&[1u8])?;
                w.write_all(dapp_id.as_slice())?;
            }
            AccountInfo::VmAccountHash(account_hash) => {
                w.write_all(&[2u8])?;
                w.write_all(account_hash.as_slice())?;
            }
        }
        Ok(())
    }

    fn read_value<R: std::io::Read>(r: &mut R) -> anyhow::Result<Self> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        Ok(match tag[0] {
            1u8 => {
                let mut dapp_id_bytes = [0u8; 32];
                r.read_exact(&mut dapp_id_bytes)?;
                AccountInfo::Redirect(DAppIdentifier::new(dapp_id_bytes))
            }
            2u8 => {
                let mut account_hash_bytes = [0u8; 32];
                r.read_exact(&mut account_hash_bytes)?;
                AccountInfo::VmAccountHash(AccountHash::new(account_hash_bytes))
            }
            _ => anyhow::bail!("Unknown account info tag: {}", tag[0]),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DurableStateSnapshot {
    pub maps: Vec<u8>,
    pub accounts: Vec<(AccountRouting, Vec<u8>)>,
}
