use anyhow::ensure;
use node_types::AccountHash;
use node_types::DAppIdentifier;
use node_types::TransactionHash;

use crate::DAppAccountMapHash;

type LegacyNodeId = u32;

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct LegacyMapKey([u8; 32]);

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct LegacyMapKeyPath {
    prefix: LegacyMapKey,
    len: u8,
}

#[repr(C)]
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct LegacyNode {
    hash: [u8; 32],
    tag: u8,
    _pad: [u8; 3],
    a: u32,
    b: u32,
    c: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(bound(serialize = "V: serde::Serialize", deserialize = "V: serde::de::DeserializeOwned"))]
struct LegacyTrieMapSnapshot<V> {
    nodes: Vec<LegacyNode>,
    values: Vec<V>,
    branch_children: Vec<LegacyNodeId>,
    ext_paths: Vec<u8>,
    root: LegacyNodeId,
    root_path: LegacyMapKeyPath,
}

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct LegacyAccountInfo {
    last_trans_hash: TransactionHash,
    last_trans_lt: u64,
    account_hash: AccountHash,
    redirect_dapp_id: Option<DAppIdentifier>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct LegacyCompositeDurableStateSnapshot {
    thread_dapps: LegacyTrieMapSnapshot<DAppAccountMapHash>,
    dapp_accounts: Vec<(DAppAccountMapHash, LegacyTrieMapSnapshot<LegacyAccountInfo>)>,
    accounts: Vec<(AccountHash, Vec<u8>)>,
}

pub fn ensure_legacy_durable_snapshot_is_empty(bytes: &[u8]) -> anyhow::Result<()> {
    let snapshot: LegacyCompositeDurableStateSnapshot = bincode::deserialize(bytes)
        .map_err(|e| anyhow::format_err!("Failed to deserialize legacy durable snapshot: {e}"))?;

    ensure!(
        snapshot.accounts.is_empty(),
        "Non-empty legacy durable snapshots are not supported: found {} account blobs",
        snapshot.accounts.len()
    );
    ensure!(
        snapshot.dapp_accounts.is_empty(),
        "Non-empty legacy durable snapshots are not supported: found {} dapp account maps",
        snapshot.dapp_accounts.len()
    );
    ensure!(
        is_empty_trie_snapshot(&snapshot.thread_dapps),
        "Non-empty legacy durable snapshots are not supported: thread_dapps trie is not empty"
    );

    Ok(())
}

fn is_empty_trie_snapshot<V>(snapshot: &LegacyTrieMapSnapshot<V>) -> bool {
    snapshot.root == 0
        && snapshot.root_path.len == 0
        && snapshot.root_path.prefix.0 == [0; 32]
        && snapshot.values.is_empty()
        && snapshot.branch_children.is_empty()
        && snapshot.ext_paths.is_empty()
        && snapshot.nodes.len() == 1
        && snapshot.nodes[0].tag == 0
        && snapshot.nodes[0].a == 0
        && snapshot.nodes[0].b == 0
        && snapshot.nodes[0].c == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_thread_dapps_snapshot() -> LegacyTrieMapSnapshot<DAppAccountMapHash> {
        LegacyTrieMapSnapshot {
            nodes: vec![LegacyNode { hash: [0; 32], tag: 0, _pad: [0; 3], a: 0, b: 0, c: 0 }],
            values: Vec::new(),
            branch_children: Vec::new(),
            ext_paths: Vec::new(),
            root: 0,
            root_path: LegacyMapKeyPath { prefix: LegacyMapKey([0; 32]), len: 0 },
        }
    }

    fn empty_legacy_snapshot_bytes() -> Vec<u8> {
        bincode::serialize(&LegacyCompositeDurableStateSnapshot {
            thread_dapps: empty_thread_dapps_snapshot(),
            dapp_accounts: Vec::new(),
            accounts: Vec::new(),
        })
        .unwrap()
    }

    #[test]
    fn test_legacy_durable_empty_snapshot_is_accepted() {
        let bytes = empty_legacy_snapshot_bytes();
        ensure_legacy_durable_snapshot_is_empty(&bytes).unwrap();
    }

    #[test]
    fn test_legacy_durable_non_empty_snapshot_is_rejected() {
        let bytes = bincode::serialize(&LegacyCompositeDurableStateSnapshot {
            thread_dapps: empty_thread_dapps_snapshot(),
            dapp_accounts: Vec::new(),
            accounts: vec![(AccountHash::new([1; 32]), vec![1, 2, 3])],
        })
        .unwrap();

        let err = ensure_legacy_durable_snapshot_is_empty(&bytes).unwrap_err();
        assert!(err.to_string().contains("Non-empty legacy durable snapshots are not supported"));
    }

    #[test]
    fn test_legacy_durable_non_canonical_empty_trie_is_rejected() {
        let mut thread_dapps = empty_thread_dapps_snapshot();
        thread_dapps.root_path = LegacyMapKeyPath { prefix: LegacyMapKey([1; 32]), len: 1 };

        let bytes = bincode::serialize(&LegacyCompositeDurableStateSnapshot {
            thread_dapps,
            dapp_accounts: Vec::new(),
            accounts: Vec::new(),
        })
        .unwrap();

        let err = ensure_legacy_durable_snapshot_is_empty(&bytes).unwrap_err();
        assert!(err.to_string().contains("thread_dapps trie is not empty"));
    }
}
