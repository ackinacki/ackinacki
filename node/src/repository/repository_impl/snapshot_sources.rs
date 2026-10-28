//! Consumer-side preferences over the same frozen publisher schedule.
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use url::Url;

use super::snapshot_manifest::ThreadSnapshotManifest;
use super::snapshot_schedule::manifest_partition_count;
use super::snapshot_schedule::partition_publication_position;
use super::snapshot_schedule::rank_snapshot_publishers;
use crate::block_keeper_system::BlockKeeperSet;
use crate::node::NodeIdentifier;

pub(crate) struct SnapshotSourcePlan {
    // Missing endpoints remain empty slots: never rerank a filtered/live set.
    publishers: Vec<Vec<Url>>,
    partition_count: usize,
    blob_ids: Vec<[u8; 32]>,
    account_count: usize,
}

impl SnapshotSourcePlan {
    /// Construct after bounded primary decoding. The downloader may prefetch
    /// before the importer accepts primary, but never installs state on that
    /// basis: primary trust/protocol checks precede all archive writes. Endpoints
    /// must already be known/configured and end in '/', normally /v2/storage/.
    /// Publisher membership comes from primary BK metadata, not the peer list. Unknown peers
    /// are ignored; multiple endpoints per committed node are retained/deduped.
    pub(crate) fn new(
        manifest: &ThreadSnapshotManifest,
        bk: &BlockKeeperSet,
        known: impl IntoIterator<Item = (NodeIdentifier, Url)>,
    ) -> anyhow::Result<Self> {
        let ranked = rank_snapshot_publishers(bk, manifest)?;
        let members: BTreeSet<_> = ranked.iter().cloned().collect();
        let mut endpoints = BTreeMap::<_, BTreeSet<_>>::new();
        let mut owners = BTreeMap::new();
        for (node, url) in known {
            if !members.contains(&node) {
                continue;
            }
            anyhow::ensure!(matches!(url.scheme(), "http" | "https") && url.host().is_some()
                && url.path().ends_with('/') && url.query().is_none() && url.fragment().is_none()
                && url.username().is_empty() && url.password().is_none(),
                "snapshot endpoint must be an HTTP(S) directory URL without credentials/query/fragment");
            if let Some(owner) = owners.insert(url.clone(), node.clone()) {
                anyhow::ensure!(owner == node, "snapshot endpoint belongs to multiple node IDs");
            }
            endpoints.entry(node).or_default().insert(url);
        }
        let publishers = ranked
            .into_iter()
            .map(|node| endpoints.remove(&node).unwrap_or_default().into_iter().collect())
            .collect();
        let partition_count = manifest_partition_count(manifest);
        let blob_ids = (0..partition_count)
            .map(|index| manifest.partition_blob_id(index).expect("manifest index"))
            .collect();
        Ok(Self {
            publishers,
            partition_count,
            blob_ids,
            account_count: manifest.account_partition_roots.len(),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.partition_count
    }

    pub(crate) fn blob_id(&self, index: usize) -> Option<[u8; 32]> {
        self.blob_ids.get(index).copied()
    }

    pub(crate) fn partition_path(&self, primary: &[u8; 32], index: usize) -> String {
        snapshot_storage::partition_path(
            primary,
            &self.blob_ids[index],
            if index < self.account_count {
                snapshot_storage::PartitionKind::Accounts
            } else {
                snapshot_storage::PartitionKind::Messages
            },
        )
    }

    /// Earlier preparation position wins; frozen node rank breaks ties. Endpoint
    /// order is canonical and independent of discovery order. For N committed
    /// nodes, P partitions and E known endpoints, cost O(N log P + E log E), memory O(E); no P*N
    /// per-node permutations are materialized.
    pub(crate) fn partition_sources(&self, index: usize) -> anyhow::Result<Vec<Url>> {
        let blob_id = self
            .blob_id(index)
            .ok_or_else(|| anyhow::anyhow!("partition index outside manifest"))?;
        let mut sources = Vec::new();
        for (rank, endpoints) in self.publishers.iter().enumerate() {
            let position = partition_publication_position(
                self.partition_count,
                rank,
                self.publishers.len(),
                index,
            )?;
            for endpoint in endpoints {
                sources.push((position, rank, endpoint));
            }
        }
        sources.sort_unstable_by(|a, b| (a.0, a.1, a.2.as_str()).cmp(&(b.0, b.1, b.2.as_str())));
        sources.into_iter().map(|(_, _, base)| Ok(base.join(&hex::encode(blob_id))?)).collect()
    }
}

#[cfg(test)]
#[path = "snapshot_sources_tests.rs"]
mod tests;
