//! Compatibility entry points for the channel-driven Tokio snapshot actors.
//! Local and legacy import retain their codecs and staged-commit boundaries.
use std::path::Path;

use account_state::DurableThreadAccountsRepository;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use url::Url;

use super::snapshot_consumption::SnapshotImportLimits;
use super::snapshot_consumption::StagedPartitionedSnapshot;
use super::snapshot_partitioned::PrimaryThreadSnapshot;
use super::thread_snapshot_import::BlobDownloadLimits;
use crate::node::NodeIdentifier;

#[derive(Clone, Copy)]
pub(crate) struct SnapshotFetchLimits {
    pub concurrent_downloads: usize,
}

impl Default for SnapshotFetchLimits {
    fn default() -> Self {
        Self { concurrent_downloads: 4 }
    }
}

pub(crate) struct SnapshotHttpImport<'a> {
    pub repository: &'a DurableThreadAccountsRepository,
    pub cache_directory: &'a Path,
    pub expected: (ThreadIdentifier, BlockIdentifier),
    #[cfg(test)]
    pub primary_sources: &'a [Url],
    pub known_sources: &'a [(NodeIdentifier, Url)],
    /// Explicitly configured/current-BK bases, tried after ranked publishers.
    pub fallback_sources: &'a [Url],
    pub download_limits: BlobDownloadLimits,
    pub import_limits: SnapshotImportLimits,
    pub fetch_limits: SnapshotFetchLimits,
}

#[cfg(test)]
pub(crate) fn download_and_stage_snapshot<'a, C, V>(
    request: SnapshotHttpImport<'a>,
    verify_primary: V,
    cancel: &C,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>>
where
    C: Fn() -> bool + Sync,
    V: FnMut(&PrimaryThreadSnapshot) -> anyhow::Result<()>,
{
    validate_fetch_limits(&request)?;
    let sources = request.primary_sources.to_vec();
    super::thread_snapshot_import::import_snapshot(request, None, sources, verify_primary, cancel)
}

/// Runtime's legacy/partitioned format dispatcher has already decoded and
/// checked the primary. Reuse it, rather than redownload or decode it twice.
pub(crate) fn stage_snapshot_partitions<'a, C: Fn() -> bool + Sync>(
    request: SnapshotHttpImport<'a>,
    primary: PrimaryThreadSnapshot,
    cancel: &C,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>> {
    validate_fetch_limits(&request)?;
    super::thread_snapshot_import::import_snapshot(
        request,
        Some(primary),
        Vec::new(),
        |_| Ok(()),
        cancel,
    )
}

fn validate_fetch_limits(request: &SnapshotHttpImport<'_>) -> anyhow::Result<()> {
    let limits = request.fetch_limits;
    anyhow::ensure!(limits.concurrent_downloads > 0, "snapshot fetch limits must be non-zero");
    anyhow::ensure!(
        request.download_limits.rounds > 0,
        "snapshot download limits must be non-zero"
    );
    anyhow::ensure!(
        request.import_limits.accounts_per_batch > 0
            && request.import_limits.account_bytes_per_batch > 0,
        "invalid snapshot import batch limits"
    );
    for base in request.fallback_sources {
        anyhow::ensure!(
            matches!(base.scheme(), "http" | "https")
                && base.host_str().is_some()
                && base.username().is_empty()
                && base.password().is_none()
                && base.query().is_none()
                && base.fragment().is_none()
                && base.path().ends_with('/'),
            "invalid fallback snapshot source"
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_http_import_tests.rs"]
pub(crate) mod tests;
