//! Runtime dispatch under the existing trusted-publisher snapshot model.
//! Request/protocol/root checks are not a consensus-authenticated state commitment.
use std::io::Cursor;
use std::io::Read;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use url::Url;

use super::snapshot_compression::COMPRESSED_SNAPSHOT_MAGIC;
use super::snapshot_compression::PARTITION_WINDOW_LOG;
use crate::node::NodeIdentifier;
use crate::repository::repository_impl::snapshot_consumption::SnapshotImportLimits;
use crate::repository::repository_impl::snapshot_http_import::stage_snapshot_partitions;
use crate::repository::repository_impl::snapshot_http_import::SnapshotFetchLimits;
use crate::repository::repository_impl::snapshot_http_import::SnapshotHttpImport;
use crate::repository::repository_impl::snapshot_manifest::MAX_SNAPSHOT_ROOTS_PER_LIST;
use crate::repository::repository_impl::snapshot_partitioned::read_primary_snapshot_checked;
use crate::repository::repository_impl::snapshot_partitioned::MAGIC;
use crate::repository::repository_impl::thread_snapshot_import::BlobDownloadLimits;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::repository_impl::SnapshotImportStatus;
use crate::types::BlockHeight;

#[allow(clippy::too_many_arguments)] // Explicit worker inputs; no additional runtime state.
pub(crate) fn import_downloaded_snapshot<C: Fn() -> bool + Sync>(
    input: &mut dyn Read,
    repository: &mut RepositoryImpl,
    expected: (ThreadIdentifier, BlockIdentifier),
    anchor: BlockHeight,
    cache: &Path,
    known: &[(NodeIdentifier, Url)],
    fallbacks: &[Url],
    cancel: &C,
) -> anyhow::Result<SnapshotImportStatus> {
    anyhow::ensure!(!cancel(), "snapshot candidate cancelled");
    let mut envelope = [0; 4];
    input.read_exact(&mut envelope)?;
    anyhow::ensure!(
        &envelope == COMPRESSED_SNAPSHOT_MAGIC,
        "unsupported snapshot compression envelope"
    );
    let input = snapshot_storage::chunks::PayloadReader::after_magic(input);
    let mut decoder = zstd::Decoder::with_buffer(input)?;
    decoder.window_log_max(PARTITION_WINDOW_LOG)?;
    let mut marker = [0; 7];
    decoder.read_exact(&mut marker)?;
    anyhow::ensure!(&marker == MAGIC, "unsupported snapshot format; SNAP2V2 required");
    let mut decoder = decoder.single_frame();
    let primary = read_primary_snapshot_checked(
        &mut Cursor::new(marker).chain(&mut decoder),
        MAX_SNAPSHOT_ROOTS_PER_LIST,
        cancel,
    )?;
    decoder.finish().verify_trailer()?;
    anyhow::ensure!(
        (primary.metadata.state.thread_id, primary.metadata.state.block_id) == expected,
        "snapshot request mismatch"
    );
    if anchor.thread_identifier() == &expected.0 {
        anyhow::ensure!(primary.metadata.block_height == anchor, "snapshot anchor height mismatch");
    }
    anyhow::ensure!(
        primary.metadata.block_protocol_version_state.to_use().to_string() != "None",
        "missing snapshot block protocol"
    );
    crate::repository::repository_impl::snapshot_manifest::partition_capacity_for_block(
        &primary.metadata.block_protocol_version_state,
        &repository.config_read(),
    )?;
    // Source trust is inherited from the worker's configured/current-BK URLs.
    // Do not claim to verify signatures or a block-committed manifest here.
    std::fs::create_dir_all(cache)?;
    let download_limits = BlobDownloadLimits {
        rounds: 2,
        connect_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(120),
        retry_delay: Duration::from_secs(2),
        deadline: Instant::now() + Duration::from_secs(3600),
    };
    let accounts = repository.thread_accounts_repository().clone();
    let staged = stage_snapshot_partitions(
        SnapshotHttpImport {
            repository: accounts.durable_map_repo(),
            cache_directory: cache,
            expected,
            #[cfg(test)]
            primary_sources: &[],
            known_sources: known,
            fallback_sources: fallbacks,
            download_limits,
            import_limits: SnapshotImportLimits::default(),
            fetch_limits: SnapshotFetchLimits::default(),
        },
        primary,
        cancel,
    )?;
    repository.install_partitioned_snapshot(staged, &expected.0, cancel)
}
