use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;

use crate::domain::grouping::ArchiveFile;

pub type AnchorTimestamp = i64;

pub trait DbClient {
    fn create_daily_db(&self, src_paths: &[PathBuf], dst_path: &Path) -> anyhow::Result<()>;
    fn merge_daily_into_full(&self, src_db: &Path, target_db: &Path) -> anyhow::Result<()>;
}

pub trait FileSystemClient {
    /// Scans incoming/1..n, parses the files, and groups the database files by anchor within the specified window.
    fn get_arch_files(&self) -> anyhow::Result<BTreeMap<String, Vec<ArchiveFile>>>;

    /// Moves a processed database file to the archive directory, optionally compressing it with gzip.
    fn move_processed(
        &self,
        src_db_path: impl AsRef<Path>,
        processed_root: impl AsRef<Path>,
        gzip: bool,
        dry_run: bool,
    ) -> anyhow::Result<PathBuf>;
}

#[async_trait]
pub trait S3Client {
    async fn upload(&self, bucket: &str, key: &str, file_path: &Path) -> anyhow::Result<String>;
}
