// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::domain::grouping::ArchiveFile;
use crate::domain::traits::FileSystemClient;

/// Mock file system client for testing without filesystem I/O.
///
/// Allows pre-populating files and tracking calls.
#[allow(unused)]
#[derive(Clone)]
pub struct MockFileSystemClient {
    files: BTreeMap<String, Vec<ArchiveFile>>,
    move_processed_calls: Arc<Mutex<Vec<(PathBuf, PathBuf)>>>,
}

impl MockFileSystemClient {
    pub fn new() -> Self {
        MockFileSystemClient {
            files: BTreeMap::new(),
            move_processed_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[cfg(test)]
    pub fn with_files(mut self, files: BTreeMap<String, Vec<ArchiveFile>>) -> Self {
        self.files = files;
        self
    }

    #[cfg(test)]
    pub fn move_processed_calls(&self) -> Vec<(PathBuf, PathBuf)> {
        self.move_processed_calls.lock().unwrap().clone()
    }
}

impl Default for MockFileSystemClient {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemClient for MockFileSystemClient {
    fn get_arch_files(&self) -> anyhow::Result<BTreeMap<String, Vec<ArchiveFile>>> {
        Ok(self.files.clone())
    }

    fn move_processed(
        &self,
        src_db_path: impl AsRef<Path>,
        processed_root: impl AsRef<Path>,
        _gzip: bool,
        _dry_run: bool,
    ) -> anyhow::Result<PathBuf> {
        let src = src_db_path.as_ref().to_path_buf();
        let dest = processed_root.as_ref().join(src.file_name().unwrap());
        self.move_processed_calls.lock().unwrap().push((src.clone(), dest.clone()));
        Ok(dest)
    }
}
