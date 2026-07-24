// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::domain::grouping::ArchiveFile;
use crate::domain::traits::CompressionMode;
use crate::domain::traits::FileSystemClient;

/// Mock file system client for testing without filesystem I/O.
///
/// Allows pre-populating files and tracking calls.
#[allow(unused)]
#[derive(Clone)]
pub struct MockFileSystemClient {
    files: BTreeMap<String, Vec<ArchiveFile>>,
    move_processed_calls: Arc<Mutex<Vec<(PathBuf, PathBuf)>>>,
    remove_file_calls: Arc<Mutex<Vec<PathBuf>>>,
}

#[allow(dead_code)]
impl MockFileSystemClient {
    #[cfg(test)]
    pub fn new() -> Self {
        MockFileSystemClient {
            files: BTreeMap::new(),
            move_processed_calls: Arc::new(Mutex::new(Vec::new())),
            remove_file_calls: Arc::new(Mutex::new(Vec::new())),
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

    #[cfg(test)]
    pub fn remove_file_calls(&self) -> Vec<PathBuf> {
        self.remove_file_calls.lock().unwrap().clone()
    }
}

#[cfg(test)]
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
        _compression: CompressionMode,
        _dry_run: bool,
    ) -> anyhow::Result<PathBuf> {
        let src = src_db_path.as_ref().to_path_buf();
        let dest = processed_root.as_ref().join(src.file_name().unwrap());
        self.move_processed_calls.lock().unwrap().push((src.clone(), dest.clone()));
        Ok(dest)
    }

    fn remove_file(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        self.remove_file_calls.lock().unwrap().push(path.as_ref().to_path_buf());
        Ok(())
    }
}
