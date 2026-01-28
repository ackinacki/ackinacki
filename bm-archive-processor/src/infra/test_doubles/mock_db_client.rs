// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::domain::traits::DbClient;

/// Mock database client for testing without database I/O.
///
/// Tracks calls and allows verification of interaction contracts.
#[allow(unused)]
#[derive(Clone)]
pub struct MockDbClient {
    create_daily_calls: Arc<Mutex<Vec<(Vec<PathBuf>, PathBuf)>>>,
    merge_calls: Arc<Mutex<Vec<(PathBuf, PathBuf)>>>,
}

impl MockDbClient {
    pub fn new() -> Self {
        MockDbClient {
            create_daily_calls: Arc::new(Mutex::new(Vec::new())),
            merge_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[cfg(test)]
    pub fn create_daily_calls(&self) -> Vec<(Vec<PathBuf>, PathBuf)> {
        self.create_daily_calls.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub fn merge_calls(&self) -> Vec<(PathBuf, PathBuf)> {
        self.merge_calls.lock().unwrap().clone()
    }
}

impl Default for MockDbClient {
    fn default() -> Self {
        Self::new()
    }
}

impl DbClient for MockDbClient {
    fn create_daily_db(&self, src_paths: &[PathBuf], dst_path: &Path) -> anyhow::Result<()> {
        self.create_daily_calls.lock().unwrap().push((src_paths.to_vec(), dst_path.to_path_buf()));
        Ok(())
    }

    fn merge_daily_into_full(&self, src_db: &Path, target_db: &Path) -> anyhow::Result<()> {
        self.merge_calls.lock().unwrap().push((src_db.to_path_buf(), target_db.to_path_buf()));
        Ok(())
    }
}
