// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::bail;
use lru::LruCache;
use parking_lot::Mutex;

use super::repository_impl::load_from_file;
use super::repository_impl::save_to_file;
use crate::repository::CrossThreadRefData;
use crate::types::BlockIdentifier;

pub trait CrossThreadRefDataRead {
    fn get_cross_thread_ref_data(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<CrossThreadRefData>;
}

// This is a solution for a multithreaded env Node Join operation.
// It does not track how long the history must be added to a shared
// state. (Hack) Instead it adds up to 100 blocks of history.
// Assuming it should be enough before the complete implementation added.
pub trait CrossThreadRefDataHistory {
    fn get_history_tail(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<Vec<CrossThreadRefData>>;
}

const CROSS_THREAD_REF_DATA_CACHE_SIZE: usize = 100;

#[derive(Clone)]
pub struct CrossThreadRefDataRepository {
    data_dir: PathBuf,
    cross_thread_ref_data_cache: Arc<Mutex<LruCache<BlockIdentifier, CrossThreadRefData>>>,
}

impl CrossThreadRefDataRead for CrossThreadRefDataRepository {
    fn get_cross_thread_ref_data(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<CrossThreadRefData> {
        let mut cross_thread_ref_data_cache = self.cross_thread_ref_data_cache.lock();
        if let Some(cross_thread_ref_data) = cross_thread_ref_data_cache.get(identifier) {
            return Ok(cross_thread_ref_data.clone());
        }
        let path = self.get_cross_thread_ref_data_path(identifier);
        let data: Option<CrossThreadRefData> = load_from_file(&path)
            .unwrap_or_else(|_| panic!("Failed to load file: {}", path.display()));
        if let Some(cross_thread_ref_data) = data {
            cross_thread_ref_data_cache.put(
                cross_thread_ref_data.block_identifier().clone(),
                cross_thread_ref_data.clone(),
            );
            Ok(cross_thread_ref_data)
        } else {
            bail!("cross thread ref data was not set {}", identifier)
        }
    }
}

impl CrossThreadRefDataHistory for CrossThreadRefDataRepository {
    fn get_history_tail(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<Vec<CrossThreadRefData>> {
        // Let's include the block data first. It must be there or fail.
        let mut history = vec![self.get_cross_thread_ref_data(identifier)?];
        let mut cursor = history.last().unwrap().parent_block_identifier().clone();
        for _ in 0..100 {
            let Ok(ref_data) = self.get_cross_thread_ref_data(&cursor) else {
                tracing::trace!("Missing cross-thread-ref-data. Continue as is. Possible if this node recently joined to the network");
                break;
            };
            history.push(ref_data.clone());
            if cursor == BlockIdentifier::default() {
                break;
            }
            cursor = ref_data.parent_block_identifier().clone();
        }
        Ok(history)
    }
}

impl CrossThreadRefDataRepository {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            cross_thread_ref_data_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(CROSS_THREAD_REF_DATA_CACHE_SIZE).unwrap(),
            ))),
        }
    }

    fn get_cross_thread_ref_data_path(&self, block_id: &BlockIdentifier) -> PathBuf {
        // hex format
        let oid = format!("{block_id:x}");
        self.data_dir.join("cross-thread-ref-data").join(oid)
    }

    pub fn set_cross_thread_ref_data(
        &mut self,
        cross_thread_ref_data: CrossThreadRefData,
    ) -> anyhow::Result<()> {
        let id = cross_thread_ref_data.block_identifier().clone();
        let path = self.get_cross_thread_ref_data_path(&id);
        save_to_file(&path, &cross_thread_ref_data)?;
        self.cross_thread_ref_data_cache.lock().put(id, cross_thread_ref_data);
        Ok(())
    }
}
