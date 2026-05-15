use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;

use crate::thread_accounts::durable::maps::ThreadAccountMap;
use crate::StateAccountsMetrics;
use crate::ThreadAccountMapRepository;

pub struct MerkleMapStore {
    store_path: PathBuf,
    map_repo: ThreadAccountMapRepository,
    metrics: Option<StateAccountsMetrics>,
}

impl MerkleMapStore {
    pub fn new(
        durable_path: impl Into<PathBuf>,
        maps: ThreadAccountMapRepository,
        metrics: Option<StateAccountsMetrics>,
    ) -> Self {
        Self { store_path: durable_path.into().join("maps"), map_repo: maps, metrics }
    }

    /// Recover from checkpoint + WAL and seed the running map_state.
    /// Must be called before the first `apply_and_append`.
    pub fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn _read(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Option<ThreadAccountMap>> {
        let path = self.map_path(block_id, thread_id);
        if !std::path::Path::new(&path).exists() {
            return Ok(None);
        }
        let f = File::open(&path)?;
        let mut r = BufReader::with_capacity(512 * 1024, f);
        let map = self.map_repo.map_read(&mut r)?;
        Ok(Some(map))
    }

    pub fn append(
        &self,
        maps: HashMap<(BlockIdentifier, ThreadIdentifier), ThreadAccountMap>,
    ) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        fs::create_dir_all(&self.store_path)?;
        for ((block_id, thread_id), map) in maps {
            let path = self.map_path(&block_id, &thread_id);
            let tmp = path.with_extension("tmp");
            {
                let f = File::create(&tmp)?;
                let mut w = BufWriter::with_capacity(512 * 1024, f);
                self.map_repo.map_write(&map, &mut w)?;
                w.flush()?;
                w.into_inner()?.sync_all()?;
            }
            fs::rename(&tmp, &path)?;
        }
        if let Some(metrics) = &self.metrics {
            metrics.report_merkle_cache_write_duration_ms(started.elapsed().as_millis() as u64);
        }
        Ok(())
    }

    pub fn list(&self) -> HashSet<(BlockIdentifier, ThreadIdentifier)> {
        let Ok(entries) = fs::read_dir(&self.store_path) else {
            return HashSet::new();
        };
        let mut maps = HashSet::new();
        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            let Some(path) = entry.path().file_name().map(|x| x.to_string_lossy().to_string())
            else {
                continue;
            };
            let Some((block_str, thread_str)) = path.split_once('_') else {
                continue;
            };
            let Ok(block_id) = BlockIdentifier::from_str(block_str) else {
                continue;
            };
            let Ok(thread_id) = ThreadIdentifier::try_from(thread_str.to_string()) else {
                continue;
            };
            maps.insert((block_id, thread_id));
        }
        maps
    }

    pub fn remove(&self, maps: HashSet<(BlockIdentifier, ThreadIdentifier)>) {
        for (block_id, thread_id) in maps {
            let path = self.map_path(&block_id, &thread_id);
            if let Err(err) = fs::remove_file(&path) {
                tracing::error!(target: "monit", "Failed to remove map file: {}", err);
            }
        }
    }

    fn map_path(&self, block_id: &BlockIdentifier, thread_id: &ThreadIdentifier) -> PathBuf {
        self.store_path.join(format!("{}_{}", block_id.to_hex_string(), thread_id.to_hex_string()))
    }
}
