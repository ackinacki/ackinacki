//! Node-local download cache, independent of published snapshot retention.
//!
//! One manager per canonical directory is shared by primary and partition
//! downloads. Reservations include private temporary files; accepted files stay
//! reusable until LRU eviction. A lease protects a file through validation and
//! consumption. Never pin a whole manifest: it may be larger than the budget.
//! The directory is owned by one node process. On first open, recover regular
//! 64-hex files and discard only known temporary-file names (never symlinks).
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use std::time::SystemTime;

use parking_lot::Condvar;
use parking_lot::Mutex;

pub(crate) const DEFAULT_DOWNLOAD_CACHE_BYTES: u64 = 50 * 1024 * 1024 * 1024;
pub(crate) const TEMP_PREFIX: &str = ".snapshot-part-";

#[derive(Clone)]
pub(crate) struct DownloadCache(Arc<Inner>);

struct Inner {
    directory: PathBuf,
    budget: u64,
    state: Mutex<State>,
    changed: Condvar,
    import: Mutex<()>,
}

#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    bytes: u64,
}

struct Entry {
    bytes: u64,
    used: SystemTime,
    active: bool,
    persisted: bool,
}

pub(crate) struct CacheLease {
    cache: DownloadCache,
    id: String,
}

impl DownloadCache {
    /// `None` reuses the configured manager, or defaults for isolated callers.
    pub(crate) fn open(directory: &Path, budget: Option<u64>) -> anyhow::Result<Self> {
        static MANAGERS: OnceLock<Mutex<HashMap<PathBuf, Weak<Inner>>>> = OnceLock::new();
        std::fs::create_dir_all(directory)?;
        let directory = directory.canonicalize()?;
        let mut managers = MANAGERS.get_or_init(Mutex::default).lock();
        if let Some(inner) = managers.get(&directory).and_then(Weak::upgrade) {
            anyhow::ensure!(
                budget.is_none_or(|value| value == inner.budget),
                "download cache budget changed while in use"
            );
            return Ok(Self(inner));
        }
        let budget = budget.unwrap_or(DEFAULT_DOWNLOAD_CACHE_BYTES);
        anyhow::ensure!(budget > 0, "snapshot download cache budget must be positive");
        let mut state = State::default();
        for item in std::fs::read_dir(&directory)? {
            let item = item?;
            if !item.file_type()?.is_file() {
                continue;
            }
            let name = item.file_name().to_string_lossy().into_owned();
            if name.starts_with(TEMP_PREFIX) || name.starts_with(".tmp") {
                std::fs::remove_file(item.path())?;
            } else if snapshot_storage::valid_id(&name) {
                let metadata = item.metadata()?;
                state.bytes = state
                    .bytes
                    .checked_add(metadata.len())
                    .ok_or_else(|| anyhow::anyhow!("download cache size overflow"))?;
                state.entries.insert(
                    name,
                    Entry {
                        bytes: metadata.len(),
                        used: metadata.modified()?,
                        active: false,
                        persisted: true,
                    },
                );
            }
        }
        let cache = Self(Arc::new(Inner {
            directory: directory.clone(),
            budget,
            state: Mutex::new(state),
            changed: Condvar::new(),
            import: Mutex::new(()),
        }));
        {
            let mut state = cache.0.state.lock();
            cache.evict_to_fit(&mut state, 0)?;
        }
        managers.retain(|_, value| value.strong_count() > 0);
        managers.insert(directory, Arc::downgrade(&cache.0));
        Ok(cache)
    }

    pub(crate) fn budget(&self) -> u64 {
        self.0.budget
    }

    /// Run at service startup, before any import or publication can use files.
    /// Older nodes downloaded monolithic snapshots into the publication root
    /// without discovery headers. Move only recognized legacy bodies into the
    /// bounded cache; never treat headerless new-format partitions as downloads.
    pub(super) fn migrate_legacy_downloads(&self, directory: &Path) -> anyhow::Result<()> {
        let mut candidates = Vec::new();
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let id = entry.file_name().to_string_lossy().into_owned();
            if !snapshot_storage::valid_id(&id)
                || !entry.file_type()?.is_file()
                || std::fs::symlink_metadata(directory.join(format!("{id}.header"))).is_ok()
            {
                continue;
            }
            let mut file = File::open(entry.path())?;
            let mut envelope = [0; 4];
            if file.read_exact(&mut envelope).is_err() {
                continue;
            }
            let marker = if &envelope == snapshot_storage::chunks::MAGIC {
                let Ok(mut decoder) = zstd::Decoder::new(file) else { continue };
                decoder.window_log_max(27)?;
                let mut marker = [0; 7];
                if decoder.read_exact(&mut marker).is_err() {
                    continue;
                }
                marker
            } else {
                let mut marker = [0; 7];
                if std::io::Cursor::new(envelope).chain(file).read_exact(&mut marker).is_err() {
                    continue;
                }
                marker
            };
            if &marker == b"SNAP2V1" {
                candidates.push((entry.metadata()?.modified()?, id, entry.path()));
            }
        }
        candidates.sort();
        for (_, id, source) in candidates {
            let bytes = std::fs::metadata(&source)?.len();
            if bytes > self.budget() {
                // A cache cannot admit this file. It has no discovery header
                // and no active readers at startup, so discard the old download.
                std::fs::remove_file(&source)?;
                tracing::info!(%id, bytes, "discarded legacy download exceeding cache budget");
                continue;
            }
            let lease = self.acquire(&id, bytes, &|| false)?;
            std::fs::rename(&source, lease.path())?;
            lease.accepted()?;
            tracing::info!(%id, bytes, "moved legacy download into bounded snapshot cache");
        }
        File::open(directory)?.sync_all()?;
        File::open(&self.0.directory)?.sync_all()?;
        Ok(())
    }

    /// Admit a primary download and its complete import callback together.
    /// Call before taking any cache lease or repository lock: queued primaries
    /// must not pin space while the active import waits for partition capacity.
    /// Partition prefetch inside the callback does not acquire this gate.
    pub(crate) fn admit_import<C: Fn() -> bool>(
        &self,
        cancel: &C,
    ) -> anyhow::Result<parking_lot::MutexGuard<'_, ()>> {
        loop {
            anyhow::ensure!(!cancel(), "snapshot import admission cancelled");
            if let Some(guard) = self.0.import.try_lock_for(Duration::from_millis(25)) {
                anyhow::ensure!(!cancel(), "snapshot import admission cancelled");
                return Ok(guard);
            }
        }
    }

    pub(crate) fn acquire<C: Fn() -> bool>(
        &self,
        id: &str,
        maximum: u64,
        cancel: &C,
    ) -> anyhow::Result<CacheLease> {
        anyhow::ensure!(snapshot_storage::valid_id(id), "invalid download cache id");
        match std::fs::symlink_metadata(self.0.directory.join(id)) {
            Ok(metadata) => anyhow::ensure!(
                metadata.file_type().is_file(),
                "download cache entry is not a regular file"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut state = self.0.state.lock();
        loop {
            anyhow::ensure!(!cancel(), "download cache wait cancelled");
            if let Some(entry) = state.entries.get_mut(id) {
                if !entry.active {
                    let previous = entry.bytes;
                    let reserved = maximum.max(previous);
                    anyhow::ensure!(
                        reserved <= self.0.budget,
                        "download reservation exceeds snapshot download cache budget"
                    );
                    entry.active = true;
                    let fits = self.evict_to_fit(&mut state, reserved - previous);
                    if !fits.as_ref().is_ok_and(|fits| *fits) {
                        state.entries.get_mut(id).expect("cache entry").active = false;
                    }
                    if fits? {
                        state.bytes += reserved - previous;
                        let entry = state.entries.get_mut(id).expect("leased entry");
                        entry.bytes = reserved;
                        entry.used = SystemTime::now();
                        return Ok(CacheLease { cache: self.clone(), id: id.to_owned() });
                    }
                }
            } else {
                anyhow::ensure!(
                    maximum <= self.0.budget,
                    "download reservation exceeds snapshot download cache budget"
                );
                if self.evict_to_fit(&mut state, maximum)? {
                    state.bytes += maximum;
                    state.entries.insert(
                        id.to_owned(),
                        Entry {
                            bytes: maximum,
                            used: SystemTime::now(),
                            active: true,
                            persisted: false,
                        },
                    );
                    return Ok(CacheLease { cache: self.clone(), id: id.to_owned() });
                }
            }
            self.0.changed.wait_for(&mut state, Duration::from_millis(25));
        }
    }

    fn evict_to_fit(&self, state: &mut State, extra: u64) -> anyhow::Result<bool> {
        while state.bytes > self.0.budget - extra {
            let oldest = state
                .entries
                .iter()
                .filter(|(_, entry)| !entry.active)
                .min_by_key(|(id, entry)| (entry.used, *id))
                .map(|(id, _)| id.clone());
            let Some(id) = oldest else {
                return Ok(false);
            };
            match std::fs::remove_file(self.0.directory.join(&id)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            state.bytes -= state.entries.remove(&id).expect("evicted entry").bytes;
        }
        Ok(true)
    }
}

impl CacheLease {
    pub(crate) fn budget(&self) -> u64 {
        self.cache.budget()
    }

    /// Actor workers must return disk pressure to their coordinator rather than
    /// all sleeping with resumable representations pinned. The coordinator can
    /// discard inactive partials or wait for the importer to release ready files.
    pub(crate) fn try_resize(&self, bytes: u64) -> anyhow::Result<bool> {
        anyhow::ensure!(
            bytes <= self.cache.0.budget,
            "snapshot blob exceeds download cache budget"
        );
        let mut state = self.cache.0.state.lock();
        let previous = state.entries[&self.id].bytes;
        if !self.cache.evict_to_fit(&mut state, bytes.saturating_sub(previous))? {
            return Ok(false);
        }
        state.bytes = state.bytes - previous + bytes;
        state.entries.get_mut(&self.id).expect("leased entry").bytes = bytes;
        self.cache.0.changed.notify_all();
        Ok(true)
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.cache.0.directory.join(&self.id)
    }

    pub(crate) fn resize<C: Fn() -> bool>(&self, bytes: u64, cancel: &C) -> anyhow::Result<()> {
        anyhow::ensure!(
            bytes <= self.cache.0.budget,
            "snapshot blob exceeds download cache budget"
        );
        let mut state = self.cache.0.state.lock();
        loop {
            anyhow::ensure!(!cancel(), "download cache wait cancelled");
            let previous = state.entries[&self.id].bytes;
            let extra = bytes.saturating_sub(previous);
            if self.cache.evict_to_fit(&mut state, extra)? {
                state.bytes = state.bytes - previous + bytes;
                state.entries.get_mut(&self.id).expect("leased entry").bytes = bytes;
                self.cache.0.changed.notify_all();
                return Ok(());
            }
            self.cache.0.changed.wait_for(&mut state, Duration::from_millis(25));
        }
    }

    pub(crate) fn accepted(&self) -> anyhow::Result<()> {
        let file = File::open(self.path())?;
        let now = SystemTime::now();
        // Persist recency without per-blob sidecars or relying on mount atime.
        file.set_modified(now)?;
        self.resize(file.metadata()?.len(), &|| false)?;
        let mut state = self.cache.0.state.lock();
        let entry = state.entries.get_mut(&self.id).expect("leased entry");
        entry.used = now;
        entry.persisted = true;
        Ok(())
    }

    pub(crate) fn invalidate(&self) -> anyhow::Result<()> {
        match std::fs::remove_file(self.path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.cache.0.state.lock().entries.get_mut(&self.id).expect("leased entry").persisted =
            false;
        Ok(())
    }
}

impl Drop for CacheLease {
    fn drop(&mut self) {
        let mut state = self.cache.0.state.lock();
        if state.entries[&self.id].persisted {
            state.entries.get_mut(&self.id).expect("leased entry").active = false;
        } else {
            // A publish/sync error can leave a destination behind. Remove it
            // before releasing its reservation, not only the private tempfile.
            match std::fs::remove_file(self.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!("cannot discard snapshot cache file {}: {error}", self.id);
                    let entry = state.entries.get_mut(&self.id).expect("leased entry");
                    entry.persisted = true;
                    entry.active = false;
                    self.cache.0.changed.notify_all();
                    return;
                }
            }
            state.bytes -= state.entries.remove(&self.id).expect("leased entry").bytes;
        }
        self.cache.0.changed.notify_all();
    }
}

/// Close a completely consumed primary before its callback starts partition
/// downloads. Otherwise many concurrent primaries could pin the entire budget
/// while all their import callbacks wait for partition reservations.
pub(super) struct CacheReader {
    file: Option<File>,
    lease: Option<CacheLease>,
}

impl CacheReader {
    pub(super) fn new(lease: CacheLease) -> anyhow::Result<Self> {
        let file = File::open(lease.path())?;
        Ok(Self { file: Some(file), lease: Some(lease) })
    }
}

impl std::io::Read for CacheReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let Some(file) = self.file.as_mut() else {
            return Ok(0);
        };
        let count = std::io::Read::read(file, buffer)?;
        if count == 0 {
            self.file.take();
            self.lease.take();
        }
        Ok(count)
    }
}

#[cfg(test)]
#[path = "download_cache_tests.rs"]
mod tests;
