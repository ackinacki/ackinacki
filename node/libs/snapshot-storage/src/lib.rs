//! Immutable snapshot resources shared by publication, HTTP and explicit GC.
//! No node or protocol codec dependency lives here.
//!
//! Primary/legacy bodies remain `<block-id>` beside `<block-id>.header`.
//! New partitions live at `<block-id>.partitions/accounts_<id>` or
//! `messages_<id>`. These are hard links into `.snapshot-resources/blobs/`:
//! snapshots remain browsable as groups without duplicating shared bodies.
//! Records and publication leases protect the pool during GC; private pool
//! names are never exposed by HTTP. Download-cache storage is independent.
pub mod chunks;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;
use tempfile::NamedTempFile;

pub type ResourceId = [u8; 32];
const PRIVATE: &str = ".snapshot-resources";

#[derive(Clone, Copy)]
pub enum PartitionKind {
    Accounts,
    Messages,
}

/// Public HTTP/disk name. Logical content IDs and manifest hashes do not change.
pub fn partition_path(primary: &ResourceId, id: &ResourceId, kind: PartitionKind) -> String {
    let prefix = match kind {
        PartitionKind::Accounts => "accounts",
        PartitionKind::Messages => "messages",
    };
    format!("{}.partitions/{prefix}_{}", hex::encode(primary), hex::encode(id))
}

/// Recognize only the exact two-component public partition namespace.
pub fn partition_ids(path: &str) -> Option<(&str, &str)> {
    let (directory, name) = path.split_once('/')?;
    let primary = directory.strip_suffix(".partitions")?;
    let id = name.strip_prefix("accounts_").or_else(|| name.strip_prefix("messages_"))?;
    (valid_id(primary) && valid_id(id)).then_some((primary, id))
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct SnapshotRecord {
    version: u8,
    primary: ResourceId,
    partitions: BTreeSet<ResourceId>,
    #[serde(default)]
    retention: Option<RetentionInfo>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RetentionInfo {
    thread: Vec<u8>,
    seq_no: u64,
    started_at: SystemTime,
    primary_ready_at: Option<SystemTime>,
}

/// Local storage policy, never a consensus/config-hash input.
#[derive(Clone, Copy)]
pub struct RetentionPolicy {
    pub min_age: Duration,
    pub min_snapshots_per_thread: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self { min_age: Duration::from_secs(3600), min_snapshots_per_thread: 2 }
    }
}

#[derive(Clone)]
pub struct SnapshotResources {
    directory: PathBuf,
}

/// An OS file lock keeps GC from retiring this record while publication runs.
/// Partial/failed attempts retain their record until explicit retirement, so
/// completed shared resources remain reusable after cancellation or a crash.
pub struct Publication {
    store: SnapshotResources,
    record: SnapshotRecord,
    _lease: File,
}

fn regular(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file())
}

fn lock(path: &Path, shared: bool) -> anyhow::Result<File> {
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
    if shared {
        file.lock_shared()?;
    } else {
        file.lock()?;
    }
    Ok(file)
}

/// Capture a file and its metadata consistently with publication/retirement.
/// HTTP releases the guard after opening its file, not after streaming it.
pub fn read_guard(directory: &Path) -> anyhow::Result<Option<File>> {
    let path = directory.join(PRIVATE).join("lock");
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(lock(&path, true)?))
}

/// Only ready primary/header pairs participate in discovery. Partitions,
/// provisional data, private indexes and arbitrary files never become snapshots.
pub fn snapshot_headers(directory: &Path) -> anyhow::Result<Vec<(String, std::time::SystemTime)>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(primary) = name.strip_suffix(".header").filter(|id| valid_id(id)) else {
            continue;
        };
        if regular(&entry.path()) && regular(&directory.join(primary)) {
            result.push((primary.to_owned(), entry.metadata()?.modified()?));
        }
    }
    result.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(result)
}

pub fn valid_id(name: &str) -> bool {
    name.len() == 64
        && name.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl SnapshotResources {
    pub fn open(directory: &Path) -> anyhow::Result<Self> {
        anyhow::ensure!(directory.is_dir(), "snapshot resource directory must exist");
        fs::create_dir_all(directory.join(PRIVATE))?;
        anyhow::ensure!(
            fs::symlink_metadata(directory.join(PRIVATE))?.file_type().is_dir(),
            "invalid private resource directory"
        );
        for child in ["records", "leases", "staging", "blobs"] {
            fs::create_dir_all(directory.join(PRIVATE).join(child))?;
            anyhow::ensure!(
                fs::symlink_metadata(directory.join(PRIVATE).join(child))?.file_type().is_dir(),
                "invalid private resource subdirectory"
            );
        }
        // Never recreate/remove the lock inode; all independent handles/processes
        // coordinate through the same file description namespace.
        drop(lock(&directory.join(PRIVATE).join("lock"), false)?);
        File::open(directory.join(PRIVATE))?.sync_all()?;
        File::open(directory)?.sync_all()?;
        Ok(Self { directory: directory.to_owned() })
    }

    fn private(&self, child: &str, id: &ResourceId) -> PathBuf {
        self.directory.join(PRIVATE).join(child).join(format!("{}.json", hex::encode(id)))
    }

    fn guard(&self) -> anyhow::Result<File> {
        lock(&self.directory.join(PRIVATE).join("lock"), false)
    }

    pub fn begin(
        &self,
        primary: ResourceId,
        partitions: impl IntoIterator<Item = ResourceId>,
    ) -> anyhow::Result<Publication> {
        self.begin_with_retention(primary, partitions, None)
    }

    pub fn begin_snapshot(
        &self,
        primary: ResourceId,
        partitions: impl IntoIterator<Item = ResourceId>,
        thread: &[u8],
        seq_no: u64,
    ) -> anyhow::Result<Publication> {
        anyhow::ensure!(!thread.is_empty(), "empty snapshot retention thread");
        self.begin_with_retention(
            primary,
            partitions,
            Some(RetentionInfo {
                thread: thread.to_vec(),
                seq_no,
                started_at: SystemTime::now(),
                primary_ready_at: None,
            }),
        )
    }

    /// Adopt a ready monolithic snapshot using its decoded discovery header.
    /// Existing ownership is never replaced. Preserve publication timestamps so
    /// periodic discovery does not renew retention of old legacy snapshots.
    pub fn register_legacy_snapshot(
        &self,
        primary: ResourceId,
        header: &[u8],
        thread: &[u8],
        seq_no: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!thread.is_empty(), "empty snapshot retention thread");
        let _guard = self.guard()?;
        let record_path = self.private("records", &primary);
        if record_path.exists() {
            return Ok(());
        }
        let records = self.records()?;
        anyhow::ensure!(
            !records.iter().any(|record| record.partitions.contains(&primary)),
            "legacy snapshot conflicts with a partition"
        );
        let body = self.directory.join(hex::encode(primary));
        let header_path = self.directory.join(format!("{}.header", hex::encode(primary)));
        anyhow::ensure!(regular(&body) && regular(&header_path), "legacy snapshot is not ready");
        anyhow::ensure!(fs::read(&header_path)? == header, "legacy snapshot header changed");
        anyhow::ensure!(
            chunks::verify_map(&mut File::open(&body)?).is_err(),
            "cannot adopt a chunked snapshot as legacy"
        );
        let published =
            fs::metadata(&body)?.modified()?.max(fs::metadata(header_path)?.modified()?);
        let record = SnapshotRecord {
            version: 1,
            primary,
            partitions: BTreeSet::new(),
            retention: Some(RetentionInfo {
                thread: thread.to_vec(),
                seq_no,
                started_at: published,
                primary_ready_at: Some(published),
            }),
        };
        atomic_write(&record_path, &serde_json::to_vec(&record)?)
    }

    fn begin_with_retention(
        &self,
        primary: ResourceId,
        partitions: impl IntoIterator<Item = ResourceId>,
        retention: Option<RetentionInfo>,
    ) -> anyhow::Result<Publication> {
        // Lease before global lock: waiting for another publication must not
        // prevent it from finishing. GC only try-locks leases, never waits.
        let lease = lock(&self.private("leases", &primary), false)?;
        let _guard = self.guard()?;
        let mut record = SnapshotRecord {
            version: 1,
            primary,
            partitions: partitions.into_iter().collect(),
            retention,
        };
        fs::create_dir_all(self.staging(&primary))?;
        anyhow::ensure!(
            !record.partitions.contains(&primary),
            "primary resource cannot also be a partition"
        );
        let path = self.private("records", &primary);
        if path.exists() {
            let existing: SnapshotRecord = serde_json::from_reader(File::open(&path)?)?;
            anyhow::ensure!(
                existing.version == record.version
                    && existing.primary == record.primary
                    && existing.partitions == record.partitions,
                "snapshot resource membership changed"
            );
            if let (Some(old), Some(new)) = (&existing.retention, &record.retention) {
                anyhow::ensure!(
                    old.thread == new.thread && old.seq_no == new.seq_no,
                    "snapshot retention identity changed"
                );
            }
            if existing.retention.is_some() {
                record.retention = existing.retention;
            } else if record.retention.is_some() {
                atomic_write(&path, &serde_json::to_vec(&record)?)?;
            }
        } else {
            anyhow::ensure!(
                fs::symlink_metadata(
                    self.directory.join(format!("{}.partitions", hex::encode(primary)))
                )
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
                "refusing to adopt an untracked partition directory"
            );
            let tracked: BTreeSet<_> = self
                .records()?
                .into_iter()
                .flat_map(|record| std::iter::once(record.primary).chain(record.partitions))
                .collect();
            if self.directory.join(format!("{}.header", hex::encode(primary))).exists() {
                anyhow::ensure!(
                    tracked.contains(&primary),
                    "refusing to adopt an untracked snapshot header"
                );
            }
            for id in std::iter::once(&primary).chain(&record.partitions) {
                if self.directory.join(hex::encode(id)).exists() {
                    anyhow::ensure!(
                        tracked.contains(id),
                        "refusing to adopt an untracked resource"
                    );
                }
            }
            atomic_write(&path, &serde_json::to_vec(&record)?)?;
        }
        Ok(Publication { store: self.clone(), record, _lease: lease })
    }

    fn staging(&self, primary: &ResourceId) -> PathBuf {
        self.directory.join(PRIVATE).join("staging").join(hex::encode(primary))
    }

    fn clear_staging(&self, primary: &ResourceId) -> anyhow::Result<()> {
        let path = self.staging(primary);
        if !path.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            anyhow::ensure!(
                entry.file_type()?.is_file()
                    && entry.file_name().to_string_lossy().starts_with(".tmp"),
                "unexpected staging entry"
            );
            fs::remove_file(entry.path())?;
        }
        File::open(path)?.sync_all()?;
        Ok(())
    }

    /// Explicit mark/sweep, serialized against short publication commits.
    /// Active publications are retained even when omitted from `keep`.
    /// Never touches untracked legacy snapshots, consumer cache files or leases.
    /// Legacy snapshots explicitly registered from ready headers are tracked.
    /// Permanent lease inodes prevent races with waiters; their small files are
    /// intentionally retained rather than unlinked and recreated.
    pub fn retain(&self, keep: &BTreeSet<ResourceId>) -> anyhow::Result<()> {
        self.retain_where(|_| keep.clone())
    }

    /// Protect ready primary snapshots for min_age and the latest block sequence
    /// numbers per thread. Pre-primary failures get a grace period from start,
    /// but never displace ready snapshots. Unknown old records are kept safely.
    pub fn retain_recent(&self, policy: RetentionPolicy, now: SystemTime) -> anyhow::Result<()> {
        self.retain_where(|records| retention_keep(records, policy, now))
    }

    fn records(&self) -> anyhow::Result<Vec<SnapshotRecord>> {
        let mut records = Vec::new();
        // Validate the complete index before deleting anything; corruption is
        // fail-closed, never an excuse to treat references as absent.
        for entry in fs::read_dir(self.directory.join(PRIVATE).join("records"))? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') {
                continue;
            }
            let id = name
                .strip_suffix(".json")
                .filter(|id| valid_id(id))
                .ok_or_else(|| anyhow::anyhow!("invalid snapshot record filename"))?;
            anyhow::ensure!(regular(&entry.path()), "invalid snapshot record file");
            let record: SnapshotRecord = serde_json::from_reader(File::open(entry.path())?)?;
            anyhow::ensure!(
                record.version == 1
                    && hex::encode(record.primary) == id
                    && !record.partitions.contains(&record.primary)
                    && record.retention.as_ref().is_none_or(|info| !info.thread.is_empty()),
                "invalid snapshot record"
            );
            records.push(record);
        }
        Ok(records)
    }

    fn retain_where(
        &self,
        choose_keep: impl FnOnce(&[SnapshotRecord]) -> BTreeSet<ResourceId>,
    ) -> anyhow::Result<()> {
        let _guard = self.guard()?;
        let records = self.records()?;
        let keep = choose_keep(&records);
        let mut live = BTreeSet::new();
        let mut retired = Vec::new();
        let mut sweep = BTreeSet::new();
        let mut leases = Vec::new();
        let mut inactive = Vec::new();
        for record in records {
            let lease = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.private("leases", &record.primary))?;
            let active = match lease.try_lock() {
                Ok(()) => false,
                Err(std::fs::TryLockError::WouldBlock) => true,
                Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            };
            if !active {
                inactive.push(record.primary);
            }
            if active || keep.contains(&record.primary) {
                live.insert(hex::encode(record.primary));
                live.extend(record.partitions.into_iter().map(hex::encode));
            } else {
                sweep.insert(hex::encode(record.primary));
                sweep.extend(record.partitions.into_iter().map(hex::encode));
                retired.push(record.primary);
            }
            leases.push(lease);
        }
        // Recover orphaned chunked files without a separate per-blob index.
        // Only recognize regular flat resource names with a verified map; never
        // infer ownership from an arbitrary footer or scan the compressed payload.
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(primary) = name.strip_suffix(".partitions").filter(|id| valid_id(id)) {
                if entry.file_type()?.is_dir() && !live.contains(primary) {
                    let id: ResourceId = hex::decode(primary)?.try_into().expect("validated id");
                    if !retired.contains(&id) {
                        // Recover aliases even if their record was lost. Only
                        // recognize exact names and verified chunk maps; an
                        // unrelated directory must not become a GC target.
                        let mut managed = true;
                        for child in fs::read_dir(entry.path())? {
                            let child = child?;
                            let path = format!("{name}/{}", child.file_name().to_string_lossy());
                            if partition_ids(&path).is_none()
                                || !child.file_type()?.is_file()
                                || chunks::verify_map(&mut File::open(child.path())?).is_err()
                            {
                                managed = false;
                                break;
                            }
                        }
                        if managed {
                            retired.push(id);
                        }
                    }
                }
                continue;
            }
            if !valid_id(&name)
                || live.contains(&name)
                || sweep.contains(&name)
                || !entry.file_type()?.is_file()
            {
                continue;
            }
            if File::open(entry.path())
                .ok()
                .is_some_and(|mut file| chunks::verify_map(&mut file).is_ok())
            {
                sweep.insert(name);
            }
        }
        // A process crash releases leases but leaves private temporary files.
        // Their per-primary directories make cleanup independent of other work.
        for primary in inactive {
            self.clear_staging(&primary)?;
        }
        for id in sweep.difference(&live) {
            remove_if_exists(&self.directory.join(format!("{id}.header")))?;
        }
        // Hide discovery durably before forgetting any references. Otherwise
        // a crash could resurrect a header whose partitions have been swept.
        File::open(&self.directory)?.sync_all()?;
        for id in sweep {
            if !live.contains(&id) {
                remove_if_exists(&self.directory.join(&id))?;
            }
        }
        File::open(&self.directory)?.sync_all()?;
        // Keep records until the owned files have been durably swept. On a
        // crash, all published blobs remain discoverable by GC through records.
        for primary in retired {
            // Only unlink known partition aliases; never recursively remove an
            // arbitrary directory. Shared inodes remain alive in other snapshots.
            let directory = self.directory.join(format!("{}.partitions", hex::encode(primary)));
            if directory.exists() {
                anyhow::ensure!(
                    fs::symlink_metadata(&directory)?.file_type().is_dir(),
                    "invalid partition directory"
                );
                for entry in fs::read_dir(&directory)? {
                    let entry = entry?;
                    let name = format!(
                        "{}.partitions/{}",
                        hex::encode(primary),
                        entry.file_name().to_string_lossy()
                    );
                    anyhow::ensure!(
                        partition_ids(&name).is_some() && entry.file_type()?.is_file(),
                        "invalid partition alias"
                    );
                    fs::remove_file(entry.path())?;
                }
                fs::remove_dir(directory)?;
            }
            File::open(&self.directory)?.sync_all()?;
            remove_if_exists(&self.private("records", &primary))?;
        }
        // The private content pool is shared by all snapshot directories.
        // Records/leases, not hard-link counts, determine its live ownership.
        let pool = self.directory.join(PRIVATE).join("blobs");
        for entry in fs::read_dir(&pool)? {
            let entry = entry?;
            let id = entry.file_name().to_string_lossy().into_owned();
            if valid_id(&id) && entry.file_type()?.is_file() && !live.contains(&id) {
                fs::remove_file(entry.path())?;
            }
        }
        File::open(pool)?.sync_all()?;
        File::open(self.directory.join(PRIVATE).join("records"))?.sync_all()?;
        Ok(())
    }
}

impl Publication {
    /// Encode outside the global index lock. The lease protects all resources
    /// while GC runs; only atomic publication is locked.
    pub fn publish(
        &self,
        id: ResourceId,
        write: impl FnOnce(&mut dyn Write) -> anyhow::Result<()>,
    ) -> anyhow::Result<String> {
        self.publish_checked(id, write, &|| false)
    }

    pub fn publish_checked(
        &self,
        id: ResourceId,
        write: impl FnOnce(&mut dyn Write) -> anyhow::Result<()>,
        should_cancel: &impl Fn() -> bool,
    ) -> anyhow::Result<String> {
        self.publish_resource_checked(id, None, write, should_cancel)
    }

    /// Share immutable partition contents through a private pool; each snapshot
    /// exposes its own hard link, without duplicating bodies on disk.
    pub fn publish_partition_checked(
        &self,
        id: ResourceId,
        kind: PartitionKind,
        write: impl FnOnce(&mut dyn Write) -> anyhow::Result<()>,
        should_cancel: &impl Fn() -> bool,
    ) -> anyhow::Result<String> {
        anyhow::ensure!(self.record.partitions.contains(&id), "partition not registered");
        self.publish_resource_checked(id, Some(kind), write, should_cancel)
    }

    fn publish_resource_checked(
        &self,
        id: ResourceId,
        kind: Option<PartitionKind>,
        write: impl FnOnce(&mut dyn Write) -> anyhow::Result<()>,
        should_cancel: &impl Fn() -> bool,
    ) -> anyhow::Result<String> {
        anyhow::ensure!(!should_cancel(), "snapshot resource publication cancelled");
        anyhow::ensure!(
            id == self.record.primary || self.record.partitions.contains(&id),
            "resource not registered for snapshot"
        );
        let mut temporary = NamedTempFile::new_in(self.store.staging(&self.record.primary))?;
        write(temporary.as_file_mut())?;
        temporary.as_file_mut().flush()?;
        temporary.as_file().sync_all()?;
        let bytes = temporary.as_file().metadata()?.len();
        // ChunkWriter has already hashed the compressed payload. Its immutable
        // footer identifies that complete wire representation; do not hash it
        // again here or scan an existing blob while holding the directory lock.
        // Non-chunked callers retain byte-exact collision checks.
        let footer = chunks::read_footer(temporary.as_file_mut()).ok();
        let fallback =
            if footer.is_none() { Some(digest(temporary.reopen()?, should_cancel)?) } else { None };
        let verify_existing = |mut file: File| -> anyhow::Result<()> {
            anyhow::ensure!(!should_cancel(), "snapshot resource publication cancelled");
            anyhow::ensure!(
                file.metadata()?.len() == bytes,
                "existing snapshot resource has different bytes"
            );
            if let Some(expected) = &footer {
                anyhow::ensure!(
                    chunks::read_footer(&mut file)? == *expected,
                    "existing snapshot resource has different footer"
                );
            } else {
                let existing = digest(file, should_cancel)?;
                let expected = fallback.as_ref().expect("non-chunked digest");
                anyhow::ensure!(
                    existing.bytes == expected.bytes && existing.digest == expected.digest,
                    "existing snapshot resource has different bytes"
                );
            }
            Ok(())
        };
        let name = hex::encode(id);
        let destination = match kind {
            Some(_) => self.store.directory.join(PRIVATE).join("blobs").join(&name),
            None => self.store.directory.join(&name),
        };
        let existing = {
            let _guard = self.store.guard()?;
            anyhow::ensure!(!should_cancel(), "snapshot resource publication cancelled");
            match temporary.persist_noclobber(&destination) {
                Ok(_) => None,
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    anyhow::ensure!(regular(&destination), "invalid existing resource file");
                    Some(File::open(&destination)?)
                }
                Err(error) => return Err(error.error.into()),
            }
        };
        // The publication lease protects both this snapshot and its shared
        // pool references from GC, so immutable contents can be checked outside
        // the global lock. Other snapshots' GET/HEAD must not wait for that I/O.
        if let Some(file) = existing {
            verify_existing(file)?;
        }
        let guard = self.store.guard()?;
        anyhow::ensure!(!should_cancel(), "snapshot resource publication cancelled");
        if let Some(kind) = kind {
            File::open(destination.parent().expect("blob parent"))?.sync_all()?;
            let name = partition_path(&self.record.primary, &id, kind);
            let alias = self.store.directory.join(&name);
            let directory = alias.parent().expect("partition parent");
            match fs::create_dir(directory) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    anyhow::ensure!(
                        fs::symlink_metadata(directory)?.file_type().is_dir(),
                        "invalid partition directory"
                    );
                }
                Err(error) => return Err(error.into()),
            }
            let existing_alias = match fs::hard_link(&destination, &alias) {
                Ok(()) => None,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    anyhow::ensure!(regular(&alias), "invalid existing partition alias");
                    let source = File::open(&destination)?;
                    let alias = File::open(&alias)?;
                    if same_inode(&source, &alias)? {
                        None
                    } else {
                        Some(alias)
                    }
                }
                Err(error) => return Err(error.into()),
            };
            File::open(directory)?.sync_all()?;
            File::open(&self.store.directory)?.sync_all()?;
            drop(guard);
            if let Some(file) = existing_alias {
                verify_existing(file)?;
            }
            return Ok(name);
        }
        File::open(&self.store.directory)?.sync_all()?;
        Ok(name)
    }

    /// Discovery becomes ready after primary, independently of partition readiness.
    pub fn publish_header(&self, bytes: &[u8]) -> anyhow::Result<()> {
        let _guard = self.store.guard()?;
        anyhow::ensure!(
            regular(&self.store.directory.join(hex::encode(self.record.primary))),
            "primary not published"
        );
        let path =
            self.store.directory.join(format!("{}.header", hex::encode(self.record.primary)));
        // Persist the first ready time before discovery. Retries do not renew
        // the grace period; imported v1 ownership records are backfilled safely.
        let record_path = self.store.private("records", &self.record.primary);
        let mut record: SnapshotRecord = serde_json::from_reader(File::open(&record_path)?)?;
        if let Some(info) = &mut record.retention {
            if info.primary_ready_at.is_none() {
                info.primary_ready_at = Some(SystemTime::now());
                atomic_write(&record_path, &serde_json::to_vec(&record)?)?;
            }
        }
        if path.exists() {
            anyhow::ensure!(fs::read(&path)? == bytes, "snapshot header changed");
        } else {
            atomic_write(&path, bytes)?;
        }
        Ok(())
    }
}

fn retention_keep(
    records: &[SnapshotRecord],
    policy: RetentionPolicy,
    now: SystemTime,
) -> BTreeSet<ResourceId> {
    let mut keep = BTreeSet::new();
    let mut threads: BTreeMap<&[u8], Vec<&SnapshotRecord>> = BTreeMap::new();
    for record in records {
        let Some(info) = &record.retention else {
            keep.insert(record.primary);
            continue;
        };
        let age = now.duration_since(info.primary_ready_at.unwrap_or(info.started_at));
        if age.is_err() || age.is_ok_and(|age| age < policy.min_age) {
            keep.insert(record.primary);
        }
        if info.primary_ready_at.is_some() {
            threads.entry(&info.thread).or_default().push(record);
        }
    }
    for records in threads.values_mut() {
        records.sort_by_key(|record| (record.retention.as_ref().unwrap().seq_no, record.primary));
        keep.extend(
            records.iter().rev().take(policy.min_snapshots_per_thread).map(|record| record.primary),
        );
    }
    keep
}

fn atomic_write(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().expect("resource parent");
    let mut temporary = NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn remove_if_exists(path: &Path) -> anyhow::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

struct BodyDigest {
    bytes: u64,
    digest: [u8; 32],
}

fn digest(mut input: File, should_cancel: &impl Fn() -> bool) -> anyhow::Result<BodyDigest> {
    let mut hash = blake3::Hasher::new();
    let mut bytes = 0;
    let mut buffer = [0; 65536];
    loop {
        anyhow::ensure!(!should_cancel(), "snapshot resource publication cancelled");
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes += count as u64;
        hash.update(&buffer[..count]);
    }
    input.sync_all()?;
    Ok(BodyDigest { bytes, digest: *hash.finalize().as_bytes() })
}

fn same_inode(first: &File, second: &File) -> anyhow::Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let first = first.metadata()?;
        let second = second.metadata()?;
        Ok(first.dev() == second.dev() && first.ino() == second.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = (first, second);
        Ok(false)
    }
}

#[cfg(test)]
mod tests;
