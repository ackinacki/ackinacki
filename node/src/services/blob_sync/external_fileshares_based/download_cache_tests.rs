use std::io::Read;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use super::*;

fn id(index: u8) -> String {
    hex::encode([index; 32])
}

fn put(cache: &DownloadCache, index: u8, bytes: usize) -> anyhow::Result<()> {
    let lease = cache.acquire(&id(index), bytes as u64, &|| false)?;
    std::fs::write(lease.path(), vec![index; bytes])?;
    lease.accepted()?;
    Ok(())
}

#[test]
fn evicts_lru_but_preserves_recently_reused_files() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = DownloadCache::open(dir.path(), Some(20))?;
    put(&cache, 1, 10)?;
    put(&cache, 2, 10)?;
    let lease = cache.acquire(&id(1), 0, &|| false)?;
    lease.accepted()?;
    drop(lease);
    put(&cache, 3, 10)?;
    assert!(dir.path().join(id(1)).exists());
    assert!(!dir.path().join(id(2)).exists());
    assert!(dir.path().join(id(3)).exists());
    assert_eq!(cache.0.state.lock().bytes, 20);
    Ok(())
}

#[test]
fn private_reservations_block_until_release_and_observe_cancellation() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = DownloadCache::open(dir.path(), Some(10))?;
    let first = cache.acquire(&id(1), 10, &|| false)?;
    assert_eq!(cache.0.state.lock().bytes, 10);
    let waiting = AtomicBool::new(false);
    let complete = AtomicBool::new(false);
    std::thread::scope(|scope| -> anyhow::Result<()> {
        let handle = scope.spawn(|| -> anyhow::Result<()> {
            let _second = cache.acquire(&id(2), 10, &|| {
                waiting.store(true, Ordering::Release);
                false
            })?;
            complete.store(true, Ordering::Release);
            Ok(())
        });
        while !waiting.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        assert!(!complete.load(Ordering::Acquire));
        assert!(cache.acquire(&id(3), 1, &|| true).is_err());
        drop(first);
        handle.join().unwrap()?;
        Ok(())
    })?;
    assert!(complete.load(Ordering::Acquire));
    assert_eq!(cache.0.state.lock().bytes, 0);
    Ok(())
}

#[test]
fn active_cache_hit_is_not_evicted_and_duplicate_acquire_can_cancel() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = DownloadCache::open(dir.path(), Some(20))?;
    put(&cache, 1, 10)?;
    put(&cache, 2, 10)?;
    let active = cache.acquire(&id(1), 0, &|| false)?;
    put(&cache, 3, 10)?;
    assert!(active.path().exists());
    assert!(!dir.path().join(id(2)).exists());
    assert!(cache.acquire(&id(1), 0, &|| true).is_err());
    drop(active);
    Ok(())
}

#[test]
fn restart_recovers_usage_cleans_temps_and_enforces_smaller_budget() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let cache = DownloadCache::open(dir.path(), Some(30))?;
        put(&cache, 1, 10)?;
        put(&cache, 2, 10)?;
    }
    File::open(dir.path().join(id(1)))?
        .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1))?;
    std::fs::write(dir.path().join(format!("{TEMP_PREFIX}orphan")), b"private")?;
    std::fs::write(dir.path().join(".tmp-old-transport"), b"private")?;
    std::fs::write(dir.path().join("unrelated"), b"keep")?;
    std::fs::create_dir(dir.path().join(id(8)))?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(dir.path().join("unrelated"), dir.path().join(id(9)))?;
    let cache = DownloadCache::open(dir.path(), Some(10))?;
    assert_eq!(cache.0.state.lock().bytes, 10);
    assert!(!dir.path().join(id(1)).exists());
    assert!(dir.path().join(id(2)).exists());
    assert!(!dir.path().join(format!("{TEMP_PREFIX}orphan")).exists());
    assert!(!dir.path().join(".tmp-old-transport").exists());
    assert!(dir.path().join("unrelated").exists());
    assert!(dir.path().join(id(8)).is_dir());
    #[cfg(unix)]
    assert!(cache.acquire(&id(9), 0, &|| false).is_err());
    Ok(())
}

#[test]
fn managers_share_reservations_and_never_clean_live_temporary_files() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = DownloadCache::open(dir.path(), Some(10))?;
    let lease = cache.acquire(&id(1), 10, &|| false)?;
    let temporary = tempfile::Builder::new().prefix(TEMP_PREFIX).tempfile_in(dir.path())?;
    let other = DownloadCache::open(dir.path(), None)?;
    assert!(Arc::ptr_eq(&cache.0, &other.0));
    assert!(temporary.path().exists());
    assert_eq!(other.0.state.lock().bytes, 10);
    assert!(DownloadCache::open(dir.path(), Some(11)).is_err());
    drop(lease);
    Ok(())
}

#[test]
fn many_failed_or_completed_attempts_stay_within_budget() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = DownloadCache::open(dir.path(), Some(20))?;
    for index in 1..=30 {
        // Simulate acceptance of one partition, then a failed attempt. Accepted
        // partitions remain useful, but the accumulated disk size stays bounded.
        put(&cache, index, 7)?;
        assert!(cache.0.state.lock().bytes <= 20);
        assert!(
            std::fs::read_dir(dir.path())?
                .map(|item| item.unwrap().metadata().unwrap().len())
                .sum::<u64>()
                <= 20
        );
    }
    assert!(cache.acquire(&id(99), 21, &|| false).is_err());
    let unfinished = cache.acquire(&id(100), 1, &|| false)?;
    std::fs::write(unfinished.path(), b"x")?;
    drop(unfinished);
    assert!(!dir.path().join(id(100)).exists());
    Ok(())
}

#[test]
fn import_admission_is_shared_and_waits_are_cancellable() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = DownloadCache::open(dir.path(), Some(10))?;
    let same = DownloadCache::open(dir.path(), None)?;
    let first = cache.admit_import(&|| false)?;
    assert!(same.admit_import(&|| true).is_err());
    drop(first);
    let _next = same.admit_import(&|| false)?;
    Ok(())
}

#[test]
fn rejecting_a_file_keeps_its_reservation_until_the_private_file_is_gone() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    assert!(DownloadCache::open(dir.path(), Some(0)).is_err());
    let cache = DownloadCache::open(dir.path(), Some(10))?;
    let lease = cache.acquire(&id(1), 10, &|| false)?;
    std::fs::write(lease.path(), b"bad")?;
    lease.accepted()?;
    lease.resize(10, &|| false)?;
    lease.invalidate()?;
    assert!(!lease.path().exists());
    assert_eq!(cache.0.state.lock().bytes, 10);
    assert!(cache.acquire(&id(2), 1, &|| true).is_err());
    drop(lease);
    assert_eq!(cache.0.state.lock().bytes, 0);
    Ok(())
}

#[test]
fn primary_reader_releases_disk_budget_after_eof() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let cache = DownloadCache::open(dir.path(), Some(10))?;
    put(&cache, 1, 10)?;
    let lease = cache.acquire(&id(1), 0, &|| false)?;
    let mut reader = CacheReader::new(lease)?;
    assert!(reader.lease.is_some());
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    assert_eq!(bytes, vec![1; 10]);
    assert!(reader.file.is_none());
    assert!(reader.lease.is_none());
    put(&cache, 2, 10)?;
    assert!(!dir.path().join(id(1)).exists());
    Ok(())
}
