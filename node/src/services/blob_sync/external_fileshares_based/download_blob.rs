// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use tempfile::NamedTempFile;
use thiserror::Error;

use super::download_cache::CacheLease;
use super::download_cache::DownloadCache;

/// Primary/legacy downloads share the partition cache budget. Reserve the
/// advertised complete response size before writing, not incrementally: several
/// half-downloaded files must not consume all space and wait on each other.
pub(super) fn download_snapshot_blob(
    cache: &DownloadCache,
    id: &str,
    urls: &[url::Url],
    max_tries: u8,
    retry_timeout: Option<std::time::Duration>,
    deadline: Option<std::time::Instant>,
) -> anyhow::Result<CacheLease> {
    download_snapshot_with_attempt(
        cache,
        id,
        urls,
        max_tries,
        retry_timeout,
        deadline,
        |url, file, deadline, lease| {
            download_file_impl(url, file, deadline, Some((lease, cache.budget())))
        },
    )
}

fn download_snapshot_with_attempt<F>(
    cache: &DownloadCache,
    id: &str,
    urls: &[url::Url],
    max_tries: u8,
    retry_timeout: Option<std::time::Duration>,
    deadline: Option<std::time::Instant>,
    mut attempt: F,
) -> anyhow::Result<CacheLease>
where
    F: FnMut(
        &url::Url,
        &mut std::fs::File,
        Option<std::time::Instant>,
        &CacheLease,
    ) -> anyhow::Result<()>,
{
    let lease = cache.acquire(id, 0, &|| {
        crate::helper::SHUTDOWN_FLAG.get() == Some(&true)
            || deadline.is_some_and(|end| end <= std::time::Instant::now())
    })?;
    let path = lease.path();
    download_blob_with_attempt(
        &path,
        path.parent().expect("cache directory"),
        urls,
        max_tries,
        retry_timeout,
        deadline,
        |url, file, deadline| {
            // The outer retry loop has truncated the previous private file.
            // Do not retain its old reservation while requesting a larger one.
            lease.invalidate()?;
            lease.resize(0, &|| false)?;
            let result = attempt(url, file, deadline, &lease);
            if result.is_err() {
                reset_download_file(file)?;
                lease.resize(0, &|| false)?;
            }
            result
        },
    )?;
    lease.accepted()?;
    Ok(lease)
}

const CONNECT_TIMEOUT: Option<std::time::Duration> = Some(std::time::Duration::from_secs(3));

#[derive(Debug, Error, PartialEq)]
pub enum DownloadError {
    #[error("Max tries exceeded")]
    MaxTriesExceeded,
}

pub fn download_blob(
    share_full_path: &std::path::PathBuf,
    tmp_dir_path: &Path,
    urls: &[url::Url],
    max_tries: u8,
    retry_timeout: Option<std::time::Duration>,
    deadline: Option<std::time::Instant>,
) -> anyhow::Result<()> {
    download_blob_with_attempt(
        share_full_path,
        tmp_dir_path,
        urls,
        max_tries,
        retry_timeout,
        deadline,
        download_file,
    )
}

fn reset_download_file(file: &mut std::fs::File) -> anyhow::Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.sync_all()?;
    Ok(())
}

fn download_blob_with_attempt<F>(
    share_full_path: &std::path::PathBuf,
    tmp_dir_path: &Path,
    urls: &[url::Url],
    max_tries: u8,
    retry_timeout: Option<std::time::Duration>,
    deadline: Option<std::time::Instant>,
    mut attempt_download: F,
) -> anyhow::Result<()>
where
    F: FnMut(&url::Url, &mut std::fs::File, Option<std::time::Instant>) -> anyhow::Result<()>,
{
    if share_full_path.exists() {
        if std::fs::metadata(share_full_path)?.len() > 0 {
            return Ok(());
        }
        tracing::warn!(
            "download_blob: removing empty cached blob before retry: {}",
            share_full_path.display()
        );
        std::fs::remove_file(share_full_path)?;
    }
    // Honor the caller's retry budget. Previously this function ignored
    // its `_max_tries` / `_retry_timeout` / `_deadline` parameters and
    // used hardcoded 2000 tries / 30s sleep / 1h deadline, which made
    // every persistent-404 download take an hour to give up. The caller
    // now controls the budget; for the load worker that's a short
    // per-candidate cycle so it can move on to the next candidate
    // quickly on transient HTTP failures.
    let max_tries: u32 = max_tries.max(1) as u32;
    tracing::trace!("downloading blob: max_tries={max_tries}, retry_timeout={retry_timeout:?}, deadline={deadline:?}");
    if !tmp_dir_path.exists() {
        tracing::trace!("download_blob: trying create to parent dir: {tmp_dir_path:?}");
        std::fs::create_dir_all(tmp_dir_path).expect("Failed to create dir for shared state");
    }
    let mut temp = NamedTempFile::new_in(tmp_dir_path)?;
    tracing::trace!("download_blob: created temp file: {:?}", temp.path());
    let mut is_downloaded = false;
    for _ in 0..max_tries {
        for url in urls.iter() {
            reset_download_file(temp.as_file_mut())?;
            if let Some(deadline) = deadline {
                if deadline <= std::time::Instant::now() {
                    anyhow::bail!("Failed to download a blob: deadline.");
                }
            }
            match attempt_download(url, temp.as_file_mut(), deadline) {
                Ok(()) => {
                    let downloaded_len = temp.as_file().metadata()?.len();
                    anyhow::ensure!(downloaded_len > 0, "Failed to download a blob: empty file");
                    is_downloaded = true;
                    break;
                }
                Err(e) => {
                    tracing::error!("Download failed: {}", e);
                    reset_download_file(temp.as_file_mut())?;
                }
            }
        }
        if is_downloaded {
            break;
        }
        if let Some(retry_timeout) = retry_timeout {
            std::thread::sleep(retry_timeout);
        }
    }
    if !is_downloaded {
        anyhow::bail!(DownloadError::MaxTriesExceeded);
    }
    tracing::trace!("download_blob: rename file: {:?} -> {share_full_path:?}", temp.path());
    temp.persist(share_full_path)?;
    Ok(())
}

fn download_file(
    url: &url::Url,
    file: &mut std::fs::File,
    deadline: Option<std::time::Instant>,
) -> anyhow::Result<()> {
    download_file_impl(url, file, deadline, None)
}

fn download_file_impl(
    url: &url::Url,
    file: &mut std::fs::File,
    deadline: Option<std::time::Instant>,
    cache: Option<(&CacheLease, u64)>,
) -> anyhow::Result<()> {
    tracing::trace!("Downloading {} ...", url);
    let timeout = match deadline {
        Some(deadline) => {
            let timeout = deadline.saturating_duration_since(std::time::Instant::now());
            if timeout.is_zero() {
                anyhow::bail!("Failed to download a blob: deadline.");
            }
            Some(timeout)
        }
        None => None,
    };
    let client: reqwest::blocking::Client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .connect_timeout(CONNECT_TIMEOUT)
        .build()?;
    let mut response = client.get(url.clone()).send()?;
    if response.status().is_server_error() {
        anyhow::bail!("download blob: server error!");
    }
    if !response.status().is_success() {
        anyhow::bail!("download blob: Some error happened. Status: {:?}", response.status());
    }
    // Initial snapshot downloads share one import-admission slot, independently
    // of partition workers. Legacy files and partitioned primary use the same
    // transport; only successful HTTP bodies count, never polling HTTP 404s.
    let _activity = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|id| snapshot_storage::valid_id(id))
        .map(|_| {
            crate::helper::metrics::ActivityMetric::parallel(
                "snapshot_download",
                "transfer",
                "initial:0",
            )
            .enter()
        });
    if let Some((lease, budget)) = cache {
        let limit = response.content_length().unwrap_or(budget);
        anyhow::ensure!(limit <= budget, "snapshot blob exceeds download cache budget");
        lease.resize(limit, &|| {
            crate::helper::SHUTDOWN_FLAG.get() == Some(&true)
                || deadline.is_some_and(|end| end <= std::time::Instant::now())
        })?;
        let mut bytes = 0u64;
        let mut buffer = [0; 64 * 1024];
        loop {
            let count = std::io::Read::read(&mut response, &mut buffer)?;
            if count == 0 {
                break;
            }
            anyhow::ensure!(
                count as u64 <= limit - bytes,
                "snapshot response exceeds reserved disk budget"
            );
            std::io::Write::write_all(file, &buffer[..count])?;
            bytes += count as u64;
        }
    } else {
        response.copy_to(file)?;
    }
    file.sync_all()?;
    tracing::trace!("Downloaded {}", url);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::download_blob_with_attempt;

    #[test]
    fn concurrent_primary_retries_release_old_reservations_before_growing() -> anyhow::Result<()> {
        use std::io::Write;
        use std::sync::Arc;
        use std::sync::Barrier;
        use std::time::Duration;
        use std::time::Instant;

        use super::download_snapshot_with_attempt;
        use super::DownloadCache;
        let directory = tempfile::tempdir()?;
        let cache = DownloadCache::open(directory.path(), Some(100))?;
        let barrier = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| -> anyhow::Result<()> {
            let mut handles = Vec::new();
            for index in [1, 2] {
                let cache = cache.clone();
                let barrier = barrier.clone();
                handles.push(scope.spawn(move || -> anyhow::Result<()> {
                    let deadline = Instant::now() + Duration::from_secs(2);
                    let mut count = 0;
                    let lease = download_snapshot_with_attempt(
                        &cache,
                        &hex::encode([index; 32]),
                        &[url::Url::parse("http://example.invalid/primary")?],
                        2,
                        None,
                        Some(deadline),
                        |_, file, _, lease| {
                            count += 1;
                            if count == 1 {
                                lease.resize(50, &|| Instant::now() >= deadline)?;
                                file.write_all(&[0; 25])?;
                                // Both failed requests hold 50 before retrying.
                                barrier.wait();
                                anyhow::bail!("partial first response");
                            }
                            lease.resize(60, &|| Instant::now() >= deadline)?;
                            file.write_all(&[0; 60])?;
                            Ok(())
                        },
                    )?;
                    assert_eq!(count, 2);
                    assert_eq!(std::fs::metadata(lease.path())?.len(), 60);
                    drop(lease);
                    Ok(())
                }));
            }
            for handle in handles {
                handle.join().unwrap()?;
            }
            Ok(())
        })
    }

    #[test]
    fn retry_download_restarts_from_file_start_after_partial_failure() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let share_full_path = dir.path().join("snapshot");
        let url = url::Url::parse("http://example.invalid/snapshot")?;
        let mut attempt = 0usize;

        download_blob_with_attempt(
            &share_full_path,
            dir.path(),
            &[url],
            2,
            None,
            None,
            |_url, file, _deadline| {
                attempt += 1;
                match attempt {
                    1 => {
                        use std::io::Write;
                        file.write_all(b"partial")?;
                        anyhow::bail!("synthetic failure after partial write")
                    }
                    2 => {
                        use std::io::Write;
                        file.write_all(b"complete-snapshot")?;
                        file.sync_all()?;
                        Ok(())
                    }
                    _ => anyhow::bail!("unexpected extra attempt"),
                }
            },
        )?;

        assert_eq!(std::fs::read(&share_full_path)?, b"complete-snapshot");
        Ok(())
    }

    #[test]
    fn failed_download_removes_temp_file() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let share_full_path = dir.path().join("snapshot");
        let url = url::Url::parse("http://example.invalid/snapshot")?;

        let err = download_blob_with_attempt(
            &share_full_path,
            dir.path(),
            &[url],
            1,
            None,
            None,
            |_url, file, _deadline| {
                use std::io::Write;
                file.write_all(b"partial")?;
                anyhow::bail!("synthetic failure")
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("Max tries exceeded"));
        assert!(!share_full_path.exists());
        let leftovers = std::fs::read_dir(dir.path())?.collect::<Result<Vec<_>, _>>()?;
        assert!(leftovers.is_empty(), "unexpected temp files left after failed download");
        Ok(())
    }
}
