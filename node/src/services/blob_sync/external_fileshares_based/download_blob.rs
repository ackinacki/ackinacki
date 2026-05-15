// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use thiserror::Error;

use crate::helper::get_temp_file_path;

const CONNECT_TIMEOUT: Option<std::time::Duration> = Some(std::time::Duration::from_secs(3));

#[derive(Debug, Error, PartialEq)]
pub enum DownloadError {
    #[error("Max tries exceeded")]
    MaxTriesExceeded,
}

pub fn download_blob(
    share_full_path: &PathBuf,
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
    share_full_path: &PathBuf,
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
    let tmp_file_path = get_temp_file_path(tmp_dir_path);
    tracing::trace!("download_blob: trying to create file: {tmp_file_path:?}");
    if let Some(parent) = tmp_file_path.parent() {
        if !parent.exists() {
            tracing::trace!("download_blob: trying create to parent dir: {parent:?}");
            std::fs::create_dir_all(parent).expect("Failed to create dir for shared state");
        }
    }
    let mut file = std::fs::File::create(tmp_file_path.clone())?;
    let mut is_downloaded = false;
    for _ in 0..max_tries {
        for url in urls.iter() {
            reset_download_file(&mut file)?;
            if let Some(deadline) = deadline {
                if deadline <= std::time::Instant::now() {
                    anyhow::bail!("Failed to download a blob: deadline.");
                }
            }
            match attempt_download(url, &mut file, deadline) {
                Ok(()) => {
                    let downloaded_len = file.metadata()?.len();
                    anyhow::ensure!(downloaded_len > 0, "Failed to download a blob: empty file");
                    is_downloaded = true;
                    break;
                }
                Err(e) => {
                    tracing::error!("Download failed: {}", e);
                    reset_download_file(&mut file)?;
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
    tracing::trace!("download_blob: rename file: {tmp_file_path:?} -> {share_full_path:?}");
    std::fs::rename(tmp_file_path, share_full_path)?;
    Ok(())
}

fn download_file(
    url: &url::Url,
    file: &mut std::fs::File,
    deadline: Option<std::time::Instant>,
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
    response.copy_to(file)?;
    file.sync_all()?;
    tracing::trace!("Downloaded {}", url);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::download_blob_with_attempt;

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
}
