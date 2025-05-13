// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;

const CONNECT_TIMEOUT: Option<std::time::Duration> = Some(std::time::Duration::from_secs(3));

pub fn download_blob(
    share_full_path: &PathBuf,
    tmp_dir_path: &Path,
    urls: &[url::Url],
    max_tries: u8,
    retry_timeout: Option<std::time::Duration>,
    deadline: Option<std::time::Instant>,
) -> anyhow::Result<()> {
    if share_full_path.exists() {
        return Ok(());
    }
    let tmp_file_path = {
        // TODO: must be guarded code here and in the share blob fn.
        let mut path;
        while {
            let tmp_file_name = format!("_{}.tmp", rand::random::<u64>());
            path = tmp_dir_path.join(tmp_file_name);
            path.exists()
        } {}
        path
    };
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
            if let Some(deadline) = deadline {
                if deadline <= std::time::Instant::now() {
                    anyhow::bail!("Failed to download a blob: deadline.");
                }
            }
            match download_file(url, &mut file, deadline) {
                Ok(()) => {
                    is_downloaded = true;
                    break;
                }
                Err(e) => {
                    tracing::error!("Download failed: {}", e);
                    file.set_len(0)?;
                    file.sync_all()?;
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
        anyhow::bail!("Failed to download a blob: max tries");
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
    tracing::trace!("Downloading {}...", url);
    let client: reqwest::blocking::Client = reqwest::blocking::Client::builder()
        .timeout(
            deadline.map(|deadline| deadline.saturating_duration_since(std::time::Instant::now())),
        )
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
