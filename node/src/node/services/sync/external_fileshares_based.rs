// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use sha2::Digest;
use sha2::Sha256;
use typed_builder::TypedBuilder;

use crate::node::services::sync::StateSyncService;

const RETRY_DOWNLOAD_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(TypedBuilder)]
pub struct ExternalFileSharesBased {
    static_storages: Vec<url::Url>,
    local_storage_share_base_path: PathBuf,
    timeout: Duration,
}

impl StateSyncService for ExternalFileSharesBased {
    type ResourceAddress = String;

    fn generate_resource_address(&self, data: &[u8]) -> anyhow::Result<Self::ResourceAddress> {
        let cid: String = {
            let hash = Sha256::digest(data);
            hex::encode(hash)
        };
        Ok(cid)
    }

    fn add_share_state_task(&mut self, data: Vec<u8>) -> anyhow::Result<Self::ResourceAddress> {
        tracing::trace!("add_share_state_task");
        let cid = self.generate_resource_address(&data)?;
        let final_file_name = self.local_storage_share_base_path.join(&cid);
        if final_file_name.exists() {
            return Ok(cid);
        }
        let cid_clone = cid.clone();
        let tmp_file_path = {
            let mut path;
            while {
                let tmp_file_name = format!("_{}.tmp", rand::random::<u64>());
                path = self.local_storage_share_base_path.join(tmp_file_name);
                path.exists()
            } {}
            path
        };
        thread::Builder::new()
            .name(format!("save-{}", cid))
            .spawn(move || {
                {
                    tracing::trace!(
                        "add_share_state_task: trying to create file: {tmp_file_path:?}"
                    );
                    if let Some(parent) = tmp_file_path.parent() {
                        if !parent.exists() {
                            tracing::trace!(
                                "add_share_state_task: trying create to parent dir: {parent:?}"
                            );
                            std::fs::create_dir_all(parent)
                                .expect("Failed to create dir for shared state");
                        }
                    }
                    let mut file = std::fs::File::create(tmp_file_path.clone())
                        .expect("Failed to create shared state file");
                    file.write_all(&data).expect("Failed to save shared state file");
                }
                tracing::trace!(
                    "add_share_state_task: rename file: {tmp_file_path:?} -> {final_file_name:?}"
                );
                std::fs::rename(tmp_file_path, final_file_name)
                    .expect("Failed to move shared state file");
            })
            .map_err(anyhow::Error::from)?;
        Ok(cid_clone)
    }

    fn add_load_state_task(
        &mut self,
        resource_address: Self::ResourceAddress,
        output: Sender<anyhow::Result<(Self::ResourceAddress, Vec<u8>)>>,
    ) -> anyhow::Result<()> {
        let mut urls = Vec::new();
        for storage in self.static_storages.iter() {
            urls.push(storage.join(&resource_address)?);
        }
        let timeout = self.timeout;

        thread::Builder::new()
            .name(format!("load-{}", resource_address))
            .spawn(move || {
                tracing::info!("loading resource {}", resource_address);

                let mut buf = Vec::new();
                if let Err(err) = load_file(&urls, &mut buf, timeout) {
                    tracing::error!("error loading resource {}: {}", resource_address, err);
                    // sleep(RETRY_DOWNLOAD_ATTEMPT_TIMEOUT);
                    Ok(())
                } else {
                    tracing::trace!("File was loaded");
                    output.send(Ok((resource_address, buf)))
                }
            })
            .map_err(anyhow::Error::from)?;
        Ok(())
    }
}

fn load_file(urls: &Vec<url::Url>, buf: &mut Vec<u8>, timeout: Duration) -> anyhow::Result<()> {
    tracing::trace!("load_file start {:?}", urls);
    let (tx, rx) = mpsc::channel();

    let mut threads = Vec::new();
    let mut stop_threads = Vec::new();

    for url in urls.iter() {
        let (stop_tx, stop_rx) = mpsc::channel();
        stop_threads.push(stop_tx);

        let url = url.to_string();
        let tx_clone = tx.clone();
        let timeout_clone = timeout;
        threads.push(
            thread::Builder::new()
                .name(format!("download-{}", &url))
                .spawn(move || {
                    tracing::trace!("Start reqwest loop: {}", &url);
                    let start = std::time::Instant::now();
                    loop {
                        // break if timeout is exceeded
                        if start.elapsed() >= timeout_clone {
                            break;
                        }

                        // break if ok
                        match reqwest::blocking::get(&url) {
                            Ok(r) if r.status().is_success() => {
                                let e = tx_clone.send(r);
                                tracing::trace!("response send result: {:?}", e);
                                let _ = stop_rx.recv();
                                break;
                            }
                            Ok(r) => {
                                tracing::error!(
                                    "http error downloading file {}: {}",
                                    &url,
                                    r.status()
                                );
                            }
                            Err(err) => {
                                tracing::error!("error downloading file {}: {}", &url, err);
                            }
                        }
                        tracing::trace!("waiting for stop: {}", &url);

                        // break if got stop
                        match stop_rx.try_recv() {
                            Err(TryRecvError::Empty) => {}
                            _ => {
                                break;
                            }
                        };

                        sleep(RETRY_DOWNLOAD_ATTEMPT_TIMEOUT);
                    }
                })
                .expect("spawn scoped thread"),
        );
    }

    tracing::trace!("Start waiting for rx");
    match rx.recv() {
        Ok(mut resp) => {
            tracing::trace!("Response received: {:?}", resp);
            resp.copy_to(buf)?;
            tracing::trace!("Response received, buf_len:{}", buf.len());
        }
        Err(err) => {
            tracing::error!("error downloading file: {}", err);
        }
    }

    tracing::trace!("Response received success");

    for stop_tx in stop_threads.into_iter() {
        let _ = stop_tx.send(());
    }

    for thread in threads.into_iter() {
        if let Err(err) = thread.join() {
            tracing::warn!("error joining thread: {:?}", err);
        }
    }

    Ok(())
}
