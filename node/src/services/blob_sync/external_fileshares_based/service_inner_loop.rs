// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use rand::seq::SliceRandom;
use rand::thread_rng;
use telemetry_utils::mpsc::InstrumentedReceiver;

use super::download_blob::download_blob;
use super::download_blob::download_snapshot_blob;
use super::download_cache::CacheReader;
use super::download_cache::DownloadCache;
use super::share_blob::share_blob;
use super::ResourceId;

type ShareCallback = Box<dyn FnOnce(anyhow::Result<()>) + Send + Sync + 'static>;
type LoadSuccessCallback =
    Box<dyn FnOnce(&mut dyn std::io::Read) -> anyhow::Result<()> + Send + Sync + 'static>;
type LoadErrCallback = Box<dyn FnOnce(anyhow::Error) + Send + Sync + 'static>;
type KnownExternalFileshares = Vec<url::Url>;

pub struct DownloadOptions {
    pub max_tries: u8,
    pub retry_timeout: Option<std::time::Duration>,
    pub deadline: Option<std::time::Instant>,
}

pub(super) enum Command {
    Share(ResourceId, Box<dyn std::io::Read + Send + Sync + 'static>, ShareCallback),
    Load(
        ResourceId,
        KnownExternalFileshares,
        DownloadOptions,
        LoadSuccessCallback,
        LoadErrCallback,
    ),
}

pub(super) fn service_inner_loop(
    local_storage_share_base_path: PathBuf,
    control: InstrumentedReceiver<Command>,
    cache: DownloadCache,
) {
    let download_directory = local_storage_share_base_path.join(".snapshot-downloads");
    let local_storage_share_base_path = &local_storage_share_base_path;
    let download_directory = &download_directory;
    std::thread::scope(|s| loop {
        match control.recv() {
            Err(std::sync::mpsc::RecvError) => {
                tracing::trace!("Stopping blob sharing service");
                break;
            }
            Ok(Command::Share(resource_id, stream, on_complete)) => {
                std::thread::Builder::new()
                    .name(format!("share-{}", resource_id))
                    .spawn_scoped(s, move || {
                        tracing::trace!("share_blob: sharing {:?}", &resource_id);
                        let share_full_path = local_storage_share_base_path.join(resource_id);
                        let share_result = {
                            let mut stream = stream;
                            share_blob(share_full_path, local_storage_share_base_path, &mut *stream)
                        };
                        on_complete(share_result);
                    })
                    .unwrap();
            }
            Ok(Command::Load(
                resource_id,
                known_external_file_shares,
                options,
                on_success,
                on_error,
            )) => {
                let cache = cache.clone();
                std::thread::Builder::new()
                    .name(format!("load-{}", resource_id))
                    .spawn_scoped(s, move || {
                        #[cfg(feature = "test_rotate_after_sync")]
                        std::thread::sleep(std::time::Duration::from_secs(100));
                        let snapshot = snapshot_storage::valid_id(&resource_id);
                        // Acquire admission before downloading/opening primary,
                        // not inside on_success after it already holds a lease.
                        // Include borrowed publications: their import callback
                        // also needs the repository and partition-cache budget.
                        let _import = if snapshot {
                            match cache.admit_import(&|| {
                                crate::helper::SHUTDOWN_FLAG.get() == Some(&true)
                                    || options
                                        .deadline
                                        .is_some_and(|end| end <= std::time::Instant::now())
                            }) {
                                Ok(guard) => Some(guard),
                                Err(error) => {
                                    on_error(error);
                                    return;
                                }
                            }
                        } else {
                            None
                        };
                        tracing::trace!("share_blob: loading {:?}", &resource_id);
                        let mut urls: Vec<url::Url> = known_external_file_shares
                            .into_iter()
                            .map(|e| e.join(&resource_id))
                            .collect::<Result<Vec<_>, _>>()
                            .expect("resource id must be usable as a url path");
                        urls.shuffle(&mut thread_rng());
                        let published = local_storage_share_base_path.join(&resource_id);
                        let borrowed_publication = snapshot && published.is_file();
                        let directory = if snapshot && !borrowed_publication {
                            download_directory
                        } else {
                            local_storage_share_base_path
                        };
                        let local_share_full_path = directory.join(resource_id);
                        let loaded = if snapshot && !borrowed_publication {
                            download_snapshot_blob(
                                &cache,
                                local_share_full_path.file_name().unwrap().to_str().unwrap(),
                                &urls,
                                options.max_tries,
                                options.retry_timeout,
                                options.deadline,
                            )
                            .map(Some)
                        } else {
                            download_blob(
                                &local_share_full_path,
                                directory,
                                &urls,
                                options.max_tries,
                                options.retry_timeout,
                                options.deadline,
                            )
                            .map(|()| None)
                        };
                        match loaded {
                            Ok(lease) => {
                                let load_result = (|| -> anyhow::Result<()> {
                                    if let Some(lease) = lease {
                                        on_success(&mut CacheReader::new(lease)?)
                                    } else {
                                        on_success(&mut std::fs::File::open(
                                            &local_share_full_path,
                                        )?)
                                    }
                                })();
                                if let Err(err) = load_result {
                                    if snapshot && !borrowed_publication {
                                        let id = local_share_full_path
                                            .file_name()
                                            .unwrap()
                                            .to_str()
                                            .unwrap();
                                        if let Err(error) = cache
                                            .acquire(id, 0, &|| {
                                                crate::helper::SHUTDOWN_FLAG.get() == Some(&true)
                                                    || options.deadline.is_some_and(|end| {
                                                        end <= std::time::Instant::now()
                                                    })
                                            })
                                            .and_then(|lease| lease.invalidate())
                                        {
                                            tracing::warn!(
                                                "cannot reject downloaded primary: {error}"
                                            );
                                        }
                                    } else if !borrowed_publication {
                                        let _ = std::fs::remove_file(&local_share_full_path);
                                    }
                                    on_error(err);
                                }
                            }
                            Err(error) => {
                                on_error(error);
                            }
                        }
                    })
                    .unwrap();
            }
        }
    });
}
