// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use rand::seq::SliceRandom;
use rand::thread_rng;
use telemetry_utils::mpsc::InstrumentedReceiver;

use super::download_blob::download_blob;
use super::share_blob::share_blob;
use super::ResourceId;

type ShareCallback = Box<dyn FnOnce(anyhow::Result<()>) + Send + Sync + 'static>;
type LoadSuccessCallback = Box<dyn FnOnce(&mut dyn std::io::Read) + Send + Sync + 'static>;
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
) {
    let local_storage_share_base_path = &local_storage_share_base_path;
    std::thread::scope(|s| loop {
        match control.recv() {
            Err(std::sync::mpsc::RecvError) => {
                tracing::trace!("Stopping blob sharing service");
                break;
            }
            Ok(Command::Share(resource_id, stream, on_complete)) => {
                std::thread::Builder::new()
                    .name(format!("share-{}", &resource_id))
                    .spawn_scoped(s, move || {
                        tracing::trace!("share_blob: sharing {:?}", &resource_id);
                        let share_full_path = local_storage_share_base_path.join(resource_id);
                        let share_result = {
                            let stream = Box::leak(stream);
                            share_blob(share_full_path, local_storage_share_base_path, stream)
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
                std::thread::Builder::new()
                    .name(format!("load-{}", &resource_id))
                    .spawn_scoped(s, move || {
                        #[cfg(feature = "rotate_after_sync")]
                        std::thread::sleep(std::time::Duration::from_secs(100));
                        tracing::trace!("share_blob: loading {:?}", &resource_id);
                        let mut urls: Vec<url::Url> = known_external_file_shares
                            .into_iter()
                            .map(|e| e.join(&resource_id))
                            .collect::<Result<Vec<_>, _>>()
                            .expect("resource id must be usable as a url path");
                        urls.shuffle(&mut thread_rng());
                        let local_share_full_path = local_storage_share_base_path.join(resource_id);
                        match download_blob(
                            &local_share_full_path,
                            local_storage_share_base_path,
                            &urls,
                            options.max_tries,
                            options.retry_timeout,
                            options.deadline,
                        ) {
                            Ok(()) => {
                                if let Ok(mut file) = std::fs::File::open(local_share_full_path) {
                                    on_success(&mut file);
                                } else {
                                    on_error(anyhow::Error::msg(
                                        "Failed to open a downloaded blob",
                                    ));
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
