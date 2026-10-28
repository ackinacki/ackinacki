// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod external_fileshares_based;

pub type ResourceId = String;

pub trait Blob {
    fn into_read(self) -> anyhow::Result<impl std::io::Read + Send + Sync + 'static>;
}

pub trait BlobSyncService {
    fn share_blob<Callback>(
        &mut self,
        resource_id: ResourceId,
        blob: impl Blob,
        on_complete: Callback,
    ) -> anyhow::Result<()>
    where
        Callback: FnOnce(anyhow::Result<()>) + Send + Sync + 'static;

    #[allow(clippy::too_many_arguments)]
    fn load_blob<SuccessCallback, ErrorCallback>(
        &mut self,
        resource_id: ResourceId,
        known_external_blob_share_services: Vec<url::Url>,
        max_tries: u8,
        retry_download_timeout: Option<std::time::Duration>,
        deadline: Option<std::time::Instant>,
        on_success: SuccessCallback,
        on_error: ErrorCallback,
    ) -> anyhow::Result<()>
    where
        SuccessCallback:
            FnOnce(&mut dyn std::io::Read) -> anyhow::Result<()> + Send + Sync + 'static,
        ErrorCallback: FnOnce(anyhow::Error) + Send + Sync + 'static;
}

impl<T> Blob for T
where
    T: std::io::Read + Send + Sync + 'static,
{
    fn into_read(self) -> anyhow::Result<impl std::io::Read + Send + Sync + 'static> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ExampleUsecase {
        data: std::sync::Arc<std::sync::Mutex<std::vec::Vec<i32>>>,
    }
    impl ExampleUsecase {
        pub fn new() -> Self {
            let data = std::sync::Arc::new(std::sync::Mutex::new(vec![1, 2, 3]));
            Self { data }
        }
    }

    impl Blob for ExampleUsecase {
        fn into_read(self) -> anyhow::Result<impl std::io::Read + Send + Sync + 'static> {
            let Ok(guarded) = self.data.lock() else {
                anyhow::bail!("failed to aquire lock");
            };
            let data: Vec<i32> = guarded.clone();
            // Example
            let cursor = std::io::Cursor::new(
                data.into_iter().flat_map(|e| e.to_be_bytes()).collect::<Vec<u8>>(),
            );
            Ok(cursor)
        }
    }

    #[allow(unused_labels)]
    fn example_usecase<T>(mut service: T)
    where
        T: BlobSyncService,
    {
        let tmp_dir = tempfile::tempdir().unwrap();
        std::env::set_current_dir(&tmp_dir).unwrap();
        let some_resource_id = "some-resource-id".to_string();
        'share_blob: {
            let (tx, rx) = std::sync::mpsc::channel();
            let some_blob_source = ExampleUsecase::new();
            const MARKER: u32 = 1;
            service
                .share_blob(some_resource_id.clone(), some_blob_source, move |_e| {
                    tx.send(MARKER).unwrap();
                })
                .expect("ok");
            let data = rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("Must be able to finish a lot faster than a second");
            assert!(data == MARKER);
        }
        'must_be_able_to_access_own_blob: {
            let (tx, rx) = std::sync::mpsc::channel();
            let tx_ok = tx.clone();
            let tx_fail = tx;
            const MARKER_SUCCESS: u32 = 1;
            const MARKER_FAILED: u32 = 2;
            service
                .load_blob(
                    some_resource_id,
                    vec![],
                    1,
                    None,
                    None,
                    move |e| {
                        let mut buffer = Vec::new();
                        e.read_to_end(&mut buffer).expect("read to end on a blob");
                        let data: Vec<_> = buffer
                            .chunks(4)
                            .map(|e| i32::from_be_bytes(<[u8; 4]>::try_from(e).unwrap()))
                            .collect();
                        tx_ok.send((MARKER_SUCCESS, data)).unwrap();
                        Ok(())
                    },
                    move |_e| {
                        tx_fail.send((MARKER_FAILED, vec![])).unwrap();
                    },
                )
                .expect("ok");
            let (marker, data) = rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("Read local must be able to finish a lot faster than a second");
            assert!(marker == MARKER_SUCCESS);
            assert!(data == [1i32, 2i32, 3i32])
        }
        // TODO: add an integration test to check blob remote access
    }

    #[test]
    fn ensure_basic_flow_for_external_fileshares_based_service() {
        let service = external_fileshares_based::ExternalFileSharesBased::builder()
            .local_storage_share_base_path("./tmp".into())
            .build()
            .start(None)
            .expect("should be able to start");
        example_usecase(service.interface());
    }

    #[test]
    fn queued_primaries_do_not_pin_cache_while_waiting_for_repository() -> anyhow::Result<()> {
        use std::collections::HashMap;
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::time::Duration;
        use std::time::Instant;

        use external_fileshares_based::download_cache::DownloadCache;
        use parking_lot::Mutex;

        use crate::repository::repository_impl::snapshot_http_import::tests::Server;

        for borrowed in [false, true] {
            let directory = tempfile::tempdir()?;
            let private = directory.path().join(".snapshot-downloads");
            std::fs::create_dir(&private)?;
            let mut bodies = HashMap::new();
            for index in [1, 2, 3] {
                let id = hex::encode([index; 32]);
                let body = vec![index; 20];
                std::fs::write(private.join(&id), &body)?;
                bodies.insert(id, body);
            }
            let primary = hex::encode([1; 32]);
            if borrowed {
                std::fs::write(directory.path().join(&primary), [1; 20])?;
            }
            let server = Server::new(bodies, primary.clone(), None, None, None);
            let service = external_fileshares_based::ExternalFileSharesBased::builder()
                .local_storage_share_base_path(directory.path().to_owned())
                .download_cache_bytes(60)
                .build()
                .start(None)?;
            let cache = DownloadCache::open(&private, None)?;
            // Hold admission while all three loads are dispatched. No callback
            // may acquire a primary lease and wait on the repository behind us.
            let admission = cache.admit_import(&|| false)?;
            let repository = Arc::new(Mutex::new(()));
            let mut interface = service.interface();
            let (tx, rx) = mpsc::channel();
            let deadline = Instant::now() + Duration::from_secs(5);
            for index in [1, 2, 3] {
                let (entered, failed) = (tx.clone(), tx.clone());
                let (cache, repository) = (cache.clone(), repository.clone());
                interface.load_blob(
                    hex::encode([index; 32]),
                    vec![server.base.clone()],
                    1,
                    None,
                    Some(deadline),
                    move |reader| {
                        entered.send(Ok(false))?;
                        let _repo = repository.lock();
                        let mut body = Vec::new();
                        reader.read_to_end(&mut body)?;
                        assert_eq!(body, vec![index; 20]);
                        // Analogous to partition import: need almost the entire
                        // budget while holding the repository mutex. Other
                        // queued primaries must remain evictable, not leased.
                        let partition =
                            cache.acquire(&hex::encode([index + 10; 32]), 50, &|| {
                                Instant::now() >= deadline
                            })?;
                        std::fs::write(partition.path(), [0; 50])?;
                        partition.accepted()?;
                        drop(partition);
                        entered.send(Ok(true))?;
                        Ok(())
                    },
                    move |error| {
                        let _ = failed.send(Err(error.to_string()));
                    },
                )?;
            }
            assert!(matches!(
                rx.recv_timeout(Duration::from_millis(200)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            drop(admission);
            let mut completed = 0;
            for _ in 0..6 {
                match rx.recv_timeout(Duration::from_secs(5))? {
                    Ok(done) => completed += usize::from(done),
                    Err(error) => anyhow::bail!("queued primary failed: {error}"),
                }
            }
            assert_eq!(completed, 3);
            if borrowed {
                assert_eq!(std::fs::read(directory.path().join(primary))?, [1; 20]);
            }
            drop(interface);
            service.join();
        }
        Ok(())
    }

    #[test]
    fn snapshot_consumer_failure_preserves_published_resource() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let id = "ab".repeat(32);
        let path = directory.path().join(&id);
        std::fs::write(&path, [1, 2, 3])?;
        let service = external_fileshares_based::ExternalFileSharesBased::builder()
            .local_storage_share_base_path(directory.path().to_owned())
            .build()
            .start(None)?;
        let mut interface = service.interface();
        let (tx, rx) = std::sync::mpsc::channel();
        interface.load_blob(
            id,
            vec![],
            1,
            None,
            None,
            |_| anyhow::bail!("rejected snapshot"),
            move |error| {
                tx.send(error.to_string()).unwrap();
            },
        )?;
        assert!(rx.recv_timeout(std::time::Duration::from_secs(5))?.contains("rejected snapshot"));
        assert_eq!(std::fs::read(path)?, [1, 2, 3]);
        drop(interface);
        service.join();
        Ok(())
    }

    #[test]
    fn load_blob_removes_cached_file_when_consumer_fails() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let base_path = tmp_dir.path().join("cache");
        let service = external_fileshares_based::ExternalFileSharesBased::builder()
            .local_storage_share_base_path(base_path.clone())
            .build()
            .start(None)
            .expect("should be able to start");
        let mut interface = service.interface();
        let resource_id = "corrupt-snapshot".to_string();
        let cached_path = base_path.join(&resource_id);

        let (share_tx, share_rx) = std::sync::mpsc::channel();
        interface
            .share_blob(resource_id.clone(), std::io::Cursor::new(vec![1u8, 2, 3]), move |res| {
                share_tx.send(res.map(|_| ())).unwrap();
            })
            .unwrap();
        share_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("share callback must complete")
            .unwrap();
        assert!(cached_path.exists());

        let (load_tx, load_rx) = std::sync::mpsc::channel();
        interface
            .load_blob(
                resource_id,
                vec![],
                1,
                None,
                None,
                |_reader| Err(anyhow::anyhow!("snapshot processing failed")),
                move |err| {
                    load_tx.send(err.to_string()).unwrap();
                },
            )
            .unwrap();

        let err = load_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("load error callback must complete");
        assert!(err.contains("snapshot processing failed"));
        assert!(!cached_path.exists());
    }
}
