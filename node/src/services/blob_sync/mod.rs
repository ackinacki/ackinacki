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
        SuccessCallback: FnOnce(&mut dyn std::io::Read) + Send + Sync + 'static,
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
            .start()
            .expect("should be able to start");
        example_usecase(service.interface());
    }
}
