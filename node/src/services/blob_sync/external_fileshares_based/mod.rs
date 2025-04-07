// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedSender;
use typed_builder::TypedBuilder;

use super::Blob;
use super::BlobSyncService;
use super::ResourceId;
use crate::helper::metrics::BlockProductionMetrics;

mod download_blob;
mod service_inner_loop;
mod share_blob;

#[derive(TypedBuilder)]
pub struct ExternalFileSharesBased {
    local_storage_share_base_path: PathBuf,
}

pub struct Service {
    inner_loop: std::thread::JoinHandle<()>,
    interface: ServiceInterface,
}

#[derive(Clone)]
pub struct ServiceInterface {
    control: InstrumentedSender<service_inner_loop::Command>,
}

impl ExternalFileSharesBased {
    pub fn start(self, metrics: Option<BlockProductionMetrics>) -> anyhow::Result<Service> {
        let (tx, rx) = instrumented_channel(
            metrics.clone(),
            crate::helper::metrics::BLOB_SYNC_COMMAND_CHANNEL,
        );
        let inner_loop = std::thread::Builder::new()
            .name("External file share service inner loop".to_string())
            .spawn(move || {
                service_inner_loop::service_inner_loop(self.local_storage_share_base_path, rx);
            })?;
        Ok(Service { inner_loop, interface: ServiceInterface { control: tx } })
    }
}
impl Service {
    pub fn interface(&self) -> ServiceInterface {
        self.interface.clone()
    }

    pub fn join(self) {
        drop(self.interface);
        let _ = self.inner_loop.join();
    }
}
impl BlobSyncService for ServiceInterface {
    fn share_blob<Callback>(
        &mut self,
        resource_id: ResourceId,
        blob: impl Blob,
        on_complete: Callback,
    ) -> anyhow::Result<()>
    where
        Callback: FnOnce(anyhow::Result<()>) + Send + Sync + 'static,
    {
        tracing::trace!("share_blob");
        self.control
            .send(service_inner_loop::Command::Share(
                resource_id,
                Box::new(blob.into_read()?),
                Box::new(on_complete),
            ))
            .map_err(anyhow::Error::msg)?;
        Ok(())
    }

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
        ErrorCallback: FnOnce(anyhow::Error) + Send + Sync + 'static,
    {
        tracing::trace!("load_blob");
        let options = service_inner_loop::DownloadOptions {
            max_tries,
            retry_timeout: retry_download_timeout,
            deadline,
        };
        self.control
            .send(service_inner_loop::Command::Load(
                resource_id,
                known_external_blob_share_services,
                options,
                Box::new(on_success),
                Box::new(on_error),
            ))
            .map_err(anyhow::Error::msg)?;
        Ok(())
    }
}
