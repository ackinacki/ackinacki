// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use http_server::ApiBkSet;
use network::topology::NetTopology;
use node_types::AccountIdentifier;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedSender;
use tokio::sync::watch::Receiver;
use url::Url;

use crate::helper::SHUTDOWN_FLAG;
use crate::node::services::sync::snapshot_compression::COMPRESSED_SNAPSHOT_MAGIC;
use crate::node::services::sync::FileSavingService;
use crate::node::services::sync::StateSyncService;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::services::blob_sync::external_fileshares_based::ServiceInterface;
use crate::services::blob_sync::BlobSyncService;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;

#[derive(Clone)]
pub struct ExternalFileSharesBased {
    pub static_storages: Vec<url::Url>,
    pub max_download_tries: u8,
    pub retry_download_timeout: std::time::Duration,
    pub download_deadline_timeout: std::time::Duration,
    blob_sync: ServiceInterface,
    file_saving_service: FileSavingService,
    state_load_thread: Arc<Mutex<Option<JoinHandle<()>>>>,
    net_topology_rx: Receiver<NetTopology<NodeIdentifier>>,
    bk_set_rx: tokio::sync::watch::Receiver<ApiBkSet>,
}

impl AllowGuardedMut for Option<JoinHandle<()>> {}

impl ExternalFileSharesBased {
    pub fn new(
        blob_sync: ServiceInterface,
        file_saving_service: FileSavingService,
        net_topology_rx: Receiver<NetTopology<NodeIdentifier>>,
        bk_set_rx: tokio::sync::watch::Receiver<ApiBkSet>,
    ) -> Self {
        // TODO: move to config
        Self {
            static_storages: vec![],
            max_download_tries: 30,
            retry_download_timeout: Duration::from_secs(2),
            download_deadline_timeout: Duration::from_secs(600),
            blob_sync,
            file_saving_service,
            state_load_thread: Arc::new(Mutex::new(None)),
            net_topology_rx,
            bk_set_rx,
        }
    }
}

impl StateSyncService for ExternalFileSharesBased {
    type Repository = RepositoryImpl;

    fn save_state_for_sharing(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Arc<OptimisticStateImpl>>,
        finalizing_block_id: BlockIdentifier,
    ) -> anyhow::Result<()> {
        tracing::trace!("save_state_for_sharing: {:?}", block_id);
        let file_name = PathBuf::from(block_id.to_string());
        self.file_saving_service.save_object(
            block_id,
            thread_id,
            min_state,
            file_name,
            finalizing_block_id,
        )
    }

    // TODO: load state thread needs timeout guard to prevent thread stuck on "infinite" size state
    fn add_load_state_task(
        &mut self,
        resource_address: BTreeMap<ThreadIdentifier, BlockIdentifier>,
        repository: RepositoryImpl,
        output: InstrumentedSender<anyhow::Result<BTreeMap<ThreadIdentifier, BlockIdentifier>>>,
    ) -> anyhow::Result<()> {
        let mut thread = self.state_load_thread.lock();
        if let Some(thread) = thread.as_ref() {
            if !thread.is_finished() {
                tracing::trace!("add_load_state_task: skip. state is already downloading");
                return Ok(());
            }
        }
        let metrics = repository.get_metrics().cloned();
        let repo = Arc::new(Mutex::new(repository.clone()));
        tracing::trace!("add_load_state_task: adding {resource_address:?}");
        let checker = Arc::new(Mutex::new(resource_address.clone()));
        let current_bk_set_node_ids = self
            .bk_set_rx
            .borrow()
            .current
            .iter()
            .map(|bk| NodeIdentifier::from(AccountIdentifier::new(bk.owner_address.0)))
            .collect::<HashSet<_>>();
        let last_load_error = Arc::new(Mutex::new(None::<Result<(), anyhow::Error>>));
        for (thread_id, block_id) in resource_address.clone() {
            let output_clone = output.clone();
            let checker_clone = checker.clone();
            let checker_clone2 = checker.clone();
            let repo_clone = repo.clone();
            let external_blob_share_services = {
                let mut services = HashSet::<Url>::from_iter(self.static_storages.iter().cloned());
                for (node_id, peers) in self.net_topology_rx.borrow().peer_resolver() {
                    if current_bk_set_node_ids.contains(node_id) {
                        for peer in peers {
                            if let Some(base_url) = &peer.bk_api_url_for_storage_sync {
                                // TODO: make it connected to http-server settings so that it's not hardcoded
                                if let Ok(url) = base_url.join("v2/storage/") {
                                    services.insert(url);
                                }
                            }
                        }
                    }
                }
                Vec::from_iter(services)
            };
            if let Some(m) = metrics.as_ref() {
                m.report_state_request()
            }
            let metrics_on_success = metrics.clone();
            let metrics_on_error = metrics.clone();

            self.blob_sync.load_blob(
                block_id.to_string(),
                external_blob_share_services,
                self.max_download_tries,
                Some(self.retry_download_timeout),
                Some(std::time::Instant::now() + self.download_deadline_timeout),
                {
                    move |e| {
                        let mut buffer: Vec<u8> = vec![];
                        match e.read_to_end(&mut buffer) {
                            Ok(_size) => {
                                // Transparent decompression: detect magic header
                                let payload = if buffer.starts_with(COMPRESSED_SNAPSHOT_MAGIC) {
                                    match zstd::decode_all(&buffer[COMPRESSED_SNAPSHOT_MAGIC.len()..]) {
                                        Ok(decompressed) => {
                                            tracing::trace!(
                                                "add_load_state_task: decompressed snapshot for {thread_id:?} (compressed={} decompressed={})",
                                                buffer.len(), decompressed.len()
                                            );
                                            decompressed
                                        }
                                        Err(e) => {
                                            let _ = output_clone.send(Err(e.into()));
                                            return;
                                        }
                                    }
                                } else {
                                    // Legacy uncompressed snapshot — use as-is
                                    buffer
                                };

                                let res = repo_clone.lock().set_state_from_snapshot(
                                    payload,
                                    &ThreadIdentifier::default(),
                                    Arc::new(Mutex::new(HashSet::new())),
                                );
                                tracing::trace!(
                                    "add_load_state_task: for {thread_id:?} res={res:?}"
                                );
                                if let Err(e) = res {
                                    let _ = output_clone.send(Err(e));
                                } else {
                                    tracing::trace!("add_load_state_task: done for {thread_id:?}");
                                    checker_clone.lock().remove(&thread_id);
                                }
                            }
                            Err(e) => {
                                if let Some(m) = metrics_on_success {
                                    m.report_error("load_state_fail");
                                }
                                let _ = output_clone.send(Err(e.into()));
                            }
                        }
                    }
                },
                {
                    let output_clone = output.clone();
                    let last_load_error_clone = last_load_error.clone();
                    move |e| {
                        tracing::trace!(target: "node", "Load state thread failed: {e}");
                        // Handle error
                        *last_load_error_clone.lock() = Some(Err(anyhow::format_err!("Load state thread failed: {e}")));
                        let _ = output_clone.send(Err(e));
                        checker_clone2.lock().clear();
                        if let Some(m) = metrics_on_error {
                            m.report_error("load_state_error");
                        }
                    }
                },
            )?;
        }
        let spawned = std::thread::Builder::new().name("State load".to_string()).spawn_critical(
            move || loop {
                if SHUTDOWN_FLAG.get() == Some(&true) {
                    return Ok(());
                }
                let checker = checker.lock();
                if checker.is_empty() {
                    if let Some(err) = last_load_error.lock().take() {
                        return err;
                    }
                    let _ = output.send(Ok(resource_address));
                    return Ok(());
                }
                drop(checker);
                std::thread::sleep(Duration::from_millis(50));
            },
        )?;
        *thread = Some(spawned);
        Ok(())
    }

    fn is_load_thread_available(&self) -> bool {
        self.state_load_thread.guarded_mut(|load_thread| {
            if load_thread.is_none() {
                return true;
            }
            if load_thread.as_ref().map(|t| t.is_finished()).unwrap_or(false) {
                let res = load_thread.take().unwrap().join();
                tracing::trace!(target: "node", "load state thread finished with res: {res:?}");
                if res.is_err() {
                    true
                } else {
                    false // do not start new load, use prepared result
                }
            } else {
                false
            }
        })
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.file_saving_service.flush()
    }
}
