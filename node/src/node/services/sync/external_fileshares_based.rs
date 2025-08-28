// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use chitchat::ChitchatRef;
use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedSender;
use url::Url;

use crate::helper::SHUTDOWN_FLAG;
use crate::node::services::sync::FileSavingService;
use crate::node::services::sync::StateSyncService;
use crate::node::services::sync::GOSSIP_API_ADVERTISE_ADDR_KEY;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::services::blob_sync::external_fileshares_based::ServiceInterface;
use crate::services::blob_sync::BlobSyncService;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
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
    chitchat: ChitchatRef,
}

impl ExternalFileSharesBased {
    pub fn new(
        blob_sync: ServiceInterface,
        file_saving_service: FileSavingService,
        chitchat: ChitchatRef,
    ) -> Self {
        // TODO: move to config
        Self {
            static_storages: vec![],
            max_download_tries: 3,
            retry_download_timeout: Duration::from_secs(2),
            download_deadline_timeout: Duration::from_secs(120),
            blob_sync,
            file_saving_service,
            state_load_thread: Arc::new(Mutex::new(None)),
            chitchat,
        }
    }
}

impl StateSyncService for ExternalFileSharesBased {
    type Repository = RepositoryImpl;

    fn save_state_for_sharing(&self, state: Arc<OptimisticStateImpl>) -> anyhow::Result<()> {
        let block_id = state.block_id.clone();
        tracing::trace!("save_state_for_sharing: {:?}", block_id);
        let file_name = PathBuf::from(block_id.to_string());
        self.file_saving_service.save_object(state, file_name.clone())
    }

    fn reset_sync(&self) {
        self.state_load_thread.lock().take();
    }

    fn add_load_state_task(
        &mut self,
        resource_address: HashMap<ThreadIdentifier, BlockIdentifier>,
        repository: RepositoryImpl,
        output: InstrumentedSender<anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        let mut thread = self.state_load_thread.lock();
        if let Some(thread) = thread.as_ref() {
            if !thread.is_finished() {
                tracing::trace!("add_load_state_task: skip. state is already downloading");
                return Ok(());
            }
        }
        let repo = Arc::new(Mutex::new(repository.clone()));
        tracing::trace!("add_load_state_task: adding {resource_address:?}");
        let checker = Arc::new(Mutex::new(resource_address.clone()));
        for (thread_id, block_id) in resource_address {
            let output_clone = output.clone();
            let checker_clone = checker.clone();
            let repo_clone = repo.clone();
            let external_blob_share_services = {
                let mut services = HashSet::<Url>::from_iter(self.static_storages.iter().cloned());
                Extend::extend(
                    &mut services,
                    self.chitchat
                    .lock()
                    .state_snapshot()
                    .node_states
                    .iter()
                    .flat_map(|node| node.get(GOSSIP_API_ADVERTISE_ADDR_KEY))
                    .flat_map(|raw_url| Url::parse(raw_url).ok())
                    // TODO: make it connected to http-server settings so that it's not hardcoded
                    .flat_map(|url| url.join("v2/storage/").ok()),
                );
                services.drain().collect()
            };
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
                                let res = repo_clone.lock().set_state_from_snapshot(
                                    buffer,
                                    &ThreadIdentifier::default(),
                                    Arc::new(Mutex::new(HashSet::new())),
                                );
                                tracing::trace!(
                                    "add_load_state_task: for {thread_id:?} res={res:?}"
                                );
                                res.expect("Failed to set state from snapshot");
                                tracing::trace!("add_load_state_task: done for {thread_id:?}");
                                checker_clone.lock().remove(&thread_id);
                            }
                            Err(e) => {
                                let _ = output_clone.send(Err(e.into()));
                            }
                        }
                    }
                },
                {
                    let output_clone = output.clone();
                    move |e| {
                        // Handle error
                        let _ = output_clone.send(Err(e));
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
                    let _ = output.send(Ok(()));
                    return Ok(());
                }
                drop(checker);
                std::thread::sleep(Duration::from_millis(50));
            },
        )?;
        *thread = Some(spawned);
        Ok(())
    }
}
