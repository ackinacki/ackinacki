// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use salvo::prelude::*;
use snapshot_storage::SnapshotResources;
// pub type GetBlockFn = Arc<dyn Fn(Vec<u8>) -> anyhow::Result<Vec<u8>> + Send + Sync>;

pub struct StorageLatestHandler {
    pub resources: SnapshotResources,
}

impl StorageLatestHandler {
    pub fn new(resources: SnapshotResources) -> Self {
        Self { resources }
    }
}

#[async_trait]
impl Handler for StorageLatestHandler {
    async fn handle(
        &self,
        _req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let resources = self.resources.clone();
        // GC holds this mutex while scanning/deleting/fsyncing resources.
        // Waiting for it and walking the directory must not occupy an async
        // runtime worker, just as in the /storage/ listing handler.
        let latest = tokio::task::spawn_blocking(move || get_latest_file(&resources)).await;
        let file_name = match latest {
            Ok(Ok(file)) => file,
            Ok(Err(e)) => {
                res.status_code(StatusCode::NOT_FOUND);
                res.body(format!("Error: {e}"));
                return;
            }
            Err(e) => {
                res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                res.body(format!("Error: {e}"));
                return;
            }
        };

        res.render(Redirect::temporary(format!("/v2/storage/{file_name}")));
    }
}

fn get_latest_file(resources: &SnapshotResources) -> anyhow::Result<String> {
    match resources
        .headers()?
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)))
        .map(|(file, _)| file)
    {
        Some(file) => Ok(file),
        None => anyhow::bail!("No file found in storage directory"),
    }
}
