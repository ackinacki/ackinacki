// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;

use salvo::prelude::*;
// pub type GetBlockFn = Arc<dyn Fn(Vec<u8>) -> anyhow::Result<Vec<u8>> + Send + Sync>;

pub struct StorageLatestHandler {
    pub storage_dir: PathBuf,
}

impl StorageLatestHandler {
    pub fn new(storage_dir: PathBuf) -> Self {
        Self { storage_dir }
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
        let file_name: String = match get_latest_file(&self.storage_dir) {
            Ok(file) => file,
            Err(e) => {
                res.status_code(StatusCode::NOT_FOUND);
                res.body(format!("Error: {e}"));
                return;
            }
        };

        res.render(Redirect::temporary(format!("/v2/storage/{file_name}")));
    }
}

fn get_latest_file(storage_dir: &Path) -> anyhow::Result<String> {
    match snapshot_storage::snapshot_headers(storage_dir)?
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)))
        .map(|(file, _)| file)
    {
        Some(file) => Ok(file),
        None => anyhow::bail!("No file found in storage directory"),
    }
}
