// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

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
        let file_name: String = match get_latest_file(&self.storage_dir.clone()) {
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

fn get_latest_file(storage_dir: &PathBuf) -> anyhow::Result<String> {
    match std::fs::read_dir(storage_dir)?
        .flatten()
        .filter_map(|e| match e.metadata() {
            Ok(metadata) if metadata.is_file() => Some((e.file_name(), metadata.modified())),
            _ => None,
        })
        .filter_map(|v| match v.1 {
            Ok(modified) => Some((v.0, modified)),
            _ => None,
        })
        .max_by_key(|&(_, modified)| modified)
        .map(|(file, _)| file.to_string_lossy().to_string())
    {
        Some(file) => Ok(file),
        None => anyhow::bail!("No file found in storage directory"),
    }
}
