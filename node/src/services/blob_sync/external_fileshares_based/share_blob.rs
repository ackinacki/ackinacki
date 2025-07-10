// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;

use crate::helper::get_temp_file_path;

pub fn share_blob(
    share_full_path: PathBuf,
    tmp_dir_path: &Path,
    data: &mut dyn std::io::Read,
) -> anyhow::Result<()> {
    if share_full_path.exists() {
        return Ok(());
    }
    let tmp_file_path = get_temp_file_path(tmp_dir_path);
    tracing::trace!("share_blob: trying to create file: {tmp_file_path:?}");
    if let Some(parent) = tmp_file_path.parent() {
        if !parent.exists() {
            tracing::trace!("share_blob: trying create to parent dir: {parent:?}");
            std::fs::create_dir_all(parent).expect("Failed to create dir for shared state");
        }
    }
    let mut file = std::fs::File::create(tmp_file_path.clone())?;
    std::io::copy(data, &mut file)?;
    file.sync_all()?;
    tracing::trace!("share_blob: rename file: {tmp_file_path:?} -> {share_full_path:?}");
    std::fs::rename(tmp_file_path, share_full_path)?;
    Ok(())
}
