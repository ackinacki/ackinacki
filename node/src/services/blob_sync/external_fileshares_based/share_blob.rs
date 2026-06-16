// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;

use tempfile::NamedTempFile;

pub fn share_blob(
    share_full_path: PathBuf,
    tmp_dir_path: &Path,
    data: &mut dyn std::io::Read,
) -> anyhow::Result<()> {
    if share_full_path.exists() {
        return Ok(());
    }
    if !tmp_dir_path.exists() {
        tracing::trace!("share_blob: trying create to parent dir: {tmp_dir_path:?}");
        std::fs::create_dir_all(tmp_dir_path).expect("Failed to create dir for shared state");
    }
    let mut temp = NamedTempFile::new_in(tmp_dir_path)?;
    tracing::trace!("share_blob: created temp file: {:?}", temp.path());
    std::io::copy(data, temp.as_file_mut())?;
    temp.as_file_mut().sync_all()?;
    tracing::trace!("share_blob: persist file: {:?} -> {share_full_path:?}", temp.path());
    temp.persist(share_full_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::share_blob;

    struct FailingReader {
        emitted: bool,
    }

    impl std::io::Read for FailingReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.emitted {
                return Err(std::io::Error::other("synthetic read failure"));
            }
            self.emitted = true;
            let data = b"partial";
            buf[..data.len()].copy_from_slice(data);
            Ok(data.len())
        }
    }

    #[test]
    fn failed_share_removes_temp_file() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let share_full_path = dir.path().join("snapshot");
        let mut reader = FailingReader { emitted: false };

        let err = share_blob(share_full_path.clone(), dir.path(), &mut reader).unwrap_err();

        assert!(err.to_string().contains("synthetic read failure"));
        assert!(!share_full_path.exists());
        let leftovers = std::fs::read_dir(dir.path())?.collect::<Result<Vec<_>, _>>()?;
        assert!(leftovers.is_empty(), "unexpected temp files left after failed share");
        Ok(())
    }
}
