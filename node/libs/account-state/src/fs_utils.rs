use std::path::Path;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub(crate) fn read<T: DeserializeOwned>(
    root_path: impl AsRef<Path>,
    name: impl AsRef<str>,
) -> anyhow::Result<Option<T>> {
    let file_path = root_path.as_ref().join(name.as_ref());
    if !file_path.exists() {
        return Ok(None);
    }
    let t = bincode::deserialize_from(std::fs::File::open(file_path)?)?;
    Ok(Some(t))
}

pub(crate) fn write(
    root_path: impl AsRef<Path>,
    name: impl AsRef<str>,
    data: Option<&impl Serialize>,
) -> anyhow::Result<()> {
    let file_path = root_path.as_ref().join(name.as_ref());
    if let Some(data) = data {
        if !file_path.exists() {
            let tmp_file_path = tempfile::NamedTempFile::new_in(root_path)?;
            bincode::serialize_into(tmp_file_path.as_file(), data)?;
            match tmp_file_path.persist(file_path) {
                Ok(_) => {}
                Err(e) => {
                    if e.error.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(e.into());
                    }
                }
            }
        }
    } else if file_path.exists() {
        match std::fs::remove_file(file_path) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn fs_repo<Repository>(
    factory: impl FnOnce(PathBuf) -> anyhow::Result<Repository>,
    root_path: PathBuf,
) -> anyhow::Result<Repository> {
    std::fs::create_dir_all(&root_path)?;
    factory(root_path)
}
