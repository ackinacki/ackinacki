use std::fs;
use std::path::PathBuf;

use anyhow::Context;

pub struct Quarantine {
    dir: PathBuf,
}

impl Quarantine {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
        let dir = path.join("quarantine");
        // Create directory `dir` if it doesn't exist
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create quarantine directory {}", dir.display()))?;

        // Check writability by creating and removing a temporary file.
        let test_path = dir.join(".quarantine_write_test");
        fs::write(&test_path, b"ok").with_context(|| {
            format!("failed to create quarantine test file {}", test_path.display())
        })?;
        fs::remove_file(&test_path).with_context(|| {
            format!("failed to remove quarantine test file {}", test_path.display())
        })?;

        Ok(Self { dir })
    }

    pub fn store(&self, file_name: &str, data: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let path = self.dir.join(file_name);
        fs::write(&path, data.as_ref())
            .with_context(|| format!("failed to create quarantine file {}", path.display()))?;
        Ok(())
    }

    pub fn read(&self, file_name: &str) -> anyhow::Result<Vec<u8>> {
        let path = self.dir.join(file_name);
        let data = fs::read(&path)
            .with_context(|| format!("failed to read quarantine file {}", path.display()))?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use super::*;

    fn uniq_name(seed: &str) -> String {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        format!("{seed}_{}", since_epoch.as_nanos())
    }

    #[test]
    fn new_creates_dir_and_is_writable() -> anyhow::Result<()> {
        let base = tempfile::tempdir().unwrap().path().join(uniq_name("a"));

        // ensure parent exists
        std::fs::create_dir_all(&base)?;

        let q = Quarantine::new(base.clone())?;
        assert!(q.dir.exists());

        q.store("foo.txt", b"hello")?;
        let data = q.read("foo.txt")?;
        assert_eq!(data, b"hello");
        Ok(())
    }

    #[test]
    fn new_fails_if_parent_not_writable() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let base = tempfile::tempdir().unwrap().path().join(uniq_name("b"));
        std::fs::create_dir_all(&base)?;

        // remove write bit from parent so creating subdir should fail
        let mut perms = std::fs::metadata(&base)?.permissions();
        perms.set_mode(0o500);
        std::fs::set_permissions(&base, perms)?;

        let res = Quarantine::new(base.clone());
        assert!(res.is_err(), "expected Quarantine::new to fail on non-writable parent");
        Ok(())
    }
}
