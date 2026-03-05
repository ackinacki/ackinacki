// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::File;
use std::fs::{self};
use std::io::BufReader;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use flate2::write::GzEncoder;
use flate2::Compression;
use flate2::GzBuilder;
use xz2::write::XzEncoder;

use crate::domain::grouping::ArchiveFile;
use crate::domain::traits::CompressionMode;
use crate::domain::traits::FileSystemClient;
use crate::utils::parse_timestamp_from_filename;

pub struct FileSystemClientImpl {
    incoming_dir: PathBuf,
}

impl FileSystemClientImpl {
    pub fn new(incoming_dir: PathBuf) -> Self {
        FileSystemClientImpl { incoming_dir }
    }
}

// This fn returns BTreeMap<ServerId, Vec<ArchiveFile>>
impl FileSystemClient for FileSystemClientImpl {
    fn get_arch_files(&self) -> anyhow::Result<BTreeMap<String, Vec<ArchiveFile>>> {
        // collect BM dirs -> [(ts, path)]
        let mut per_server: BTreeMap<String, Vec<ArchiveFile>> = BTreeMap::new();

        for entry in fs::read_dir(self.incoming_dir.clone())
            .with_context(|| format!("Failed to read folder '{}'", self.incoming_dir.display()))?
        {
            // We only search for directories whose names contain only the numbers
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }
            let server_id = match path.file_name().and_then(OsStr::to_str) {
                Some(name) if name.chars().all(|c| c.is_ascii_digit()) => name.to_string(),
                _ => continue, // ignore unnumbered dirs
            };

            // and now we only search for files whose names contain timestamp
            let mut files = Vec::new();
            for f in fs::read_dir(&path)
                .with_context(|| format!("Failed to read '{}'", path.display()))?
            {
                let file_path = f?.path();
                if !file_path.is_file() {
                    continue;
                }
                if let Some(ts) = parse_timestamp_from_filename(&file_path) {
                    files.push(ArchiveFile { _bm_id: server_id.clone(), ts, path: file_path });
                }
            }

            per_server.insert(server_id, files);
        }

        Ok(per_server)
    }

    fn move_processed(
        &self,
        src_db_path: impl AsRef<Path>,
        processed_root: impl AsRef<Path>,
        compression: CompressionMode,
        dry_run: bool,
    ) -> anyhow::Result<PathBuf> {
        let src_db_path = src_db_path.as_ref();
        let processed_root = processed_root.as_ref();

        // bm-archive-<TIMESTAMP>.db
        let file_name = src_db_path.file_name().ok_or_else(|| {
            anyhow::anyhow!("source path has no file name: {}", src_db_path.display())
        })?;

        let num_dir = src_db_path.parent().and_then(|p| p.file_name()).ok_or_else(|| {
            anyhow::anyhow!("cannot infer <NUM> directory from {}", src_db_path.display())
        })?;

        let dest_dir = processed_root.join(num_dir);
        fs::create_dir_all(&dest_dir)
            .with_context(|| format!("failed to create dir {}", dest_dir.display()))?;

        let dest_file_name: PathBuf = match compression {
            CompressionMode::None => PathBuf::from(file_name),
            CompressionMode::Gzip => {
                let mut s = file_name.to_os_string();
                s.push(".gz");
                PathBuf::from(s)
            }
            CompressionMode::Xz => {
                let mut s = file_name.to_os_string();
                s.push(".xz");
                PathBuf::from(s)
            }
        };

        let dest_path = dest_dir.join(dest_file_name);

        match compression {
            CompressionMode::None => move_file(src_db_path, &dest_path, dry_run)?,
            CompressionMode::Gzip => gzip_move(src_db_path, &dest_path, dry_run)?,
            CompressionMode::Xz => xz_move(src_db_path, &dest_path, dry_run)?,
        }

        Ok(dest_path)
    }
}

// Compresses a file with gzip and moves it to the destination.
// Uses atomic operations to prevent data loss.
fn gzip_move(src: &Path, dest_gz: &Path, dry_run: bool) -> Result<()> {
    let started_at = Instant::now();
    let tmp_path = {
        let mut tmp = dest_gz.as_os_str().to_os_string();
        tmp.push(".part");
        PathBuf::from(tmp)
    };

    // force clean up any leftover temp file
    let _ = fs::remove_file(&tmp_path);

    let src_file = File::open(src)
        .with_context(|| format!("failed to open source file: {}", src.display()))?;

    // get mtime from source file
    let mtime_secs = src_file
        .metadata()
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as u32)
        .unwrap_or_else(|| {
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as u32).unwrap_or(0)
        });

    // create temp output file
    let tmp_file = File::create(&tmp_path)
        .with_context(|| format!("failed to create temp file: {}", tmp_path.display()))?;

    // extract original filename without .gz extension for gzip header
    let filename_in_gz =
        dest_gz.file_stem().and_then(|s| s.to_str()).unwrap_or("archive.db").to_string();

    let gz_builder = GzBuilder::new().mtime(mtime_secs).filename(filename_in_gz);

    let mut encoder: GzEncoder<File> = gz_builder.write(tmp_file, Compression::default());

    // todo: tune buffer size
    let mut reader = BufReader::with_capacity(2 << 20, src_file); // 2 MiB buffer
    io::copy(&mut reader, &mut encoder).context("failed to compress file")?;

    let out_file = encoder.finish().context("failed to finalize gzip compression")?;
    out_file.sync_all().context("failed to sync compressed file")?;
    drop(out_file);

    if dest_gz.exists() {
        fs::remove_file(dest_gz)
            .with_context(|| format!("failed to remove existing file: {}", dest_gz.display()))?;
    }

    fs::rename(&tmp_path, dest_gz).with_context(|| {
        format!("failed to rename {} -> {}", tmp_path.display(), dest_gz.display())
    })?;

    if !dry_run {
        fs::remove_file(src)
            .with_context(|| format!("failed to remove source file: {}", src.display()))?;
    }

    tracing::info!(
        src = %src.display(),
        dest = %dest_gz.display(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "gzip move completed"
    );

    Ok(())
}

// Compresses a file with xz and moves it to the destination.
// Uses atomic operations to prevent data loss.
fn xz_move(src: &Path, dest_xz: &Path, dry_run: bool) -> Result<()> {
    let started_at = Instant::now();
    let tmp_path = {
        let mut tmp = dest_xz.as_os_str().to_os_string();
        tmp.push(".part");
        PathBuf::from(tmp)
    };

    // force clean up any leftover temp file
    let _ = fs::remove_file(&tmp_path);

    let src_file = File::open(src)
        .with_context(|| format!("failed to open source file: {}", src.display()))?;

    // create temp output file
    let tmp_file = File::create(&tmp_path)
        .with_context(|| format!("failed to create temp file: {}", tmp_path.display()))?;

    let mut encoder = XzEncoder::new(tmp_file, 6);

    // todo: tune buffer size
    let mut reader = BufReader::with_capacity(2 << 20, src_file); // 2 MiB buffer
    io::copy(&mut reader, &mut encoder).context("failed to compress file")?;

    let out_file = encoder.finish().context("failed to finalize xz compression")?;
    out_file.sync_all().context("failed to sync compressed file")?;
    drop(out_file);

    if dest_xz.exists() {
        fs::remove_file(dest_xz)
            .with_context(|| format!("failed to remove existing file: {}", dest_xz.display()))?;
    }

    fs::rename(&tmp_path, dest_xz).with_context(|| {
        format!("failed to rename {} -> {}", tmp_path.display(), dest_xz.display())
    })?;

    if !dry_run {
        fs::remove_file(src)
            .with_context(|| format!("failed to remove source file: {}", src.display()))?;
    }

    tracing::info!(
        src = %src.display(),
        dest = %dest_xz.display(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "xz move completed"
    );

    Ok(())
}

// Moves a file, with fallback to copy+delete for cross-device moves.
fn move_file(src: &Path, dest: &Path, dry_run: bool) -> Result<()> {
    let Err(err) = fs::rename(src, dest) else { return Ok(()) };

    if !is_cross_device_error(&err) {
        return Err(err).context("failed to move file");
    }

    // Cross-device move: copy -> sync -> delete
    fs::copy(src, dest)
        .with_context(|| format!("failed to copy {} -> {}", src.display(), dest.display()))?;

    // Sync the copied file
    if let Ok(f) = File::open(dest) {
        f.sync_all().ok(); // Best effort
    }

    if !dry_run {
        // Remove source only after successful copy
        fs::remove_file(src)
            .with_context(|| format!("failed to remove source after copy: {}", src.display()))?;
    }

    Ok(())
}

/// Checks if an I/O error is a cross-device link error (EXDEV)
#[inline]
fn is_cross_device_error(e: &io::Error) -> bool {
    #[cfg(unix)]
    {
        e.raw_os_error() == Some(18)
    }
    #[cfg(windows)]
    {
        // ERROR_NOT_SAME_DEVICE = 17
        e.raw_os_error() == Some(17)
    }
    #[cfg(not(any(unix, windows)))]
    {
        // doesn't matter, but try to detect for other platforms
        matches!(e.kind(), io::ErrorKind::CrossesDevices)
    }
}

#[cfg(test)]
mod tests {
    use testdir::testdir;

    use super::*;

    #[test]
    fn test_move_file() -> Result<()> {
        let tmp = testdir!();
        let src = tmp.join("source.txt");
        let dest = tmp.join("dest.txt");

        fs::write(&src, b"test data")?;
        move_file(&src, &dest, false)?;

        assert!(!src.exists());
        assert!(dest.exists());
        assert_eq!(fs::read(&dest)?, b"test data");
        Ok(())
    }

    #[test]
    fn test_gzip_move() -> Result<()> {
        let tmp = testdir!();
        let src = tmp.join("source.db");
        let dest = tmp.join("dest.db.gz");

        fs::write(&src, b"test database content")?;
        gzip_move(&src, &dest, false)?;

        assert!(dest.exists());
        assert!(dest.metadata()?.len() > 0);
        assert!(!src.exists());
        Ok(())
    }

    #[test]
    fn test_xz_move() -> Result<()> {
        let tmp = testdir!();
        let src = tmp.join("source.db");
        let dest = tmp.join("dest.db.xz");

        fs::write(&src, b"test database content")?;
        xz_move(&src, &dest, false)?;

        assert!(dest.exists());
        assert!(dest.metadata()?.len() > 0);
        assert!(!src.exists());
        Ok(())
    }

    #[test]
    fn test_move_processed_xz() -> Result<()> {
        let tmp = testdir!();
        let incoming = tmp.join("incoming");
        let processed = tmp.join("processed");
        let src_dir = incoming.join("1");
        fs::create_dir_all(&src_dir)?;

        let src = src_dir.join("bm-archive-123.db");
        fs::write(&src, b"test database content")?;

        let fs_client = FileSystemClientImpl::new(incoming);
        let dest = fs_client.move_processed(&src, &processed, CompressionMode::Xz, false)?;

        assert_eq!(dest, processed.join("1").join("bm-archive-123.db.xz"));
        assert!(dest.exists());
        assert!(!src.exists());
        Ok(())
    }
}
