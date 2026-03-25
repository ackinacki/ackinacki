// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use crate::cli::PostUploadAction;
use crate::config::AppConfig;
use crate::domain::config::ProcessingRules;
use crate::domain::grouping::group_by_timestamp;
use crate::domain::paths;
use crate::domain::traits::CompressionMode;
use crate::domain::traits::DbClient;
use crate::domain::traits::FileSystemClient;
use crate::domain::traits::S3Client;
use crate::Metrics;

/// Application orchestrator for the archive processing pipeline.
///
/// Coordinates high-level flow:
/// 1. Discovers archive files and groups them by timestamp
/// 2. Delegates processing to use cases (no business logic here)
/// 3. Handles metrics and logging (infrastructure concerns)
/// 4. Manages configuration transformation (CLI → domain rules)
pub struct App {
    cfg: AppConfig,
    metrics: Option<Metrics>,
    pub rules: ProcessingRules,
}

impl App {
    pub fn new(cfg: AppConfig, metrics: Option<Metrics>) -> Self {
        // Transform AppConfig into domain rules
        let rules = ProcessingRules::builder()
            .require_all_servers(cfg.require_all_servers)
            .compression(cfg.compression)
            .match_window_sec(3600)
            .build();
        App { cfg, metrics, rules }
    }

    pub async fn run(
        &self,
        db_client: impl DbClient,
        s3_client: Option<impl S3Client>,
        fs_client: impl FileSystemClient,
    ) -> anyhow::Result<()> {
        let input_files = fs_client.get_arch_files()?;
        let groups = group_by_timestamp(
            input_files,
            self.rules.match_window_sec,
            self.rules.require_all_servers,
        );
        tracing::info!("Found {} archive groups", groups.len());

        let mut uploaded_files: HashSet<PathBuf> = HashSet::new();
        let mut failure_count = 0;
        let mut max_processed_ts = 0;
        for group in groups.iter() {
            tracing::info!(
                anchor_ts = group.anchor_timestamp,
                files_count = group.file_count(),
                "Processing archive group"
            );

            match self.process_archive_group(group, &db_client, &s3_client, &fs_client).await {
                Ok(processed_path) => {
                    if let Some(path) = processed_path {
                        uploaded_files.insert(path);
                    }
                    if let Some(metrics) = &self.metrics {
                        metrics.incoming_success.add(1, &[]);
                        if group.anchor_timestamp > max_processed_ts {
                            max_processed_ts = group.anchor_timestamp;
                            metrics.anchor_timestamp.record(max_processed_ts as u64, &[]);
                        }
                    }
                }
                Err(err) => {
                    failure_count += 1;
                    tracing::error!(
                        anchor_ts = group.anchor_timestamp,
                        error = %err,
                        "Failed to process group"
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.incoming_fail.add(1, &[]);
                    }
                }
            }
        }

        if !groups.is_empty() {
            if failure_count > 0 {
                tracing::warn!(
                    "Finished processing {} groups with {failure_count} failures",
                    groups.len(),
                );
            } else {
                tracing::info!("Finished processing {} groups", groups.len());
            }
        }

        // Process leftover daily files from previous runs
        self.process_leftover_daily(&s3_client, &fs_client, &uploaded_files).await;

        // Log block gaps remaining in the full database
        match db_client.query_block_gaps(&self.cfg.full_db) {
            Ok(gaps) if gaps.is_empty() => {
                tracing::info!("No block gaps found");
            }
            Ok(gaps) => {
                tracing::warn!("Found {} block gap(s):", gaps.len());
                for gap in &gaps {
                    tracing::warn!(
                        gap_start = gap.gap_start,
                        gap_end = gap.gap_end,
                        gap_start_ts = gap.gap_start_ts,
                        gap_end_ts = gap.gap_end_ts,
                        missing_count = gap.missing_count,
                        "block gap"
                    );
                }
            }
            Err(err) => {
                tracing::error!(error = %err, "Failed to query block gaps");
            }
        }

        Ok(())
    }

    /// Delegates processing of an archive group to the use case.
    /// Returns the path to the compressed daily DB if processing succeeded.
    async fn process_archive_group(
        &self,
        group: &crate::domain::models::ArchiveGroup,
        db_client: &impl DbClient,
        s3_client: &Option<impl S3Client>,
        fs_client: &impl FileSystemClient,
    ) -> anyhow::Result<Option<PathBuf>> {
        if self.cfg.dry_run {
            tracing::info!("DRY RUN: Would process group at {}", group.anchor_timestamp);
            return Ok(None);
        }

        let source_paths = group.paths();
        let anchor_ts = group.anchor_timestamp;

        // Step 1: Create daily database
        let daily_db_path = paths::daily_db_path(&self.cfg.daily_dir, anchor_ts);
        db_client.create_daily_db(&source_paths, &daily_db_path)?;

        // Step 2: Merge into full database
        db_client.merge_daily_into_full(&daily_db_path, &self.cfg.full_db)?;

        tracing::debug!("app cfg: {}", self.cfg);
        // Step 3: Move processed files
        for src_path in &source_paths {
            let processed = fs_client.move_processed(
                src_path,
                self.cfg.processed_dir.clone(),
                CompressionMode::None,
                false,
            )?;
            tracing::info!("File processed: {}", processed.display());
        }

        // Step 4: Move processed daily DB
        let processed = fs_client.move_processed(
            daily_db_path,
            self.cfg.daily_dir.parent().unwrap_or(std::path::Path::new("./")),
            self.rules.compression,
            false,
        )?;
        tracing::info!("File processed: {}", processed.display());

        // Step 5: Upload to S3 (if required)
        if let Some(uploader) = s3_client {
            let s3_key = paths::s3_key_from_path(&processed);
            let etag = uploader.upload(&self.cfg.bucket, &s3_key, &processed).await?;
            tracing::info!(etag = %etag, key=%s3_key, "Upload completed");
            self.apply_post_upload(&processed);
        }

        Ok(Some(processed))
    }

    /// Compress leftover uncompressed daily DBs and upload leftover archives to S3.
    async fn process_leftover_daily(
        &self,
        s3_client: &Option<impl S3Client>,
        fs_client: &impl FileSystemClient,
        already_processed: &HashSet<PathBuf>,
    ) {
        let daily_dir = &self.cfg.daily_dir;
        if !daily_dir.is_dir() {
            return;
        }

        // Step 1: Compress leftover uncompressed .db files
        if self.rules.compression != CompressionMode::None {
            let leftover_dbs: Vec<PathBuf> = match std::fs::read_dir(daily_dir) {
                Ok(entries) => entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| p.is_file() && p.extension().is_some_and(|ext| ext == "db"))
                    .collect(),
                Err(err) => {
                    tracing::error!(error = %err, "Failed to scan daily dir for leftover DBs");
                    return;
                }
            };

            for db_path in &leftover_dbs {
                tracing::info!(path = %db_path.display(), "Compressing leftover daily DB");
                let daily_parent = daily_dir.parent().unwrap_or(std::path::Path::new("./"));
                match fs_client.move_processed(
                    db_path,
                    daily_parent,
                    self.rules.compression,
                    self.cfg.dry_run,
                ) {
                    Ok(compressed) => {
                        tracing::info!(path = %compressed.display(), "Leftover daily DB compressed");
                    }
                    Err(err) => {
                        tracing::error!(
                            path = %db_path.display(),
                            error = %err,
                            "Failed to compress leftover daily DB"
                        );
                    }
                }
            }
        }

        // Step 2: Upload leftover compressed files to S3
        let Some(uploader) = s3_client else { return };

        let compressed_files: Vec<PathBuf> = match std::fs::read_dir(daily_dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.is_file()
                        && !already_processed.contains(p)
                        && is_compressed_db(p)
                        && !p.to_string_lossy().ends_with(".part")
                })
                .collect(),
            Err(err) => {
                tracing::error!(error = %err, "Failed to scan daily dir for leftover archives");
                return;
            }
        };

        for file_path in &compressed_files {
            let s3_key = paths::s3_key_from_path(file_path);
            tracing::info!(key = %s3_key, "Uploading leftover archive");
            match uploader.upload(&self.cfg.bucket, &s3_key, file_path).await {
                Ok(etag) => {
                    tracing::info!(etag = %etag, key = %s3_key, "Leftover upload completed");
                    self.apply_post_upload(file_path);
                }
                Err(err) => {
                    tracing::error!(
                        key = %s3_key,
                        error = %err,
                        "Failed to upload leftover archive"
                    );
                }
            }
        }
    }

    /// Delete or move a file after successful S3 upload, based on `--post-upload` flag.
    fn apply_post_upload(&self, path: &Path) {
        match self.cfg.post_upload {
            PostUploadAction::Keep => {}
            PostUploadAction::Delete => {
                if let Err(err) = std::fs::remove_file(path) {
                    tracing::error!(
                        path = %path.display(),
                        error = %err,
                        "Failed to delete uploaded file"
                    );
                } else {
                    tracing::info!(path = %path.display(), "Deleted after upload");
                }
            }
            PostUploadAction::Move => {
                let uploaded_dir = self.cfg.daily_dir.with_file_name("uploaded");
                if let Err(err) = std::fs::create_dir_all(&uploaded_dir) {
                    tracing::error!(
                        path = %uploaded_dir.display(),
                        error = %err,
                        "Failed to create uploaded dir"
                    );
                    return;
                }
                if let Some(file_name) = path.file_name() {
                    let dest = uploaded_dir.join(file_name);
                    if let Err(err) = std::fs::rename(path, &dest) {
                        tracing::error!(
                            src = %path.display(),
                            dest = %dest.display(),
                            error = %err,
                            "Failed to move uploaded file"
                        );
                    } else {
                        tracing::info!(dest = %dest.display(), "Moved after upload");
                    }
                }
            }
        }
    }
}

fn is_compressed_db(path: &Path) -> bool {
    let name = path.to_string_lossy();
    name.ends_with(".db.xz") || name.ends_with(".db.gz")
}
