use crate::config::AppConfig;
use crate::domain::config::ProcessingRules;
use crate::domain::grouping::group_by_timestamp;
use crate::domain::paths;
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
            .compress(cfg.compress)
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
        if groups.is_empty() {
            return Ok(());
        }

        let mut failure_count = 0;
        let mut max_processed_ts = 0;
        for group in groups.iter() {
            tracing::info!(
                anchor_ts = group.anchor_timestamp,
                files_count = group.file_count(),
                "Processing archive group"
            );

            if let Err(err) =
                self.process_archive_group(group, &db_client, &s3_client, &fs_client).await
            {
                failure_count += 1;
                tracing::error!(
                    anchor_ts = group.anchor_timestamp,
                    error = %err,
                    "Failed to process group"
                );
                if let Some(metrics) = &self.metrics {
                    metrics.incoming_fail.add(1, &[]);
                }
            } else if let Some(metrics) = &self.metrics {
                metrics.incoming_success.add(1, &[]);
                if group.anchor_timestamp > max_processed_ts {
                    max_processed_ts = group.anchor_timestamp;
                    metrics.anchor_timestamp.record(max_processed_ts as u64, &[]);
                }
            }
        }

        if failure_count > 0 {
            tracing::warn!(
                "Finished processing {} groups with {failure_count} failures",
                groups.len(),
            );
        } else {
            tracing::info!("Finished processing {} groups", groups.len());
        }
        Ok(())
    }

    /// Delegates processing of an archive group to the use case.
    /// In dry-run mode, skips execution (would use test double in production).
    async fn process_archive_group(
        &self,
        group: &crate::domain::models::ArchiveGroup,
        db_client: &impl DbClient,
        s3_client: &Option<impl S3Client>,
        fs_client: &impl FileSystemClient,
    ) -> anyhow::Result<()> {
        if self.cfg.dry_run {
            tracing::info!("DRY RUN: Would process group at {}", group.anchor_timestamp);
            return Ok(());
        }

        let source_paths = group.paths();
        let anchor_ts = group.anchor_timestamp;

        // Step 1: Create daily database
        let daily_db_path = paths::daily_db_path(&self.cfg.daily_dir, anchor_ts);
        db_client.create_daily_db(&source_paths, &daily_db_path)?;

        // Step 2: Merge into full database
        db_client.merge_daily_into_full(&daily_db_path, &self.cfg.full_db)?;

        // Step 3: Upload to S3 (if required)
        if let Some(uploader) = s3_client {
            let s3_key = paths::s3_key_from_path(&daily_db_path);
            let etag = uploader.upload(&self.cfg.bucket, &s3_key, &daily_db_path).await?;
            tracing::info!(etag = %etag, key=%s3_key, "Upload completed");
        }

        // Step 4: Move processed files
        for src_path in &source_paths {
            let processed = fs_client.move_processed(
                src_path,
                self.cfg.daily_dir.parent().unwrap_or(&self.cfg.daily_dir),
                self.rules.compress,
                false,
            )?;
            tracing::info!("File processed: {}", processed.display())
        }

        Ok(())
    }
}
