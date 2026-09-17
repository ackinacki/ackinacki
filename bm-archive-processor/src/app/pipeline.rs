// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;

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

    pub async fn run_upload_only(&self, s3_client: impl S3Client) -> anyhow::Result<()> {
        let file_path = self.cfg.upload_only.as_ref().expect("upload_only path must be set");
        anyhow::ensure!(file_path.exists(), "file not found: {}", file_path.display());

        let s3_key = paths::s3_key_from_path(file_path);
        if self.cfg.dry_run {
            tracing::info!(
                key = %s3_key,
                file = %file_path.display(),
                "Dry run: skipping upload and post-upload action"
            );
            return Ok(());
        }
        tracing::info!(key = %s3_key, file = %file_path.display(), "Uploading file to S3");
        let etag = s3_client.upload(&self.cfg.bucket, &s3_key, file_path).await?;
        tracing::info!(etag = %etag, key = %s3_key, "Upload completed");
        // Propagated: exiting 0 with the file still on disk makes the uploader
        // drop the queue marker, the next scan re-queues the file, and every
        // cycle re-uploads it to Deep Archive (180-day minimum billing each).
        self.apply_post_upload(file_path)?;
        Ok(())
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
        // Dailies this run built. A group that fails part-way leaves one behind in
        // a half-finished state (a trim hook aborting mid-prune, say); the leftover
        // scan must not ship that to S3 as the backup for the day. The next run
        // rebuilds it from scratch, since create_daily_db recreates the file.
        let mut attempted_dailies: HashSet<PathBuf> = HashSet::new();
        let mut failure_count = 0;
        let mut max_processed_ts = 0;
        for group in groups.iter() {
            tracing::info!(
                anchor_ts = group.anchor_timestamp,
                files_count = group.file_count(),
                "Processing archive group"
            );

            attempted_dailies
                .insert(paths::daily_db_path(&self.cfg.daily_dir, group.anchor_timestamp));

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
        let handled: HashSet<PathBuf> = uploaded_files.union(&attempted_dailies).cloned().collect();
        self.process_leftover_daily(&s3_client, &fs_client, &handled).await;

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

        // Surface group failures in the exit status. The wrapper decides whether
        // a day was applied by looking at it, so exiting 0 after a failed group
        // would silently mark that day as done and lose it.
        //
        // A group that keeps failing therefore keeps the exit status non-zero
        // on every run until someone looks at it. That is the intent, not a
        // gap: a day that cannot be applied is lost data, and a quarantine that
        // let the wrapper carry on would hide exactly the condition an operator
        // has to act on.
        if failure_count > 0 {
            anyhow::bail!("{failure_count} of {} group(s) failed to process", groups.len());
        }

        Ok(())
    }

    /// Runs the configured `--daily-hook` against a freshly built daily DB.
    ///
    /// The command is split on whitespace and the first token is executed
    /// directly, so a bare name is resolved through PATH. `{}` is substituted
    /// with the daily path; without it the path is appended as the last
    /// argument. Any non-zero exit fails the whole group rather than letting an
    /// unverified daily reach the full DB.
    pub(crate) fn run_daily_hook(&self, daily_db_path: &Path) -> anyhow::Result<()> {
        let Some(hook) = self.cfg.daily_hook.as_deref() else { return Ok(()) };

        let daily = daily_db_path.to_string_lossy().into_owned();
        // No shell: the command is split on whitespace, quoting is not
        // interpreted, and `{}` is substituted in whichever token carries it —
        // the program token included. Without a placeholder the path is
        // appended as the last argument.
        let mut tokens = hook.split_whitespace().map(|t| t.replace("{}", &daily));
        let program = tokens.next().ok_or_else(|| anyhow::anyhow!("--daily-hook is empty"))?;

        let mut args: Vec<String> = tokens.collect();
        if !hook.contains("{}") {
            args.push(daily.clone());
        }

        tracing::info!(program = %program, ?args, "running daily hook");
        let started_at = std::time::Instant::now();
        let status = std::process::Command::new(&program)
            .args(&args)
            .status()
            .with_context(|| format!("failed to spawn daily hook `{program}`"))?;

        if !status.success() {
            anyhow::bail!("daily hook `{hook}` failed for {daily}: {status}");
        }
        tracing::info!(elapsed_ms = started_at.elapsed().as_millis(), "daily hook completed");
        Ok(())
    }

    /// Delegates processing of an archive group to the use case.
    ///
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

        // Step 1.5: Hand the freshly built daily to the trim hook, if configured.
        // Nothing holds the daily open at this point — `create_daily_db` closed
        // its connection on return and `merge_daily_into_full` opens its own —
        // so the hook is free to rewrite or replace the file.
        self.run_daily_hook(&daily_db_path)?;

        // Step 2: Merge into full database
        db_client.merge_daily_into_full(&daily_db_path, &self.cfg.full_db)?;

        tracing::debug!("app cfg: {}", self.cfg);
        // Step 3: Move processed files
        let mut processed_source_paths = Vec::with_capacity(source_paths.len());
        for src_path in &source_paths {
            let processed = fs_client.move_processed(
                src_path,
                self.cfg.processed_dir.clone(),
                CompressionMode::None,
                false,
            )?;
            tracing::info!("File processed: {}", processed.display());
            processed_source_paths.push(processed);
        }

        // Step 4: Move processed daily DB
        let processed = fs_client.move_processed(
            daily_db_path,
            self.cfg.daily_dir.parent().unwrap_or(std::path::Path::new("./")),
            self.rules.compression,
            false,
        )?;
        tracing::info!("File processed: {}", processed.display());

        // Step 5: Upload to S3 or enqueue for later
        if self.cfg.upload_later {
            self.enqueue_upload(&processed)?;
            // Deliberate: with --upload-later the sources go as soon as the
            // daily is queued, not once it is in S3. The day's rows are in the
            // full DB by now and the BMs keep their archives for weeks, so the
            // queue marker plus the daily on disk is the retention relied on;
            // holding terabytes of sources until the deferred upload completes
            // is what this mode exists to avoid.
            if self.cfg.post_upload == PostUploadAction::Delete {
                self.remove_processed_sources(&processed_source_paths, fs_client);
            }
        } else if let Some(uploader) = s3_client {
            let s3_key = paths::s3_key_from_path(&processed);
            let etag = uploader.upload(&self.cfg.bucket, &s3_key, &processed).await?;
            tracing::info!(etag = %etag, key=%s3_key, "Upload completed");
            if self.cfg.post_upload == PostUploadAction::Delete {
                self.remove_processed_sources(&processed_source_paths, fs_client);
            }
            // Cleanup only — the day is merged and uploaded; failing the group
            // here would make the wrapper re-pull it. Known and accepted: with a
            // compressed daily the next run's leftover scan uploads the file
            // again before it retries the cleanup, so a delete that keeps
            // failing (permissions, say) costs one extra Deep Archive upload
            // per run until fixed. `--upload-only`, the path the production
            // uploader takes, propagates the error instead.
            if let Err(err) = self.apply_post_upload(&processed) {
                tracing::error!(error = %err, "Post-upload action failed");
            }
        }

        Ok(Some(processed))
    }

    /// Compress leftover uncompressed daily DBs and upload leftover archives to S3.
    ///
    /// Best-effort by design, hence no `Result`: everything here concerns
    /// dailies whose rows are already in the full DB, and each step logs and
    /// moves on so that one stuck file cannot block the rest. A failure is
    /// retried by the next run's scan; surfacing it in the exit status instead
    /// would make the wrapper treat a successfully applied day as lost and
    /// pull, trim and merge it all over again.
    pub(crate) async fn process_leftover_daily(
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
                    .filter(|p| {
                        p.is_file()
                            && p.extension().is_some_and(|ext| ext == "db")
                            // Consulted BEFORE compressing: move_processed renames
                            // the file to .db.xz/.gz, and the later scans compare
                            // against the raw path in this set — a failed group's
                            // daily would be renamed out from under its own guard
                            // and shipped anyway.
                            && !already_processed.contains(p)
                            // A queued daily belongs to the uploader: renaming it
                            // here would leave its marker pointing at nothing.
                            && !self.upload_marker_path(p).exists()
                    })
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

        // Step 2a: with --upload-later there is no S3 client in this process, so
        // the upload step below would bail out and leave stray dailies behind
        // forever. Hand them to the deferred uploader instead.
        if self.cfg.upload_later {
            self.enqueue_leftover_dailies(already_processed);
            return;
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
                    if let Err(err) = self.apply_post_upload(file_path) {
                        tracing::error!(error = %err, "Post-upload action failed");
                    }
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

    /// Queues dailies left in the daily dir by an earlier run for deferred upload.
    fn enqueue_leftover_dailies(&self, already_processed: &HashSet<PathBuf>) {
        let daily_dir = &self.cfg.daily_dir;
        let leftovers: Vec<PathBuf> = match std::fs::read_dir(daily_dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.is_file()
                        && !already_processed.contains(p)
                        && (is_compressed_db(p) || p.extension().is_some_and(|ext| ext == "db"))
                        && !p.to_string_lossy().ends_with(".part")
                })
                .collect(),
            Err(err) => {
                tracing::error!(error = %err, "Failed to scan daily dir for leftover dailies");
                return;
            }
        };

        for path in &leftovers {
            // Never overwrite an existing marker: anything already queued is
            // the uploader's business, not ours. The uploader only ever acts on
            // files that have a marker, and drops a marker whose path no longer
            // exists (stale after a rename to .db.xz), after which the file
            // shows up here as a leftover without one and is re-queued under
            // its new name. One marker per timestamp is what keeps that cycle
            // from queueing a file twice.
            let marker = self.upload_marker_path(path);
            if marker.exists() {
                continue;
            }
            if self.cfg.dry_run {
                tracing::info!(
                    path = %path.display(),
                    "DRY RUN: Would queue leftover daily archive for deferred upload"
                );
                continue;
            }
            tracing::info!(
                path = %path.display(),
                "Queueing leftover daily archive for deferred upload"
            );
            if let Err(err) = self.enqueue_upload(path) {
                tracing::error!(
                    path = %path.display(),
                    error = %err,
                    "Failed to queue leftover daily archive"
                );
            }
        }
    }

    /// Path of the upload-queue marker that stands for `path`.
    fn upload_marker_path(&self, path: &Path) -> PathBuf {
        // file_stem() peels one extension at a time, so `1760418000.db.xz` would
        // leave `1760418000.db` — and the `.db.gz` marker for the same day would
        // land on the very same name. Take everything before the first dot.
        let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("unknown");
        let marker_name = match file_name.split('.').next() {
            Some(stem) if !stem.is_empty() => stem,
            _ => file_name,
        };
        self.cfg.upload_queue_dir.join(marker_name)
    }

    pub(crate) fn enqueue_upload(&self, path: &Path) -> anyhow::Result<()> {
        let queue_dir = &self.cfg.upload_queue_dir;
        fs::create_dir_all(queue_dir).with_context(|| {
            format!("failed to create upload queue dir: {}", queue_dir.display())
        })?;

        let marker_path = self.upload_marker_path(path);
        let target = path.to_string_lossy();
        // Written to a dot-prefixed temp first: the uploader globs
        // upload-queue/[0-9]*, so a half-written file must never match, and a
        // crash or full disk mid-write must not leave a truncated marker. The
        // rename publishes it atomically.
        let marker_name = marker_path.file_name().unwrap_or_default().to_string_lossy();
        let tmp_path = marker_path.with_file_name(format!(".{marker_name}.tmp"));
        {
            use std::io::Write as _;
            let mut tmp = fs::File::create(&tmp_path).with_context(|| {
                format!("failed to create marker temp file: {}", tmp_path.display())
            })?;
            tmp.write_all(target.as_bytes())?;
            tmp.sync_all()?;
        }
        fs::rename(&tmp_path, &marker_path).with_context(|| {
            format!("failed to publish upload queue marker: {}", marker_path.display())
        })?;

        tracing::info!(
            marker = %marker_path.display(),
            file = %path.display(),
            "Enqueued for later upload"
        );
        Ok(())
    }

    /// Delete or move a file after successful S3 upload, based on `--post-upload` flag.
    fn apply_post_upload(&self, path: &Path) -> anyhow::Result<()> {
        match self.cfg.post_upload {
            PostUploadAction::Keep => {}
            PostUploadAction::Delete => {
                std::fs::remove_file(path).with_context(|| {
                    format!("failed to delete uploaded file {}", path.display())
                })?;
                tracing::info!(path = %path.display(), "Deleted after upload");
            }
            PostUploadAction::Move => {
                let uploaded_dir = self.cfg.daily_dir.with_file_name("uploaded");
                std::fs::create_dir_all(&uploaded_dir).with_context(|| {
                    format!("failed to create uploaded dir {}", uploaded_dir.display())
                })?;
                let file_name = path
                    .file_name()
                    .ok_or_else(|| anyhow::anyhow!("no file name in {}", path.display()))?;
                let dest = uploaded_dir.join(file_name);
                std::fs::rename(path, &dest).with_context(|| {
                    format!("failed to move {} -> {}", path.display(), dest.display())
                })?;
                tracing::info!(dest = %dest.display(), "Moved after upload");
            }
        }
        Ok(())
    }

    fn remove_processed_sources(
        &self,
        processed_source_paths: &[PathBuf],
        fs_client: &impl FileSystemClient,
    ) {
        tracing::info!(
            files_count = processed_source_paths.len(),
            "Deleting processed source files after successful daily archive upload"
        );

        let mut deleted_count = 0;
        let mut failed_count = 0;
        for path in processed_source_paths {
            if let Err(err) = fs_client.remove_file(path) {
                failed_count += 1;
                tracing::error!(
                    path = %path.display(),
                    error = %err,
                    "Failed to delete processed source file"
                );
            } else {
                deleted_count += 1;
                tracing::info!(path = %path.display(), "Deleted processed source file");
            }
        }

        tracing::info!(deleted_count, failed_count, "Processed source file cleanup completed");
    }
}

fn is_compressed_db(path: &Path) -> bool {
    let name = path.to_string_lossy();
    name.ends_with(".db.xz") || name.ends_with(".db.gz")
}
