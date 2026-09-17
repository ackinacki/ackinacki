// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
//! Integration tests for the archive processing pipeline.
//!
//! These tests use test doubles to verify the complete flow without I/O,
//! demonstrating how all infrastructure clients interact.

#[cfg(test)]
mod integration_tests {
    use std::collections::BTreeMap;
    use std::collections::HashSet;
    use std::path::PathBuf;

    use crate::cli::PostUploadAction;
    use crate::config::AppConfig;
    use crate::domain::grouping::ArchiveFile;
    use crate::domain::traits::CompressionMode;
    use crate::domain::traits::S3Client;
    use crate::infra::test_doubles::dry_run_s3_client::DryRunS3Client;
    use crate::infra::test_doubles::mock_db_client::MockDbClient;
    use crate::infra::test_doubles::mock_file_system_client::MockFileSystemClient;
    use crate::App;

    struct FailingS3Client;

    #[async_trait::async_trait]
    impl S3Client for FailingS3Client {
        async fn upload(
            &self,
            _bucket: &str,
            _key: &str,
            _file_path: &std::path::Path,
        ) -> anyhow::Result<String> {
            anyhow::bail!("simulated upload failure")
        }
    }

    /// Helper to create test filesystem with archive files from multiple servers
    fn create_test_filesystem(num_servers: usize, timestamps: Vec<i64>) -> MockFileSystemClient {
        let mut files: BTreeMap<String, Vec<ArchiveFile>> = BTreeMap::new();

        for timestamp in timestamps {
            for server_id in 0..num_servers {
                let file = ArchiveFile {
                    _bm_id: server_id.to_string(),
                    ts: timestamp,
                    path: PathBuf::from(format!("/incoming/{server_id}/archive-{timestamp}.db")),
                };
                files.entry(server_id.to_string()).or_default().push(file);
            }
        }

        MockFileSystemClient::new().with_files(files)
    }

    /// Helper to create test config
    fn create_test_config() -> AppConfig {
        AppConfig {
            incoming_dir: PathBuf::from("/incoming"),
            daily_dir: PathBuf::from("/daily"),
            processed_dir: PathBuf::from("/processed"),
            full_db: PathBuf::from("/full/blockchain.db"),
            bucket: "test-bucket".to_string(),
            require_all_servers: true,
            compression: CompressionMode::None,
            post_upload: PostUploadAction::Move,
            skip_upload: false,
            upload_later: false,
            upload_only: None,
            upload_queue_dir: PathBuf::from("/upload-queue"),
            paranoid: false,
            daily_hook: None,
            dry_run: false,
        }
    }

    #[tokio::test]
    async fn test_app_processes_single_group() {
        let config = create_test_config();
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let s3_client = DryRunS3Client;
        let fs_client = create_test_filesystem(1, vec![1000]);

        let result = app.run(db_client.clone(), Some(s3_client), fs_client.clone()).await;

        assert!(result.is_ok());

        // Verify database operations were called
        let create_calls = db_client.create_daily_calls();
        assert_eq!(create_calls.len(), 1);

        let merge_calls = db_client.merge_calls();
        assert_eq!(merge_calls.len(), 1);

        // Verify file was processed
        let move_calls = fs_client.move_processed_calls();
        assert_eq!(move_calls.len(), 2);
        assert_eq!(fs_client.remove_file_calls().len(), 0);
    }

    #[tokio::test]
    async fn test_app_processes_multiple_groups() {
        let config = create_test_config();
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let s3_client = DryRunS3Client;
        let fs_client = create_test_filesystem(1, vec![1000, 2000, 3000]);

        let result = app.run(db_client.clone(), Some(s3_client), fs_client.clone()).await;

        assert!(result.is_ok());

        // Each group should trigger create_daily_db
        let create_calls = db_client.create_daily_calls();
        assert_eq!(create_calls.len(), 3);

        // Each group should trigger merge_daily_into_full
        let merge_calls = db_client.merge_calls();
        assert_eq!(merge_calls.len(), 3);

        // Each group moves one source DB and one produced daily DB
        let move_calls = fs_client.move_processed_calls();
        assert_eq!(move_calls.len(), 6);
    }

    #[tokio::test]
    async fn test_app_without_s3_client() {
        let config = create_test_config();
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let fs_client = create_test_filesystem(1, vec![1000]);

        // Run without S3 client (None)
        let result = app.run(db_client.clone(), None::<DryRunS3Client>, fs_client.clone()).await;

        assert!(result.is_ok());

        // Database and file operations should still succeed
        let create_calls = db_client.create_daily_calls();
        assert_eq!(create_calls.len(), 1);

        let merge_calls = db_client.merge_calls();
        assert_eq!(merge_calls.len(), 1);

        let move_calls = fs_client.move_processed_calls();
        assert_eq!(move_calls.len(), 2);
    }

    #[tokio::test]
    async fn test_post_upload_delete_removes_processed_sources_after_upload() {
        let mut config = create_test_config();
        config.post_upload = PostUploadAction::Delete;
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let s3_client = DryRunS3Client;
        let fs_client = create_test_filesystem(2, vec![1000]);

        let result = app.run(db_client, Some(s3_client), fs_client.clone()).await;

        assert!(result.is_ok());

        let move_calls = fs_client.move_processed_calls();
        assert_eq!(move_calls.len(), 3);

        let removed = fs_client.remove_file_calls();
        let expected_removed: Vec<PathBuf> =
            move_calls.iter().take(2).map(|(_, dest)| dest.clone()).collect();
        assert_eq!(removed, expected_removed);
    }

    #[tokio::test]
    async fn test_post_upload_delete_keeps_processed_sources_when_upload_fails() {
        let mut config = create_test_config();
        config.post_upload = PostUploadAction::Delete;
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let fs_client = create_test_filesystem(1, vec![1000]);

        let result = app.run(db_client, Some(FailingS3Client), fs_client.clone()).await;

        // A failed group is reported through the exit status so the wrapper does
        // not record the day as applied; the sources still survive untouched.
        assert!(result.is_err());
        assert_eq!(fs_client.move_processed_calls().len(), 2);
        assert_eq!(fs_client.remove_file_calls().len(), 0);
    }

    #[tokio::test]
    async fn test_empty_filesystem_no_groups() {
        let config = create_test_config();
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let s3_client = DryRunS3Client;
        let fs_client = MockFileSystemClient::new(); // Empty filesystem

        let result = app.run(db_client.clone(), Some(s3_client), fs_client.clone()).await;

        assert!(result.is_ok());

        // No operations should be performed
        assert_eq!(db_client.create_daily_calls().len(), 0);
        assert_eq!(db_client.merge_calls().len(), 0);
        assert_eq!(fs_client.move_processed_calls().len(), 0);
    }

    #[tokio::test]
    async fn test_dry_run_s3_client_succeeds() {
        let s3_client = DryRunS3Client;

        // S3 upload should always succeed in dry-run
        let result =
            s3_client.upload("test-bucket", "daily/1000.db", &PathBuf::from("/tmp/1000.db")).await;

        assert!(result.is_ok());
        let etag = result.unwrap();
        assert_eq!(etag, "fake-etag-dry-run");
    }

    #[tokio::test]
    async fn test_processing_flow_verification() {
        let config = create_test_config();
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let s3_client = DryRunS3Client;
        let fs_client = create_test_filesystem(1, vec![5000]);

        app.run(db_client.clone(), Some(s3_client), fs_client.clone()).await.unwrap();

        // Verify the sequence of operations
        let create_calls = db_client.create_daily_calls();
        assert_eq!(create_calls.len(), 1);
        let (_src_paths, daily_db_path) = &create_calls[0];
        assert!(daily_db_path.to_string_lossy().contains("5000.db"));

        let merge_calls = db_client.merge_calls();
        assert_eq!(merge_calls.len(), 1);
        let (src_db, full_db) = &merge_calls[0];
        // src_db should be the daily database path
        assert_eq!(src_db, daily_db_path);
        // full_db should be the configured full database path
        assert!(full_db.to_string_lossy().contains("blockchain.db"));

        let move_calls = fs_client.move_processed_calls();
        assert_eq!(move_calls.len(), 2);
    }

    #[tokio::test]
    async fn test_app_multi_server_same_timestamp() {
        // Test processing with multiple servers having files at the same timestamp
        let config = create_test_config();
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let s3_client = DryRunS3Client;
        // 2 servers, both with files at timestamps 1000 and 2000
        let fs_client = create_test_filesystem(2, vec![1000, 2000]);

        let result = app.run(db_client.clone(), Some(s3_client), fs_client.clone()).await;

        assert!(result.is_ok());

        // With require_all_servers=true and 2 servers, each timestamp forms one group
        // Group 1 (ts=1000): file from server 0 + file from server 1
        // Group 2 (ts=2000): file from server 0 + file from server 1
        let create_calls = db_client.create_daily_calls();
        assert_eq!(create_calls.len(), 2);

        let merge_calls = db_client.merge_calls();
        assert_eq!(merge_calls.len(), 2);

        // 4 files moved (2 per group: one from each server)
        let move_calls = fs_client.move_processed_calls();
        assert_eq!(move_calls.len(), 6);
    }

    #[derive(Clone)]
    struct TrackingS3Client {
        uploads: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    }

    impl TrackingS3Client {
        fn new() -> Self {
            Self { uploads: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())) }
        }

        fn upload_calls(&self) -> Vec<(String, String)> {
            self.uploads.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl S3Client for TrackingS3Client {
        async fn upload(
            &self,
            bucket: &str,
            key: &str,
            _file_path: &std::path::Path,
        ) -> anyhow::Result<String> {
            self.uploads.lock().unwrap().push((bucket.to_string(), key.to_string()));
            Ok("fake-etag".to_string())
        }
    }

    #[tokio::test]
    async fn test_upload_later_enqueues_without_s3_upload() {
        let tmp = testdir::testdir!();
        let mut config = create_test_config();
        config.upload_later = true;
        config.skip_upload = true;
        config.upload_queue_dir = tmp.join("upload-queue");

        let app = App::new(config, None);
        let db_client = MockDbClient::new();
        let fs_client = create_test_filesystem(1, vec![1000]);

        let result = app.run(db_client.clone(), None::<TrackingS3Client>, fs_client.clone()).await;
        assert!(result.is_ok());

        // DB operations happened
        assert_eq!(db_client.create_daily_calls().len(), 1);
        assert_eq!(db_client.merge_calls().len(), 1);

        // Queue marker was created
        let queue_dir = tmp.join("upload-queue");
        assert!(queue_dir.is_dir());
        let markers: Vec<_> =
            std::fs::read_dir(&queue_dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(markers.len(), 1, "expected one queue marker");

        let marker_content = std::fs::read_to_string(markers[0].path()).unwrap();
        assert!(marker_content.contains("1000"), "marker should reference the daily file");
    }

    #[tokio::test]
    async fn test_upload_only_uploads_file_to_s3() {
        let tmp = testdir::testdir!();
        let file_path = tmp.join("daily").join("1000.db.xz");
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::write(&file_path, b"fake compressed data").unwrap();

        let mut config = create_test_config();
        config.upload_only = Some(file_path.clone());
        config.post_upload = PostUploadAction::Keep;

        let app = App::new(config, None);
        let s3_client = TrackingS3Client::new();

        let result = app.run_upload_only(s3_client.clone()).await;
        assert!(result.is_ok());

        let calls = s3_client.upload_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].1.contains("1000"), "S3 key should contain the timestamp");

        // File still exists (post_upload = Keep)
        assert!(file_path.exists());
    }

    #[tokio::test]
    async fn test_upload_only_deletes_after_upload() {
        let tmp = testdir::testdir!();
        let file_path = tmp.join("daily").join("2000.db.gz");
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::write(&file_path, b"fake compressed data").unwrap();

        let mut config = create_test_config();
        config.upload_only = Some(file_path.clone());
        config.post_upload = PostUploadAction::Delete;

        let app = App::new(config, None);
        let s3_client = TrackingS3Client::new();

        let result = app.run_upload_only(s3_client.clone()).await;
        assert!(result.is_ok());
        assert!(!file_path.exists(), "file should be deleted after upload");
    }

    #[tokio::test]
    async fn test_upload_only_fails_for_missing_file() {
        let mut config = create_test_config();
        config.upload_only = Some(PathBuf::from("/nonexistent/file.db.xz"));

        let app = App::new(config, None);
        let s3_client = TrackingS3Client::new();

        let result = app.run_upload_only(s3_client).await;
        assert!(result.is_err());
    }

    /// Builds a config whose only interesting field is the daily hook.
    fn config_with_hook(hook: Option<&str>) -> AppConfig {
        AppConfig { daily_hook: hook.map(str::to_string), ..create_test_config() }
    }

    #[test]
    fn daily_hook_absent_is_a_no_op() {
        let app = App::new(config_with_hook(None), None);
        assert!(app.run_daily_hook(&PathBuf::from("/daily/x.db")).is_ok());
    }

    #[test]
    fn daily_hook_success_is_propagated() {
        let app = App::new(config_with_hook(Some("true")), None);
        assert!(app.run_daily_hook(&PathBuf::from("/daily/x.db")).is_ok());
    }

    #[test]
    fn daily_hook_failure_aborts_the_group() {
        let app = App::new(config_with_hook(Some("false")), None);
        let err = app.run_daily_hook(&PathBuf::from("/daily/x.db")).unwrap_err();
        assert!(err.to_string().contains("daily hook"), "unexpected error: {err}");
    }

    #[test]
    fn daily_hook_missing_program_is_an_error() {
        let app = App::new(config_with_hook(Some("definitely-not-on-path-12345")), None);
        assert!(app.run_daily_hook(&PathBuf::from("/daily/x.db")).is_err());
    }

    #[test]
    fn daily_hook_substitutes_the_placeholder() {
        let dir = testdir::testdir!();
        let marker = dir.join("seen.txt");
        let daily = dir.join("bm-archive-1.db");
        // No shell is involved: the placeholder is replaced inside whichever
        // token carries it.
        let hook = format!("cp {{}} {}", marker.display());
        std::fs::write(&daily, b"payload").unwrap();

        let app = App::new(config_with_hook(Some(&hook)), None);
        app.run_daily_hook(&daily).unwrap();
        assert_eq!(std::fs::read(&marker).unwrap(), b"payload");
    }

    #[test]
    fn daily_hook_appends_the_path_when_no_placeholder() {
        let dir = testdir::testdir!();
        let daily = dir.join("bm-archive-2.db");
        std::fs::write(&daily, b"x").unwrap();

        // `test -f <path>` only passes if the daily path was appended.
        let app = App::new(config_with_hook(Some("test -f")), None);
        assert!(app.run_daily_hook(&daily).is_ok());

        let app = App::new(config_with_hook(Some("test -d")), None);
        assert!(app.run_daily_hook(&daily).is_err());
    }

    #[test]
    fn upload_queue_marker_is_named_after_the_timestamp_alone() {
        let dir = testdir::testdir!();
        let config = AppConfig { upload_queue_dir: dir.clone(), ..create_test_config() };
        let app = App::new(config, None);

        // Compressed dailies carry two extensions; a single file_stem() would
        // name both `.db.xz` and `.db.gz` markers `1760418000.db` and collide.
        app.enqueue_upload(&PathBuf::from("/daily/1760418000.db.xz")).unwrap();
        let marker = dir.join("1760418000");
        assert!(marker.exists(), "expected marker named after the timestamp only");
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "/daily/1760418000.db.xz");

        app.enqueue_upload(&PathBuf::from("/daily/1760504400.db")).unwrap();
        assert!(dir.join("1760504400").exists());
    }

    #[tokio::test]
    async fn dry_run_upload_only_does_not_touch_s3() {
        let dir = testdir::testdir!();
        let file = dir.join("1760418000.db.xz");
        std::fs::write(&file, b"payload").unwrap();

        let config =
            AppConfig { upload_only: Some(file.clone()), dry_run: true, ..create_test_config() };
        let app = App::new(config, None);

        // FailingS3Client errors on any upload, so reaching S3 at all shows up here.
        app.run_upload_only(FailingS3Client).await.unwrap();
        assert!(file.exists(), "dry run must not apply the post-upload action");
    }

    /// Builds a config with real dirs so the leftover scan has something to walk.
    fn config_with_dirs(root: &std::path::Path, upload_later: bool) -> AppConfig {
        let daily = root.join("daily");
        let queue = root.join("upload-queue");
        std::fs::create_dir_all(&daily).unwrap();
        std::fs::create_dir_all(&queue).unwrap();
        AppConfig {
            daily_dir: daily,
            upload_queue_dir: queue,
            upload_later,
            skip_upload: upload_later,
            ..create_test_config()
        }
    }

    #[tokio::test]
    async fn upload_later_queues_leftover_dailies_instead_of_stranding_them() {
        let dir = testdir::testdir!();
        let cfg = config_with_dirs(&dir, true);
        let (daily_dir, queue_dir) = (cfg.daily_dir.clone(), cfg.upload_queue_dir.clone());

        // A daily left behind by a crashed run: on disk, but never enqueued.
        let stray = daily_dir.join("1760418000.db");
        std::fs::write(&stray, b"db").unwrap();
        // The lock file lives here too and must not be mistaken for an archive.
        std::fs::write(daily_dir.join(".bm-archive-processor.lock"), b"").unwrap();

        let app = App::new(cfg, None);
        let fs_client = MockFileSystemClient::new();
        app.process_leftover_daily(&None::<DryRunS3Client>, &fs_client, &HashSet::new()).await;

        let marker = queue_dir.join("1760418000");
        assert!(marker.exists(), "leftover daily should have been queued");
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), stray.to_string_lossy());
        assert_eq!(std::fs::read_dir(&queue_dir).unwrap().count(), 1, "lock file must be ignored");
    }

    #[tokio::test]
    async fn leftover_scan_never_overwrites_a_marker_the_uploader_owns() {
        let dir = testdir::testdir!();
        let cfg = config_with_dirs(&dir, true);
        let (daily_dir, queue_dir) = (cfg.daily_dir.clone(), cfg.upload_queue_dir.clone());

        // Mid-flight state: the uploader compressed the daily and repointed the
        // marker at the .xz, but the .db entry was still visible when we scanned.
        // Rewriting it would send the uploader after a file xz already consumed.
        let raw = daily_dir.join("1760418000.db");
        let compressed = daily_dir.join("1760418000.db.xz");
        std::fs::write(&raw, b"db").unwrap();
        std::fs::write(&compressed, b"xz").unwrap();
        let marker = queue_dir.join("1760418000");
        std::fs::write(&marker, compressed.to_string_lossy().as_bytes()).unwrap();

        let app = App::new(cfg, None);
        let fs_client = MockFileSystemClient::new();
        app.process_leftover_daily(&None::<DryRunS3Client>, &fs_client, &HashSet::new()).await;

        assert_eq!(
            std::fs::read_to_string(&marker).unwrap(),
            compressed.to_string_lossy(),
            "the uploader's rewritten path must survive"
        );
    }

    #[tokio::test]
    async fn leftover_scan_skips_the_daily_handled_in_this_run() {
        let dir = testdir::testdir!();
        let cfg = config_with_dirs(&dir, true);
        let (daily_dir, queue_dir) = (cfg.daily_dir.clone(), cfg.upload_queue_dir.clone());

        let current = daily_dir.join("1760504400.db");
        std::fs::write(&current, b"db").unwrap();
        let mut handled = HashSet::new();
        handled.insert(current.clone());

        let app = App::new(cfg, None);
        let fs_client = MockFileSystemClient::new();
        app.process_leftover_daily(&None::<DryRunS3Client>, &fs_client, &handled).await;

        assert_eq!(std::fs::read_dir(&queue_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn leftover_scan_ignores_a_daily_left_by_a_failed_group() {
        let dir = testdir::testdir!();
        let cfg = config_with_dirs(&dir, true);
        let (daily_dir, queue_dir) = (cfg.daily_dir.clone(), cfg.upload_queue_dir.clone());

        // A group that died mid-trim leaves a half-processed daily behind. Shipping
        // that to S3 as the day's backup would be worse than shipping nothing: the
        // next run rebuilds it from the untouched incoming files.
        let partial = daily_dir.join("1760418000.db");
        std::fs::write(&partial, b"half-trimmed").unwrap();
        let attempted: HashSet<PathBuf> = [partial].into_iter().collect();

        let app = App::new(cfg, None);
        let fs_client = MockFileSystemClient::new();
        app.process_leftover_daily(&None::<DryRunS3Client>, &fs_client, &attempted).await;

        assert_eq!(std::fs::read_dir(&queue_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn leftover_compression_also_skips_a_daily_left_by_a_failed_group() {
        let dir = testdir::testdir!();
        let mut cfg = config_with_dirs(&dir, true);
        // With compression enabled, step 1 used to compress the failed group's
        // daily BEFORE consulting the handled set — renaming it to .db.xz out
        // from under the guard, so the later scans no longer recognized it and
        // shipped the partial archive anyway.
        cfg.compression = CompressionMode::Xz;
        let (daily_dir, queue_dir) = (cfg.daily_dir.clone(), cfg.upload_queue_dir.clone());

        let partial = daily_dir.join("1760418000.db");
        std::fs::write(&partial, b"half-trimmed").unwrap();
        let attempted: HashSet<PathBuf> = [partial.clone()].into_iter().collect();

        let app = App::new(cfg, None);
        let fs_client = MockFileSystemClient::new();
        app.process_leftover_daily(&None::<DryRunS3Client>, &fs_client, &attempted).await;

        assert!(
            fs_client.move_processed_calls().is_empty(),
            "an attempted daily must not be compressed"
        );
        assert_eq!(std::fs::read_dir(&queue_dir).unwrap().count(), 0);
        assert!(partial.exists(), "the partial daily must be left for the next run to rebuild");
    }

    #[tokio::test]
    async fn leftover_compression_leaves_a_queued_daily_to_the_uploader() {
        let dir = testdir::testdir!();
        let mut cfg = config_with_dirs(&dir, true);
        cfg.compression = CompressionMode::Xz;
        let (daily_dir, queue_dir) = (cfg.daily_dir.clone(), cfg.upload_queue_dir.clone());

        // Queued by an earlier run and not uploaded yet: renaming it to .db.xz
        // now would leave the marker pointing at a file that no longer exists.
        let queued = daily_dir.join("1760418000.db");
        std::fs::write(&queued, b"db").unwrap();
        std::fs::write(queue_dir.join("1760418000"), queued.to_string_lossy().as_bytes()).unwrap();

        let app = App::new(cfg, None);
        let fs_client = MockFileSystemClient::new();
        app.process_leftover_daily(&None::<DryRunS3Client>, &fs_client, &HashSet::new()).await;

        assert!(fs_client.move_processed_calls().is_empty(), "a queued daily must not be touched");
        assert_eq!(std::fs::read_dir(&queue_dir).unwrap().count(), 1, "the marker stays as it was");
    }

    #[tokio::test]
    async fn dry_run_leftover_scan_writes_no_queue_markers() {
        let dir = testdir::testdir!();
        let mut cfg = config_with_dirs(&dir, true);
        cfg.dry_run = true;
        let (daily_dir, queue_dir) = (cfg.daily_dir.clone(), cfg.upload_queue_dir.clone());
        std::fs::write(daily_dir.join("1760418000.db"), b"db").unwrap();

        let app = App::new(cfg, None);
        let fs_client = MockFileSystemClient::new();
        app.process_leftover_daily(&None::<DryRunS3Client>, &fs_client, &HashSet::new()).await;

        // A marker written here would make the real uploader ship a raw,
        // uncompressed daily the moment the dry run ends.
        assert_eq!(std::fs::read_dir(&queue_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn upload_later_with_post_upload_delete_removes_sources_once_queued() {
        let tmp = testdir::testdir!();
        let mut config = create_test_config();
        config.upload_later = true;
        config.skip_upload = true;
        config.post_upload = PostUploadAction::Delete;
        config.upload_queue_dir = tmp.join("upload-queue");
        let app = App::new(config, None);

        let fs_client = create_test_filesystem(2, vec![1000]);
        let result =
            app.run(MockDbClient::new(), None::<TrackingS3Client>, fs_client.clone()).await;
        assert!(result.is_ok());

        // Both sources are moved to processed/ and then removed; the daily
        // itself stays on disk for the deferred uploader, held by the marker.
        let move_calls = fs_client.move_processed_calls();
        assert_eq!(move_calls.len(), 3);
        let expected_removed: Vec<PathBuf> =
            move_calls.iter().take(2).map(|(_, dest)| dest.clone()).collect();
        assert_eq!(fs_client.remove_file_calls(), expected_removed);
        assert_eq!(std::fs::read_dir(tmp.join("upload-queue")).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn failing_daily_hook_keeps_everything_out_of_the_full_db() {
        let tmp = testdir::testdir!();
        let mut config = create_test_config();
        config.daily_hook = Some("false".to_string());
        config.post_upload = PostUploadAction::Delete;
        config.upload_queue_dir = tmp.join("upload-queue");
        let app = App::new(config, None);

        let db_client = MockDbClient::new();
        let s3_client = TrackingS3Client::new();
        let fs_client = create_test_filesystem(2, vec![1000]);
        let result = app.run(db_client.clone(), Some(s3_client.clone()), fs_client.clone()).await;

        assert!(result.is_err(), "a failed hook must fail the run");
        assert_eq!(db_client.create_daily_calls().len(), 1, "the daily is built before the hook");
        assert!(db_client.merge_calls().is_empty(), "nothing may reach the full DB");
        assert!(s3_client.upload_calls().is_empty(), "nothing may reach S3");
        assert!(fs_client.move_processed_calls().is_empty(), "sources stay in incoming/");
        assert!(fs_client.remove_file_calls().is_empty());
    }

    #[tokio::test]
    async fn upload_only_fails_when_the_post_upload_action_fails() {
        let tmp = testdir::testdir!();
        let daily_dir = tmp.join("daily");
        std::fs::create_dir_all(&daily_dir).unwrap();
        let file_path = daily_dir.join("3000.db.xz");
        std::fs::write(&file_path, b"archive").unwrap();
        // `move` wants ../uploaded next to the daily dir; a plain file in its
        // place makes the post-upload step fail after a successful upload.
        std::fs::write(tmp.join("uploaded"), b"not a directory").unwrap();

        let mut config = create_test_config();
        config.daily_dir = daily_dir;
        config.upload_only = Some(file_path.clone());
        config.post_upload = PostUploadAction::Move;
        let app = App::new(config, None);

        let s3_client = TrackingS3Client::new();
        let err = app.run_upload_only(s3_client.clone()).await.unwrap_err();
        assert_eq!(s3_client.upload_calls().len(), 1, "the upload itself went through");
        assert!(err.to_string().contains("uploaded dir"), "unexpected error: {err}");
        assert!(file_path.exists(), "the file stays for the uploader to retry");
    }
}
