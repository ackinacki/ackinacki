// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
//! Integration tests for the archive processing pipeline.
//!
//! These tests use test doubles to verify the complete flow without I/O,
//! demonstrating how all infrastructure clients interact.

#[cfg(test)]
mod integration_tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::config::AppConfig;
    use crate::domain::grouping::ArchiveFile;
    use crate::domain::traits::S3Client;
    use crate::infra::test_doubles::dry_run_s3_client::DryRunS3Client;
    use crate::infra::test_doubles::mock_db_client::MockDbClient;
    use crate::infra::test_doubles::mock_file_system_client::MockFileSystemClient;
    use crate::App;

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
            compress: false,
            skip_upload: false,
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
        assert_eq!(move_calls.len(), 1);
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

        // Each group's file should be moved
        let move_calls = fs_client.move_processed_calls();
        assert_eq!(move_calls.len(), 3);
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
        assert_eq!(move_calls.len(), 1);
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
        assert_eq!(move_calls.len(), 1);
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
        assert_eq!(move_calls.len(), 4);
    }
}
