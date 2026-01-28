// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

use async_trait::async_trait;

use crate::domain::traits::S3Client;

/// Dry-run S3 client that simulates uploads without calling AWS.
///
/// Use this in tests or when `--dry-run` is specified instead of
/// conditional logic in business code.
#[allow(unused)]
#[derive(Clone)]
pub struct DryRunS3Client;

#[async_trait]
impl S3Client for DryRunS3Client {
    async fn upload(&self, bucket: &str, key: &str, _file_path: &Path) -> anyhow::Result<String> {
        tracing::info!("DRY RUN: Would upload {key} to S3 bucket {bucket}");
        Ok("fake-etag-dry-run".to_string())
    }
}
