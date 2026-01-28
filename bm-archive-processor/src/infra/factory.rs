// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

/// Factory functions for creating infrastructure clients.
///
/// Centralizes dependency construction, making it easier to vary behavior
/// based on configuration without modifying business logic.
use crate::domain::traits::DbClient;
/// Factory functions for creating infrastructure clients.
///
/// Centralizes dependency construction, making it easier to vary behavior
/// based on configuration without modifying business logic.
use crate::domain::traits::FileSystemClient;
/// Factory functions for creating infrastructure clients.
///
/// Centralizes dependency construction, making it easier to vary behavior
/// based on configuration without modifying business logic.
use crate::domain::traits::S3Client;
use crate::infra::file_storage::FileSystemClientImpl;
use crate::infra::s3_storage::S3ClientImpl;
use crate::infra::s3_storage::S3Config;
use crate::infra::sqlite_db::SqliteClient;

/// Creates a database client, optionally with metrics.
pub fn create_db_client(
    tables: &'static [&'static str],
    metrics: Option<crate::app::metrics::Metrics>,
) -> impl DbClient {
    SqliteClient::new(tables, metrics)
}

/// Creates a filesystem client.
pub fn create_fs_client(incoming_dir: PathBuf) -> impl FileSystemClient {
    FileSystemClientImpl::new(incoming_dir)
}

/// Creates an S3 client based on configuration and flags.
///
/// Returns None if uploading is disabled, otherwise returns S3ClientImpl.
pub async fn create_s3_client(skip_upload: bool) -> anyhow::Result<Option<impl S3Client>> {
    if skip_upload {
        Ok(None)
    } else {
        let config = S3Config::from_env()?;
        let client = S3ClientImpl::new(
            config,
            Some(aws_sdk_s3::types::StorageClass::DeepArchive),
            Some(aws_sdk_s3::types::ServerSideEncryption::Aes256),
        )
        .await?;
        Ok(Some(client))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TABLES;

    #[test]
    fn test_create_db_client_without_metrics() {
        let _client = create_db_client(TABLES, None);
        // Verify it can be created
    }

    #[test]
    fn test_create_fs_client() {
        let dir = PathBuf::from("/tmp");
        let _client = create_fs_client(dir);
        // Verify it can be created
    }
}
