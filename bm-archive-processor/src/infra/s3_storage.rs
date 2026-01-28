// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use aws_sdk_s3::types::StorageClass;
use aws_sdk_s3::Client;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::domain::traits::S3Client;

const PART_SIZE: usize = 16 * 1024 * 1024;
const MAX_CONCURRENCY: usize = 8;

pub struct S3ClientImpl {
    #[cfg(test)]
    pub client: Client,
    #[cfg(not(test))]
    client: Client,
    storage_class: StorageClass,
    server_side_encryption: ServerSideEncryption,
}

impl S3ClientImpl {
    pub async fn new(
        config: S3Config,
        storage_class: Option<StorageClass>,
        server_side_encryption: Option<ServerSideEncryption>,
    ) -> anyhow::Result<Self> {
        let credentials =
            Credentials::new(&config.access_key, &config.secret_key, None, None, "env-credentials");

        let aws_config = aws_config::from_env()
            .region(Region::new(config.region.clone()))
            .credentials_provider(credentials)
            .load()
            .await;

        let client = Client::new(&aws_config);

        Ok(Self {
            client,
            storage_class: storage_class.unwrap_or(StorageClass::DeepArchive),
            server_side_encryption: server_side_encryption.unwrap_or(ServerSideEncryption::Aes256),
        })
    }
}

#[async_trait]
impl S3Client for S3ClientImpl {
    async fn upload(&self, bucket: &str, key: &str, path: &Path) -> anyhow::Result<String> {
        let started_at = Instant::now();
        tracing::debug!(bucket = %bucket, key = %key, local_file = %path.display(), "upload_to_s3");

        let metadata =
            tokio::fs::metadata(path).await.context(format!("Failed to read file {path:?}"))?;

        let file_size = metadata.len() as usize;

        if file_size == 0 {
            anyhow::bail!("File {path:?} is empty");
        }

        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .storage_class(self.storage_class.clone())
            .server_side_encryption(self.server_side_encryption.clone())
            .send()
            .await
            .context(format!("Failed to create multipart upload {path:?}"))?
            .upload_id()
            .context(format!("Missing upload_idl {path:?}"))?
            .to_string();

        tracing::info!("Upload ID: {upload_id} (file size: {file_size} bytes)");

        match upload_parts(&self.client, bucket, key, &upload_id, path, file_size).await {
            Ok(completed_parts) => {
                let upload_output =
                    complete_upload(&self.client, bucket, key, &upload_id, completed_parts)
                        .await
                        .context(format!("Failed to complete multipart upload {path:?}"))?;
                let elapsed = started_at.elapsed();
                tracing::info!(
                    bucket = %bucket,
                    key = %key,
                    elapsed_sec = elapsed.as_secs(),
                    "upload completed"
                );

                // TODO: here we can verify upload output
                Ok(upload_output.e_tag.unwrap_or_default())
            }
            Err(err) => {
                tracing::error!("Upload failed, aborting: {err}");
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                let elapsed = started_at.elapsed();
                tracing::info!(
                    bucket = %bucket,
                    key = %key,
                    elapsed_sec = elapsed.as_secs(),
                    "upload failed"
                );
                Err(err)
            }
        }
    }
}

async fn upload_parts(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    path: &Path,
    file_size: usize,
) -> anyhow::Result<Vec<aws_sdk_s3::types::CompletedPart>> {
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENCY));
    let mut join_set = JoinSet::new();

    let total_parts = file_size.div_ceil(PART_SIZE);

    tracing::debug!("uploading {total_parts} parts by {PART_SIZE} bytes");
    for part_idx in 0..total_parts {
        let part_number = (part_idx + 1) as i32;
        let start = part_idx * PART_SIZE;
        let end = (start + PART_SIZE).min(file_size);
        let chunk_size = end - start;

        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let path = path.to_path_buf();

        let permit = semaphore.clone().acquire_owned().await?;

        join_set.spawn(async move {
            let _permit = permit;

            let chunk = read_file_chunk(&path, start, chunk_size)
                .await
                .context(format!("Failed to read file chunk: {path:?}"))?;

            let resp = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(chunk))
                .send()
                .await
                .with_context(|| format!("Failed to upload part {part_number}"))?;
            let etag = resp.e_tag().context("Missing ETag in response")?.to_string();
            Ok::<_, anyhow::Error>((part_number, etag))
        });
    }

    let mut completed_parts = Vec::with_capacity(total_parts);

    while let Some(res) = join_set.join_next().await {
        let (part_number, etag) = res??;
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number)
                .e_tag(etag)
                .build(),
        );
    }

    completed_parts.sort_unstable_by_key(|p| p.part_number);
    Ok(completed_parts)
}

async fn complete_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<aws_sdk_s3::types::CompletedPart>,
) -> anyhow::Result<CompleteMultipartUploadOutput> {
    let output = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder().set_parts(Some(parts)).build(),
        )
        .send()
        .await?;
    Ok(output)
}

async fn read_file_chunk(path: &Path, offset: usize, size: usize) -> anyhow::Result<Bytes> {
    use tokio::io::AsyncSeekExt;
    let mut file = File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
    let mut buffer = vec![0u8; size];
    file.read_exact(&mut buffer).await?;
    Ok(Bytes::from(buffer))
}

#[derive(Debug, Clone)]
pub struct S3Config {
    access_key: String,
    secret_key: String,
    region: String,
    #[allow(unused)] // TODO: why we need this?
    bucket: String,
}

impl S3Config {
    /// Loads S3 client conf from env
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            access_key: std::env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID not set")?,
            secret_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                .context("AWS_SECRET_ACCESS_KEY does not set")?,
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "eu-west-2".into()),
            bucket: std::env::var("S3_BUCKET").context("S3_BUCKET not set")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use aws_sdk_s3::types::GlacierJobParameters;
    use aws_sdk_s3::types::RestoreRequest;
    use aws_sdk_s3::types::Tier;
    use sha2::Digest;
    use sha2::Sha256;
    use tempfile::NamedTempFile;
    use tokio::time::sleep;

    use super::*;
    use crate::infra::logging::init_tracing;

    const S3_BUCKET: &str = "ackinacki-bm-archive";

    fn calculate_file_hash(path: &Path) -> anyhow::Result<String> {
        let data = std::fs::read(path)
            .with_context(|| format!("Failed to read file for hashing: {}", path.display()))?;
        let hash = Sha256::digest(&data);
        Ok(format!("{hash:x}"))
    }

    fn calculate_data_hash(data: &[u8]) -> String {
        let hash = Sha256::digest(data);
        format!("{hash:x}")
    }

    fn generate_random_file(path: &Path, size_bytes: u64) -> anyhow::Result<()> {
        const CHUNK_SIZE: usize = 8 * 1024 * 1024;

        let file = std::fs::File::create(path)
            .with_context(|| format!("Failed to create file: {}", path.display()))?;
        let mut writer = std::io::BufWriter::with_capacity(CHUNK_SIZE, file);

        let mut seed =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        if seed == 0 {
            seed = 0x9E3779B97F4A7C15;
        }

        let mut remaining = size_bytes;
        let mut buffer = vec![0u8; CHUNK_SIZE];

        while remaining > 0 {
            let write_size = remaining.min(CHUNK_SIZE as u64) as usize;
            let slice = &mut buffer[..write_size];

            let full_chunks_len = (slice.len() / 8) * 8;

            for i in (0..full_chunks_len).step_by(8) {
                seed = xorshift_step(seed);
                let value = seed.wrapping_mul(0x2545F4914F6CDD1D).to_le_bytes();
                slice[i..i + 8].copy_from_slice(&value);
            }

            if full_chunks_len < slice.len() {
                seed = xorshift_step(seed);
                let value = seed.wrapping_mul(0x2545F4914F6CDD1D).to_le_bytes();
                let tail_len = slice.len() - full_chunks_len;
                slice[full_chunks_len..].copy_from_slice(&value[..tail_len]);
            }

            writer.write_all(slice)?;
            remaining -= write_size as u64;
        }

        writer.flush().with_context(|| format!("Failed to flush file: {}", path.display()))?;

        Ok(())
    }

    #[inline(always)]
    fn xorshift_step(mut x: u64) -> u64 {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        x
    }

    #[tokio::test]
    #[ignore = "requires AWS credentials and configured S3 bucket"]
    async fn test_upload_to_glacier() -> anyhow::Result<()> {
        init_tracing()?;

        let config = S3Config::from_env()?;
        let uploader = S3ClientImpl::new(config, Some(StorageClass::Glacier), None).await?;

        let temp_file = NamedTempFile::new()?.path().to_path_buf();
        std::fs::write(&temp_file, b"test data for glacier")?;

        let bucket = S3_BUCKET;
        let key = "test/glacier_upload.txt";
        let etag = uploader.upload(bucket, key, &temp_file).await?;

        assert!(!etag.is_empty());

        std::fs::remove_file(&temp_file)?;
        uploader.client.delete_object().bucket(bucket).key(key).send().await?;

        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires AWS credentials and configured S3 bucket"]
    async fn test_upload_and_restore_from_glacier_with_hash_verification() -> anyhow::Result<()> {
        init_tracing()?;

        let temp_upload_file = NamedTempFile::new()?.path().to_path_buf();
        println!("[0] Generating random binary file...");
        generate_random_file(&temp_upload_file, 6 * 1024 * 1024 * 1024)?;

        let original_hash = calculate_file_hash(&temp_upload_file)?;
        println!("Original file hash: {original_hash}");

        println!("[1] Uploading to Glacier...");

        let bucket = S3_BUCKET;
        let key = "test/glacier_restore_test.bin";

        let config = S3Config::from_env()?;

        // warn: for Glacier, this takes 3-5 hours. for testing, using GlacierIr or
        // STANDARD storage classes after upload for immediate access
        let uploader = S3ClientImpl::new(config, Some(StorageClass::Glacier), None).await?;

        let etag = uploader.upload(bucket, key, &temp_upload_file).await?;
        println!("    Uploaded with ETag: {etag}");

        println!("[2] Initiating Glacier restore (Expedited tier)...");

        let restore_request = RestoreRequest::builder()
            .days(1) // keep restored copy for 1 day
            .glacier_job_parameters(
                GlacierJobParameters::builder()
                    .tier(Tier::Expedited) // Expedited: 1-5 min, Standard: 3-5 hours
                    .build()?
            )
            .build();

        uploader
            .client
            .restore_object()
            .bucket(bucket)
            .key(key)
            .restore_request(restore_request)
            .send()
            .await
            .with_context(|| "Failed to initiate restore")?;

        println!("    Restore initiated. Waiting for object to become available...");

        println!("[3] Waiting for restore to complete (polling)...");
        let max_attempts = 60; // 5 minutes for Expedited
        let mut attempts = 0;

        loop {
            attempts += 1;

            let head_response =
                uploader.client.head_object().bucket(bucket).key(key).send().await?;

            if let Some(restore_status) = head_response.restore() {
                println!("    Restore status: {restore_status}");

                if restore_status.contains("ongoing-request=\"false\"") {
                    println!("    Restore completed!");
                    break;
                }
            }

            if attempts >= max_attempts {
                anyhow::bail!(
                    "Restore timeout: object not available after {max_attempts} attempts"
                );
            }

            println!("Waiting for restore... (attempt {attempts}/{max_attempts})");
            sleep(Duration::from_secs(5)).await;
        }

        println!("[4] Downloading restored object...");
        let get_response = uploader
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| "Failed to download restored object")?;
        println!("    Download completed");

        let downloaded_data = get_response
            .body
            .collect()
            .await
            .with_context(|| "Failed to read object body")?
            .into_bytes();

        println!("[5] Calculate hash of downloaded data");
        let downloaded_hash = calculate_data_hash(&downloaded_data);
        println!("    Downloaded data hash: {downloaded_hash}");

        println!("[6] Verify hashes match");
        assert_eq!(
            original_hash, downloaded_hash,
            "Hash mismatch! Original: {original_hash}, Downloaded: {downloaded_hash}"
        );
        println!("    Hash verification passed!");

        println!("[7] Clearing...");
        std::fs::remove_file(&temp_upload_file)?;
        println!("    Local test file deleteted");

        uploader.client.delete_object().bucket(bucket).key(key).send().await?;
        println!("    Uploaded test object deleteted");

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_upload_success() {
//         let uploader = MockS3Uploader { should_fail: false };

//         let result = uploader.upload("test-bucket", "test-key", "/tmp/test.txt", None).await;

//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), "etag-mock");
//     }

//     #[tokio::test]
//     async fn test_upload_failure() {
//         let uploader = MockS3Uploader { should_fail: true };

//         let result = uploader.upload("test-bucket", "test-key", "/tmp/test.txt", None).await;

//         assert!(result.is_err());
//     }
// }
