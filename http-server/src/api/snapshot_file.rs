//! Serve an already opened immutable snapshot file. Capture metadata under the
//! storage lock using std I/O; asynchronous seeking/streaming starts only after
//! that lock and its spawn_blocking task have been released.
use std::fs::Metadata;
use std::io;
use std::time::UNIX_EPOCH;

use salvo::http::headers::HeaderMapExt;
use salvo::http::headers::IfModifiedSince;
use salvo::http::headers::IfUnmodifiedSince;
use salvo::http::headers::LastModified;
use salvo::http::HeaderMap;
use salvo::http::HttpRange;
use salvo::prelude::*;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

pub(super) struct SnapshotFile {
    file: std::fs::File,
    metadata: Metadata,
    name: String,
}

impl SnapshotFile {
    pub(super) fn new(file: std::fs::File, name: String) -> io::Result<Self> {
        let metadata = file.metadata()?;
        Ok(Self { file, metadata, name })
    }

    // Match NamedFile's legacy inode/size/mtime validator. Managed chunked
    // resources continue to use their stronger footer-derived ETag instead.
    pub(super) fn etag(&self) -> Option<String> {
        let modified = self.metadata.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
        #[cfg(unix)]
        let inode = {
            use std::os::unix::fs::MetadataExt;
            self.metadata.ino()
        };
        #[cfg(not(unix))]
        let inode = 0u64;
        Some(format!(
            "\"{:x}-{:x}-{:x}-{:x}\"",
            inode,
            self.metadata.len(),
            modified.as_secs(),
            modified.subsec_nanos()
        ))
    }

    pub(super) async fn send(self, headers: &HeaderMap, head: bool, res: &mut Response) {
        let _ = res.add_header("Content-Type", "application/octet-stream", true);
        let _ = res.add_header(
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", self.name),
            true,
        );
        let _ = res.add_header("Accept-Ranges", "bytes", true);
        let modified = self.metadata.modified().ok();
        if let Some(modified) = modified {
            res.headers_mut().typed_insert(LastModified::from(modified));
        }

        // Evaluate preconditions before Range (RFC 9110 section 14.2). A cached
        // unchanged file returns 304 even when the requested range is invalid.
        if let (Some(modified), Some(since)) = (modified, headers.typed_get::<IfUnmodifiedSince>())
        {
            if !since.precondition_passes(modified) {
                res.status_code(StatusCode::PRECONDITION_FAILED);
                return;
            }
        }
        if let (Some(modified), Some(since)) = (modified, headers.typed_get::<IfModifiedSince>()) {
            if !since.is_modified(modified) {
                res.status_code(StatusCode::NOT_MODIFIED);
                return;
            }
        }

        let total = self.metadata.len();
        let (offset, length) = if let Some(value) = headers.get("range") {
            let Ok(value) = value.to_str() else {
                res.status_code(StatusCode::BAD_REQUEST);
                return;
            };
            let Ok(ranges) = HttpRange::parse(value, total) else {
                let _ = res.add_header("Content-Range", format!("bytes */{total}"), true);
                res.status_code(StatusCode::RANGE_NOT_SATISFIABLE);
                return;
            };
            // As with the previous NamedFile response, serve the first range.
            let Some(range) = ranges.first().filter(|range| {
                range.length > 0 && range.start < total && range.length <= total - range.start
            }) else {
                let _ = res.add_header("Content-Range", format!("bytes */{total}"), true);
                res.status_code(StatusCode::RANGE_NOT_SATISFIABLE);
                return;
            };
            let _ = res.add_header(
                "Content-Range",
                format!("bytes {}-{}/{total}", range.start, range.start + range.length - 1),
                true,
            );
            res.status_code(StatusCode::PARTIAL_CONTENT);
            (range.start, range.length)
        } else {
            res.status_code(StatusCode::OK);
            (0, total)
        };
        let _ = res.add_header("Content-Length", length.to_string(), true);
        if head || length == 0 {
            return;
        }

        // No storage lock is held, and this is an async task, not a blocking
        // worker waiting for more work from its own pool. Keep this descriptor
        // even if GC removes/replaces the resource path in the meantime.
        let mut file = tokio::fs::File::from_std(self.file);
        if file.seek(io::SeekFrom::Start(offset)).await.is_err() {
            res.headers_mut().remove("content-length");
            res.headers_mut().remove("content-range");
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            return;
        }
        res.stream(futures_util::stream::try_unfold(
            (file, length),
            |(mut file, remaining)| async move {
                if remaining == 0 {
                    return Ok::<_, io::Error>(None);
                }
                let mut bytes = vec![0; remaining.min(1024 * 1024) as usize];
                let count = file.read(&mut bytes).await?;
                if count == 0 {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                }
                bytes.truncate(count);
                Ok(Some((bytes, (file, remaining - count as u64))))
            },
        ));
    }
}
