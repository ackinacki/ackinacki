use std::path::PathBuf;

use salvo::fs::NamedFile;
use salvo::http::headers::HeaderMapExt;
use salvo::prelude::*;
use snapshot_storage::read_guard;
use snapshot_storage::snapshot_headers;
use snapshot_storage::valid_id;

pub struct StorageHandler {
    directory: PathBuf,
}

impl StorageHandler {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }
}

#[async_trait]
impl Handler for StorageHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let name = req.param::<String>("path").unwrap_or_default();
        if name.is_empty() {
            let directory = self.directory.clone();
            let snapshots = tokio::task::spawn_blocking(move || snapshot_headers(&directory)).await;
            match snapshots {
                Ok(Ok(snapshots)) => {
                    let mut listing = String::from("<!doctype html><html><body>");
                    for (id, _) in snapshots {
                        // IDs are validated lowercase hex, never arbitrary HTML.
                        listing.push_str(&format!(
                            "<a href=\"{id}\">{id}</a> <a href=\"{id}.header\">{id}.header</a><br>"
                        ));
                    }
                    listing.push_str("</body></html>");
                    res.render(Text::Html(listing));
                }
                _ => {
                    res.status_code(StatusCode::NOT_FOUND);
                }
            }
            return;
        }
        let id = name.strip_suffix(".header").unwrap_or(&name);
        let partition =
            snapshot_storage::partition_ids(&name).map(|(primary, _)| primary.to_owned());
        if !valid_id(id) && partition.is_none() {
            res.status_code(StatusCode::NOT_FOUND);
            return;
        }
        let path = self.directory.join(&name);
        let directory = self.directory.clone();
        let header = partition.is_none() && name.ends_with(".header");
        let id: [u8; 32] = hex::decode(partition.as_deref().unwrap_or(id))
            .expect("validated resource id")
            .try_into()
            .unwrap();
        let checked = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let guard = read_guard(&directory)?;
            if partition.is_some() {
                anyhow::ensure!(
                    std::fs::symlink_metadata(path.parent().expect("partition parent"))?
                        .file_type()
                        .is_dir(),
                    "invalid partition directory"
                );
            }
            let metadata = std::fs::symlink_metadata(&path)?;
            anyhow::ensure!(metadata.file_type().is_file(), "not a regular resource");
            if header {
                anyhow::ensure!(
                    std::fs::symlink_metadata(directory.join(hex::encode(id)))?
                        .file_type()
                        .is_file(),
                    "primary missing"
                );
            }
            // Legacy monolithic snapshots have no chunk footer and retain
            // NamedFile's validator. New immutable blobs carry their own ETag.
            let blob = if header {
                None
            } else {
                snapshot_storage::chunks::read_footer(&mut std::fs::File::open(&path)?).ok()
            };
            Ok((guard, blob))
        })
        .await;
        let (guard, metadata) = match checked {
            Ok(Ok(value)) => value,
            _ => {
                res.status_code(StatusCode::NOT_FOUND);
                return;
            }
        };
        let file = match NamedFile::open(self.directory.join(name)).await {
            Ok(file) => file,
            Err(_) => {
                res.status_code(StatusCode::NOT_FOUND);
                return;
            }
        };
        if let Some(map) = metadata.as_ref() {
            let _ = res.add_header(
                snapshot_storage::chunks::MAP_OFFSET_HEADER,
                map.offset.to_string(),
                true,
            );
            let _ = res.add_header(
                snapshot_storage::chunks::MAP_LENGTH_HEADER,
                map.length.to_string(),
                true,
            );
        }
        let etag = metadata.map(|metadata| metadata.etag()).or_else(|| {
            let mut headers = salvo::http::HeaderMap::new();
            headers.typed_insert(file.etag()?);
            Some(headers.get("etag")?.to_str().ok()?.to_owned())
        });
        // File descriptor + representation validator are now captured. GC may
        // unlink the name, but an in-flight stream keeps its inode alive.
        drop(guard);
        let mut headers = req.headers().clone();
        if let Some(etag) = &etag {
            if let Some(value) = headers.get("if-match") {
                if !matches_etag(value.to_str().unwrap_or(""), etag, false) {
                    res.status_code(StatusCode::PRECONDITION_FAILED);
                    let _ = res.add_header("ETag", etag, true);
                    return;
                }
            }
            if let Some(value) = headers.get("if-none-match") {
                if matches_etag(value.to_str().unwrap_or(""), etag, true) {
                    res.status_code(StatusCode::NOT_MODIFIED);
                    let _ = res.add_header("ETag", etag, true);
                    return;
                }
                headers.remove("if-modified-since");
            }
        }
        if headers.contains_key("range") && headers.contains_key("if-range") {
            // Dates and weak validators are not used for byte-precise resume.
            let matched = etag.as_ref().is_some_and(|etag| {
                headers.get("if-range").and_then(|value| value.to_str().ok()) == Some(etag.as_str())
            });
            if !matched {
                headers.remove("range");
            }
        }
        headers.remove("if-range");
        headers.remove("if-match");
        headers.remove("if-none-match");
        file.send(&headers, res).await;
        // NamedFile generates an inode validator. Managed resources instead use
        // the map digest stored in the immutable blob footer.
        if let Some(etag) = etag {
            let _ = res.add_header("ETag", etag, true);
        }
        let _ = res.add_header("Cache-Control", "no-transform", true);
        if req.method() == salvo::http::Method::HEAD {
            res.take_body();
        }
    }
}

fn matches_etag(value: &str, etag: &str, weak: bool) -> bool {
    value.split(',').map(str::trim).any(|candidate| {
        candidate == "*"
            || if weak {
                candidate.strip_prefix("W/").unwrap_or(candidate) == etag
            } else {
                candidate == etag
            }
    })
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
