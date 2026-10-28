//! Actors for one thread-snapshot import attempt.
//!
//! `ThreadSnapshotImporter` owns semantic validation and the staged archive.
//! `ThreadSnapshotDownloader` owns source selection and the download state.
//! Four `PartitionDownloader`s perform independent, single-source attempts.
//! Commands and command-specific responses live with their receiving actor;
//! every request supplies its own reply channel. No actor shares mutable state.
//!
//! A ready file remains leased until the importer acknowledges it. Finishing a
//! transfer immediately frees its worker, regardless of import progress. Only
//! the global disk-cache budget may hold back further downloads. Accounts are
//! imported before messages, but either phase consumes files in readiness order.
mod partition_downloader;
mod thread_snapshot_downloader;
mod thread_snapshot_importer;

pub(crate) use partition_downloader::BlobDownloadLimits;
pub(crate) use thread_snapshot_importer::import_snapshot;
