// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use crate::domain::grouping::ArchiveFile;
use crate::domain::traits::AnchorTimestamp;

/// Represents a group of archive files from multiple servers
/// that should be processed together based on timestamp proximity.
///
/// All files within a group are within specified window of the anchor_timestamp.
#[derive(Debug, Clone)]
pub struct ArchiveGroup {
    /// The reference timestamp for this group
    pub anchor_timestamp: AnchorTimestamp,
    /// Archive files from one or more servers with timestamps within the window
    pub archive_files: Vec<ArchiveFile>,
}

impl ArchiveGroup {
    pub fn new(anchor_timestamp: AnchorTimestamp, archive_files: Vec<ArchiveFile>) -> Self {
        ArchiveGroup { anchor_timestamp, archive_files }
    }

    pub fn file_count(&self) -> usize {
        self.archive_files.len()
    }

    pub fn paths(&self) -> Vec<PathBuf> {
        self.archive_files.iter().map(|af| af.path.clone()).collect()
    }
}
