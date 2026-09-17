// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use clap::Parser;
use clap::ValueEnum;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum ServersMatchMode {
    All,
    Any,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum CompressionMode {
    None,
    Gzip,
    Xz,
}

/// What to do with compressed daily DB after successful S3 upload.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum PostUploadAction {
    /// Keep the file in place
    Keep,
    /// Delete the file
    Delete,
    /// Move the file to the uploaded directory
    Move,
}

#[derive(Parser, Debug)]
#[command(version, about = "Selection and grouping of the SQLite archives by timestamp (±1h)")]
pub struct Args {
    /// Path to archives storage incoming/processed (defult: CWD)
    #[arg(long, default_value = ".")]
    pub root: PathBuf,

    /// Incoming archives
    #[arg(long, default_value = "incoming")]
    pub incoming: String,

    /// Store daily merged
    #[arg(long, default_value = "daily")]
    pub daily: String,

    /// Path to store processed DB
    #[arg(long, default_value = "processed")]
    pub processed: String,

    /// Compression mode for processed daily DB
    #[arg(long, value_enum, default_value_t = CompressionMode::Gzip)]
    pub compression: CompressionMode,

    /// Servers matching mode for grouping archives
    #[arg(long, value_enum, default_value_t = ServersMatchMode::Any)]
    pub servers_match_mode: ServersMatchMode,

    /// Path to full database with all dailies merged
    #[arg(long, default_value = "./db/bm-archive.db")]
    pub full_db: PathBuf,

    /// S3 bucket name (overrides default)
    #[arg(long, env = "S3_BUCKET")]
    pub bucket: Option<String>,

    /// Action after successful S3 upload: keep, delete, or move to uploaded dir
    #[arg(long, value_enum, default_value_t = PostUploadAction::Move)]
    pub post_upload: PostUploadAction,

    /// Skip uploading to S3 Glacier
    #[arg(long, default_value_t = false, conflicts_with = "upload_later")]
    pub skip_upload: bool,

    /// Skip S3 upload and enqueue compressed daily for later upload via --upload-only
    #[arg(long, default_value_t = false, conflicts_with = "skip_upload")]
    pub upload_later: bool,

    /// Upload a single file to S3 and apply post-upload action, then exit
    #[arg(long, conflicts_with_all = ["skip_upload", "upload_later"])]
    pub upload_only: Option<PathBuf>,

    /// Make missing rows fatal instead of merely logged, and run PRAGMA
    /// quick_check on the assembled daily (hours on large databases)
    ///
    /// The row check covers both merges — source into daily, and daily into the
    /// full DB — and runs after the rows it checks are committed. A failure
    /// while assembling the daily keeps everything out of the full DB; a failure
    /// on the daily-to-full merge leaves that day partially applied there. The
    /// merge is INSERT OR IGNORE, so a retry adds no duplicates, but the group
    /// keeps failing until the cause is fixed.
    #[arg(long, default_value_t = false)]
    pub paranoid: bool,

    /// Command to run on the daily DB after it is built and before it is merged
    /// into the full DB, e.g. to trim it (see NODE-3707).
    ///
    /// Split on whitespace, no shell involved (quoting is not interpreted); the
    /// first token is looked up on PATH. `{}` is replaced with the daily DB
    /// path, or the path is appended when absent.
    /// A non-zero exit status aborts processing of the group, so a failed trim
    /// can never let an unverified daily reach the full DB.
    #[arg(long, value_name = "CMD")]
    pub daily_hook: Option<String>,

    /// Dry run mode (don't upload or move files)
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}
