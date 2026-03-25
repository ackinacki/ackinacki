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
    #[arg(long, default_value_t = false)]
    pub skip_upload: bool,

    /// Dry run mode (don't upload or move files)
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}
