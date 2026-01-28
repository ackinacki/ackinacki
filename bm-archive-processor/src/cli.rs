use std::path::PathBuf;

use clap::Parser;

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

    /// Compress processed DB
    #[arg(long, default_value_t = true)]
    pub compress: bool,

    /// Process archives from all BM
    #[arg(long, default_value_t = true)]
    pub require_all_servers: bool,

    /// Path to full database with all dailies merged
    #[arg(long, default_value = "./db/bm-archive.db")]
    pub full_db: PathBuf,

    /// S3 bucket name (overrides default)
    #[arg(long, env = "S3_BUCKET")]
    pub bucket: Option<String>,

    /// Skip uploading to S3 Glacier
    #[arg(long, default_value_t = false)]
    pub skip_upload: bool,

    /// Dry run mode (don't upload or move files)
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}
