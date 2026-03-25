// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;
use std::path::PathBuf;

use crate::cli::CompressionMode as CliCompressionMode;
use crate::cli::PostUploadAction;
use crate::cli::ServersMatchMode;
use crate::domain::traits::CompressionMode;
use crate::Args;

const DEFAULT_AWS_BUCKET: &str = "ackinacki-bm-archive";

#[derive(Clone)]
pub struct AppConfig {
    pub incoming_dir: PathBuf,
    pub daily_dir: PathBuf,
    pub processed_dir: PathBuf,
    pub full_db: PathBuf,
    pub bucket: String,
    pub require_all_servers: bool,
    pub compression: CompressionMode,
    pub post_upload: PostUploadAction,
    pub skip_upload: bool,
    pub dry_run: bool,
}

impl AppConfig {
    pub fn from_args(args: Args) -> Self {
        Self {
            incoming_dir: args.root.join(&args.incoming),
            daily_dir: args.root.join(&args.daily),
            processed_dir: args.root.join(&args.processed),
            full_db: args.full_db,
            bucket: args.bucket.unwrap_or_else(|| DEFAULT_AWS_BUCKET.to_string()),
            require_all_servers: matches!(args.servers_match_mode, ServersMatchMode::All),
            compression: match args.compression {
                CliCompressionMode::None => CompressionMode::None,
                CliCompressionMode::Gzip => CompressionMode::Gzip,
                CliCompressionMode::Xz => CompressionMode::Xz,
            },
            post_upload: args.post_upload,
            skip_upload: args.skip_upload,
            dry_run: args.dry_run,
        }
    }
}

impl fmt::Display for AppConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "incoming={}", self.incoming_dir.display())?;
        writeln!(f, "daily={}", self.daily_dir.display())?;
        writeln!(f, "processed={}", self.processed_dir.display())?;
        writeln!(f, "full_db={}", self.full_db.display())?;
        writeln!(f, "bucket={}", self.bucket)?;
        writeln!(f, "require_all_servers={}", self.require_all_servers)?;
        writeln!(f, "compression={:?}", self.compression)?;
        writeln!(f, "post_upload={:?}", self.post_upload)?;
        writeln!(f, "skip_upload={}", self.skip_upload)?;
        write!(f, "dry_run={}", self.dry_run)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_context_creation() {
        let args = Args {
            root: PathBuf::from("/tmp"),
            incoming: "in".to_string(),
            daily: "day".to_string(),
            processed: "proc".to_string(),
            compression: CliCompressionMode::Gzip,
            servers_match_mode: ServersMatchMode::All,
            full_db: PathBuf::from("/tmp/full.db"),
            bucket: None,
            post_upload: PostUploadAction::Move,
            skip_upload: false,
            dry_run: false,
        };

        let cfg = AppConfig::from_args(args);
        assert_eq!(cfg.incoming_dir, PathBuf::from("/tmp/in"));
        assert_eq!(cfg.daily_dir, PathBuf::from("/tmp/day"));
        assert_eq!(cfg.bucket, DEFAULT_AWS_BUCKET);
        assert_eq!(cfg.compression, CompressionMode::Gzip);
    }
}
