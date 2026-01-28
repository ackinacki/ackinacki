// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use crate::domain::traits::AnchorTimestamp;

/// Constructs a daily database filename from a timestamp.
pub fn daily_db_filename(timestamp: AnchorTimestamp) -> String {
    format!("{timestamp}.db")
}

/// Constructs the path to a daily database in the daily directory.
pub fn daily_db_path(daily_dir: &std::path::Path, timestamp: AnchorTimestamp) -> PathBuf {
    daily_dir.join(daily_db_filename(timestamp))
}

/// Constructs an S3 object key from a file path.
/// Removes leading "./" if present for consistent key format.
pub fn s3_key_from_path(path: &std::path::Path) -> String {
    path
        .strip_prefix(".") // remove leading "./" if present
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daily_db_filename() {
        assert_eq!(daily_db_filename(1234567890), "1234567890.db");
    }

    #[test]
    fn test_daily_db_path() {
        let dir = PathBuf::from("/data/daily");
        let path = daily_db_path(&dir, 1000);
        assert_eq!(path, PathBuf::from("/data/daily/1000.db"));
    }

    #[test]
    fn test_s3_key_with_leading_dot() {
        let path = std::path::PathBuf::from("./daily/1000.db");
        let key = s3_key_from_path(&path);
        assert_eq!(key, "daily/1000.db");
    }

    #[test]
    fn test_s3_key_without_leading_dot() {
        let path = std::path::PathBuf::from("daily/1000.db");
        let key = s3_key_from_path(&path);
        assert_eq!(key, "daily/1000.db");
    }

    #[test]
    fn test_s3_key_complex_path() {
        let path = std::path::PathBuf::from("./archive/2025/daily/1234567890.db");
        let key = s3_key_from_path(&path);
        assert_eq!(key, "archive/2025/daily/1234567890.db");
    }
}
