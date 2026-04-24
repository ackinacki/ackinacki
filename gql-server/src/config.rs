// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

use serde::Deserialize;

use crate::defaults;

#[derive(Debug, Deserialize)]
pub struct GqlServerConfig {
    #[serde(default = "defaults::max_pool_connections")]
    pub max_pool_connections: u32,

    #[serde(default = "defaults::sqlite_query_timeout_secs")]
    pub sqlite_query_timeout_secs: u64,

    #[serde(default = "defaults::acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,

    #[serde(default = "defaults::sqlite_mmap_size")]
    pub sqlite_mmap_size: i64,

    #[serde(default = "defaults::sqlite_cache_size")]
    pub sqlite_cache_size: i64,

    #[serde(default = "defaults::max_attached_db")]
    pub max_attached_db: u16,

    #[serde(default)]
    pub query_duration_boundaries: Option<Vec<f64>>,

    #[serde(default)]
    pub sqlite_query_boundaries: Option<Vec<f64>>,

    /// Enable deprecated API fields at runtime.
    #[serde(default)]
    pub deprecated_api: Option<bool>,
}

impl GqlServerConfig {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path.as_ref())?;
        let config: Self = serde_yaml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.max_pool_connections > 0, "max_pool_connections must be > 0");
        anyhow::ensure!(
            self.sqlite_query_timeout_secs > 0,
            "sqlite_query_timeout_secs must be > 0"
        );
        anyhow::ensure!(self.acquire_timeout_secs > 0, "acquire_timeout_secs must be > 0");
        anyhow::ensure!(
            self.max_attached_db <= 9,
            "max_attached_db={} exceeds SQLite limit of 9",
            self.max_attached_db
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config() {
        let yaml = r#"
max_pool_connections: 20
sqlite_query_timeout_secs: 10
acquire_timeout_secs: 15
sqlite_mmap_size: 1000000
sqlite_cache_size: -500000
max_attached_db: 5
query_duration_boundaries: [10.0, 100.0, 1000.0]
sqlite_query_boundaries: [1.0, 50.0, 500.0]
"#;
        let config: GqlServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.max_pool_connections, 20);
        assert_eq!(config.sqlite_query_timeout_secs, 10);
        assert_eq!(config.acquire_timeout_secs, 15);
        assert_eq!(config.sqlite_mmap_size, 1000000);
        assert_eq!(config.sqlite_cache_size, -500000);
        assert_eq!(config.max_attached_db, 5);
        assert_eq!(config.query_duration_boundaries.unwrap(), vec![10.0, 100.0, 1000.0]);
    }

    #[test]
    fn uses_defaults_for_missing_fields() {
        let yaml = "{}";
        let config: GqlServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.max_pool_connections, defaults::MAX_POOL_CONNECTIONS);
        assert_eq!(config.sqlite_query_timeout_secs, defaults::DEFAULT_SQLITE_QUERY_TIMEOUT_SECS);
        assert_eq!(config.acquire_timeout_secs, defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS);
        assert_eq!(config.sqlite_mmap_size, defaults::DEFAULT_SQLITE_MMAP_SIZE);
        assert_eq!(config.sqlite_cache_size, defaults::DEFAULT_SQLITE_CACHE_SIZE);
        assert_eq!(config.max_attached_db, defaults::MAX_ATTACHED_DB);
        assert!(config.query_duration_boundaries.is_none());
        assert!(config.sqlite_query_boundaries.is_none());
    }

    #[test]
    fn rejects_zero_pool_connections() {
        let yaml = "max_pool_connections: 0";
        let config: GqlServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_timeout() {
        let yaml = "sqlite_query_timeout_secs: 0";
        let config: GqlServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_acquire_timeout() {
        let yaml = "acquire_timeout_secs: 0";
        let config: GqlServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_max_attached_db_over_9() {
        let yaml = "max_attached_db: 10";
        let config: GqlServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }
}
