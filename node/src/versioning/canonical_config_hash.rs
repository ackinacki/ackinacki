use std::fmt::Display;

#[derive(Debug)]
#[allow(dead_code)]
pub struct CanonicalConfigHash(String);

impl Display for CanonicalConfigHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::convert::From<&crate::config::GlobalConfig> for CanonicalConfigHash {
    fn from(config: &crate::config::GlobalConfig) -> Self {
        use bincode::Options;
        let mut canonical_bytes = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_little_endian()
            .serialize(&(
                &config.time_to_produce_block_millis,
                &config.block_keeper_epoch_code_hash,
                &config.block_keeper_preepoch_code_hash,
                &config.thread_count_soft_limit,
                &config.thread_load_window_size,
                &config.chance_of_successful_attack,
                &config.round_min_time_millis,
                &config.round_step_millis,
                &config.round_max_time_millis,
                &config.blockchain_config_hash,
                &config.tracked_ext_out_account_routings,
            ))
            .expect("serialization should succeed");
        // Retired configs supplied via -r must retain their original protocol
        // identifier. The partitioned-format protocol commits one extra u64le.
        if config.engine_version >= semver::Version::new(1, 0, 8) {
            canonical_bytes.extend_from_slice(&config.snapshot_partition_size_bytes.to_le_bytes());
        }
        let canonical_hash = blake3::hash(&canonical_bytes);
        let hash_str = canonical_hash.to_hex().to_string();
        // #[cfg(feature = "test_config_hash")]
        // let hash_str = option_env!("TEST_CONFIG_HASH").map(|s| s.to_string()).unwrap_or(hash_str);
        tracing::trace!(
            "CanonicalConfigHash::from(), {} {}",
            config.blockchain_config_hash,
            hash_str
        );
        CanonicalConfigHash(hash_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalConfig;

    #[test]
    fn partition_size_changes_new_protocol_config_hash() {
        for version in [semver::Version::new(1, 0, 8), semver::Version::new(1, 0, 9)] {
            let config = GlobalConfig { engine_version: version, ..GlobalConfig::default() };
            let mut changed = config.clone();
            changed.snapshot_partition_size_bytes = 1024;
            assert_ne!(
                CanonicalConfigHash::from(&config).to_string(),
                CanonicalConfigHash::from(&changed).to_string()
            );
        }
    }

    #[test]
    fn retired_protocol_preserves_original_hash_recipe() {
        use bincode::Options;
        let mut config = GlobalConfig {
            engine_version: semver::Version::new(1, 0, 7),
            ..GlobalConfig::default()
        };
        let bytes = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_little_endian()
            .serialize(&(
                &config.time_to_produce_block_millis,
                &config.block_keeper_epoch_code_hash,
                &config.block_keeper_preepoch_code_hash,
                &config.thread_count_soft_limit,
                &config.thread_load_window_size,
                &config.chance_of_successful_attack,
                &config.round_min_time_millis,
                &config.round_step_millis,
                &config.round_max_time_millis,
                &config.blockchain_config_hash,
                &config.tracked_ext_out_account_routings,
            ))
            .unwrap();
        let expected = blake3::hash(&bytes).to_hex().to_string();
        assert_eq!(CanonicalConfigHash::from(&config).to_string(), expected);
        config.snapshot_partition_size_bytes = 1024;
        assert_eq!(CanonicalConfigHash::from(&config).to_string(), expected);
    }
}
