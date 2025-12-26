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
        let canonical_bytes = bincode::DefaultOptions::new()
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
            ))
            .expect("serialization should succeed");
        let canonical_hash = blake3::hash(&canonical_bytes);
        let hash_str = canonical_hash.to_hex().to_string();
        #[cfg(feature = "test_config_hash")]
        let hash_str = option_env!("TEST_CONFIG_HASH").map(|s| s.to_string()).unwrap_or(hash_str);
        CanonicalConfigHash(hash_str)
    }
}
