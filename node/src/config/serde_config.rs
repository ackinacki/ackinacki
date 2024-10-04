// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use crate::config::Config;

pub fn load_config_from_file(path: &PathBuf) -> anyhow::Result<Config> {
    std::fs::read_to_string(path)
        .map_err(|e| anyhow::format_err!("Failed to open config file: {e}"))
        .and_then(|config_str| {
            serde_yaml::from_str::<Config>(&config_str)
                .map_err(|e| anyhow::format_err!("Failed to deserialize config: {e}"))
        })
}

pub fn save_config_to_file(config: &Config, path: &PathBuf) -> anyhow::Result<()> {
    let config_str = serde_yaml::to_string(config)
        .map_err(|e| anyhow::format_err!("Failed to serialize config: {e}"))?;
    std::fs::write(path, config_str)?;
    Ok(())
}
