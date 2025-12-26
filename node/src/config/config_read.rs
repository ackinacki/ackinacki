use std::fmt::Debug;
use std::sync::Arc;

use crate::config::GlobalConfig;
use crate::versioning::ProtocolVersion;

#[derive(Clone)]
struct ConfigWithVersion {
    // TODO: change local GlobalConfig usages to Arc<>
    config: Arc<GlobalConfig>,
    version: ProtocolVersion,
}

#[derive(Clone)]
pub struct ConfigRead {
    node_config: ConfigWithVersion,
    retired_node_config: Option<ConfigWithVersion>,
}

impl ConfigRead {
    pub fn new(
        preferred_protocol_version: ProtocolVersion,
        config: GlobalConfig,
        retired_version: Option<ProtocolVersion>,
        retired_config: Option<GlobalConfig>,
    ) -> Self {
        let retired_node_config = if let Some(retired_version) = retired_version {
            assert!(
                retired_config.is_some(),
                "retired config must not be empty if retired version is some"
            );
            let retired_config = retired_config.unwrap();
            Some(ConfigWithVersion { config: Arc::new(retired_config), version: retired_version })
        } else {
            None
        };
        Self {
            node_config: ConfigWithVersion {
                config: Arc::new(config),
                version: preferred_protocol_version,
            },
            retired_node_config,
        }
    }

    pub fn get(&self, block_version: &ProtocolVersion) -> Option<Arc<GlobalConfig>> {
        if let Some(prev_config) = &self.retired_node_config {
            if block_version == &prev_config.version {
                return Some(prev_config.config.clone());
            }
        }
        if block_version == &self.node_config.version
            || block_version == &ProtocolVersion::parse("None").unwrap()
        {
            return Some(self.node_config.config.clone());
        }
        None
    }

    pub fn is_retired(&self, block_version: &ProtocolVersion) -> bool {
        if let Some(prev_config) = &self.retired_node_config {
            return block_version == &prev_config.version;
        }
        false
    }
}

impl Debug for ConfigRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConfigRead:\n{{\n\"{}\":{},\n\"{}\":{}\n}}",
            self.node_config.version,
            serde_json::to_string_pretty(&self.node_config.config)
                .expect("Failed to serialize config"),
            self.retired_node_config
                .as_ref()
                .map(|c| format!("{}", c.version))
                .unwrap_or("None".to_string()),
            self.retired_node_config
                .as_ref()
                .map(|v| {
                    serde_json::to_string_pretty(&v.config)
                        .expect("Failed to serialize retired config")
                })
                .unwrap_or("None".to_string())
        )
    }
}
