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
    pub fn new(protocol_version: ProtocolVersion, config: GlobalConfig) -> Self {
        Self {
            node_config: ConfigWithVersion { config: Arc::new(config), version: protocol_version },
            retired_node_config: None,
        }
    }

    pub fn with_retired(mut self, version: ProtocolVersion, config: GlobalConfig) -> Self {
        self.retired_node_config = Some(ConfigWithVersion { config: Arc::new(config), version });
        self
    }

    pub fn get(&self, block_version: &ProtocolVersion) -> Option<Arc<GlobalConfig>> {
        if block_version == &self.node_config.version
            || block_version == &ProtocolVersion::parse("None").unwrap()
        {
            return Some(self.node_config.config.clone());
        }
        self.retired_node_config
            .as_ref()
            .filter(|config| block_version == &config.version)
            .map(|config| config.config.clone())
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
