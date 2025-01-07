use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;
use serde::Serialize;

use super::tls::CertificateWrapper;
use super::tls::PrivateKeyWrapper;

/// Proxy configuration
///
/// # Examples
///
/// ```yaml
/// server:
///   cert: certs/server.ca.pem
///   key: certs/server.key.pem
/// connections:
///   connection1:
///     subscriber: true
///     publisher: true
///     tags:
///       - tag1
///     cert: certs/client.ca.pem
///     # outer is optional
///     outer:
///       enabled: true
///       url: https://localhost:4433/1/2/3
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub connections: HashMap<String, ConnectionConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub cert: CertificateWrapper,
    pub key: PrivateKeyWrapper,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionConfig {
    pub subscriber: bool,
    pub publisher: bool,
    pub tags: Vec<String>,
    pub cert: CertificateWrapper,
    pub outer: Option<OuterConnectionConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct OuterConnectionConfig {
    pub enabled: bool,
    pub url: String,
}

impl Config {
    /// Read config from file
    pub fn read_from_file(path: impl AsRef<Path>) -> anyhow::Result<Config> {
        let file = std::fs::File::open(path.as_ref())?;
        serde_yaml::from_reader(file).context("Failed to load config")
    }

    /// Write config to file
    pub fn write_to_file(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let file = std::fs::File::create(path.as_ref())?;
        let mut writer = std::io::BufWriter::new(file);
        serde_yaml::to_writer(&mut writer, self).context("Failed to save config")?;
        writer.flush()?;
        Ok(())
    }

    pub fn try_clone_from(&mut self, other: &Config) -> anyhow::Result<()> {
        self.server = other.server.clone();
        self.connections = other.connections.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let config = r#"
        server:
          cert: certs/server.ca.pem
          key: certs/server.key.pem
        connections:
          connection1:
            subscriber: true
            publisher: true
            tags:
              - tag1
            cert: certs/client.ca.pem
            outer:
              enabled: true
              url: https://localhost:4433
        "#;
        let config: Config = serde_yaml::from_str(config).unwrap();
        assert_eq!(config.connections.len(), 1);
        assert_eq!(config.connections.iter().next().unwrap().1.tags.len(), 1);
        assert_eq!(
            config.connections.iter().next().unwrap().1.outer,
            Some(OuterConnectionConfig {
                enabled: true,
                url: "https://localhost:4433".to_string()
            })
        );
    }
}
