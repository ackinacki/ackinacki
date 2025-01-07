use std::collections::HashMap;
use std::path::Path;

use wtransport::tls::Certificate;
use wtransport::tls::CertificateChain;
use wtransport::tls::Sha256Digest;

use super::serde::ConnectionConfig;

#[derive(Debug, Clone)]
pub struct ConfigStore {
    pub config: super::serde::Config,

    /// index of certificates by their hash
    cert_hash_to_name: HashMap<Sha256Digest, String>,
}

impl ConfigStore {
    pub async fn load(path: impl AsRef<Path>) -> ConfigStore {
        Self::try_load(path).expect("failed to load config")
    }

    pub fn try_load(path: impl AsRef<Path>) -> anyhow::Result<ConfigStore> {
        let config = super::serde::Config::read_from_file(path)?;

        let mut cert_hash_to_name = HashMap::new();

        for (key, value) in config.connections.iter() {
            cert_hash_to_name.insert(value.cert.hash(), key.to_string());
        }

        Ok(ConfigStore { config, cert_hash_to_name })
    }

    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        self.config.write_to_file(path)
    }

    pub fn get_name_by_cert(&self, cert: &Certificate) -> Option<String> {
        self.cert_hash_to_name.get(&cert.hash()).cloned()
    }

    pub fn get_name_by_certchain(&self, chain: &CertificateChain) -> Option<String> {
        // TODO: ideally we should check that the chain is not empty
        chain.as_slice().first().and_then(|cert| self.get_name_by_cert(cert))
    }

    pub fn in_connections(&self) -> impl Iterator<Item = &ConnectionConfig> {
        self.config.connections.values().filter(|c| c.outer.is_none())
    }

    pub fn out_connections(&self) -> impl Iterator<Item = &ConnectionConfig> {
        self.config
            .connections
            .values()
            .filter(|c| matches!(&c.outer, Some(outer) if outer.enabled))
    }
}
