use std::collections::HashSet;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use gossip::GossipConfig;
use network::config::NetworkConfig;
use network::pub_sub::CertFile;
use network::pub_sub::CertStore;
use network::pub_sub::PrivateKeyFile;
use serde::Deserialize;
use serde::Serialize;
use tokio::signal::unix::signal;
use tokio::signal::unix::SignalKind;
use transport_layer::TlsCertCache;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProxyConfig {
    pub bind: SocketAddr,

    #[serde(default)]
    pub gossip: GossipConfig,

    /// Should be the same as it specified in proxy list
    #[serde(
        default,
        deserialize_with = "addr_serde::opt_vec_deser",
        serialize_with = "addr_serde::opt_vec_ser"
    )]
    pub my_addr: Option<Vec<SocketAddr>>,
    pub my_cert: CertFile,
    pub my_key: PrivateKeyFile,
    pub peer_certs: CertStore,
    #[serde(default, with = "transport_layer::hex_verifying_keys")]
    pub peer_ed_pubkeys: Vec<transport_layer::VerifyingKey>,
    pub bk_addrs: Vec<SocketAddr>,
    #[serde(
        serialize_with = "network::serialize_subscribe",
        deserialize_with = "network::deserialize_subscribe"
    )]
    pub subscribe: Vec<Vec<SocketAddr>>,
}

impl ProxyConfig {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path.as_ref())?;
        serde_yaml::from_reader(file).context("Failed to load config")
    }

    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let file = std::fs::File::create(path.as_ref())?;
        let mut writer = std::io::BufWriter::new(file);
        serde_yaml::to_writer(&mut writer, self).context("Failed to save config")?;
        writer.flush()?;
        Ok(())
    }

    pub fn network_config(
        &self,
        tls_cert_cache: Option<TlsCertCache>,
    ) -> anyhow::Result<NetworkConfig> {
        NetworkConfig::new(
            self.bind,
            self.my_cert.clone(),
            self.my_key.clone(),
            None,
            self.peer_certs.clone(),
            HashSet::from_iter(self.peer_ed_pubkeys.clone()),
            self.subscribe.clone(),
            vec![],
            tls_cert_cache,
        )
    }
}

pub(crate) async fn config_reload_handler(
    config_updates: tokio::sync::watch::Sender<ProxyConfig>,
    config_path: PathBuf,
) -> anyhow::Result<()> {
    // 1. config error shouldn't be an error
    // 2. every other error should panic
    let mut sig_hup = signal(SignalKind::hangup())?;
    loop {
        sig_hup.recv().await;
        tracing::info!("Received SIGHUP, reloading configuration...");

        match ProxyConfig::from_file(&config_path) {
            Ok(new_config_state) => {
                config_updates.send(new_config_state).expect("failed to send config")
            }
            Err(err) => {
                tracing::error!(
                    "Failed to load new configuration at {}: {:?}",
                    config_path.display(),
                    err
                );
            }
        };
    }
}

mod addr_serde {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;

    use super::*;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        One(T),
        Many(Vec<T>),
    }

    pub fn opt_vec_deser<'de, D>(de: D) -> Result<Option<Vec<SocketAddr>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = Option::<OneOrMany<SocketAddr>>::deserialize(de)?;
        Ok(v.map(|x| match x {
            OneOrMany::One(x) => vec![x],
            OneOrMany::Many(v) => v,
        }))
    }

    pub fn opt_vec_ser<S>(v: &Option<Vec<SocketAddr>>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match v {
            None => ser.serialize_none(),
            Some(vec) if vec.len() == 1 => ser.serialize_some(&vec[0]),
            Some(vec) => ser.serialize_some(vec),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::ProxyConfig;

    #[test]
    fn config_file_match_test() {
        let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.yaml");
        _ = ProxyConfig::from_file(config_path).unwrap();
    }
}
