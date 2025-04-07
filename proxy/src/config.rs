use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use itertools::Itertools;
use network::pub_sub::CertFile;
use network::pub_sub::CertStore;
use network::pub_sub::PrivateKeyFile;
use network::socket_addr::StringSocketAddr;
use network::socket_addr::ToOneSocketAddr;
use network::TlsConfig;
use serde::Deserialize;
use serde::Serialize;
use tokio::signal::unix::signal;
use tokio::signal::unix::SignalKind;
use url::Url;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProxyConfig {
    pub bind: SocketAddr,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gossip: Option<GossipConfig>,

    /// Should be the same as it specified in proxy list
    #[serde(default, deserialize_with = "network::deserialize_optional_publisher_url")]
    pub my_url: Option<Url>,
    pub my_cert: CertFile,
    pub my_key: PrivateKeyFile,
    pub peer_certs: CertStore,
    #[serde(
        serialize_with = "network::serialize_subscribe",
        deserialize_with = "network::deserialize_subscribe"
    )]
    pub subscribe: Vec<Vec<Url>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GossipConfig {
    /// UDP socket address to listen gossip.
    /// Defaults to "127.0.0.1:10000"
    #[serde(default = "default_gossip_listen_addr")]
    pub listen_addr: StringSocketAddr,

    /// Gossip advertise socket address.
    /// Defaults to `bind` address
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertise_addr: Option<StringSocketAddr>,

    /// Gossip seed nodes socket addresses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seeds: Vec<StringSocketAddr>,

    /// Chitchat cluster id for gossip
    #[serde(default = "default_chitchat_cluster_id")]
    pub cluster_id: String,
}

fn default_gossip_listen_addr() -> StringSocketAddr {
    StringSocketAddr::from("127.0.0.1:10000")
}

fn default_chitchat_cluster_id() -> String {
    "acki_nacki".to_string()
}

impl ProxyConfig {
    pub(crate) fn tls_config(&self) -> TlsConfig {
        TlsConfig {
            my_cert: self.my_cert.clone(),
            my_key: self.my_key.clone(),
            peer_certs: self.peer_certs.clone(),
        }
    }

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
}

impl GossipConfig {
    pub fn get_seeds(&self) -> Vec<String> {
        self.seeds
            .iter()
            .map(|s| {
                s.try_to_socket_addr().map_err(|e| {
                    tracing::error!(
                        "Failed to convert gossip seed {} to SocketAddr, skip it ({})",
                        s,
                        e
                    );
                    e
                })
            })
            .filter_map(|res| res.map(|socket| socket.to_string()).ok())
            .collect_vec()
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
