// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use chitchat::Chitchat;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use url::Url;

use crate::gossip::get_live_peers;
use crate::pub_sub::CertFile;
use crate::pub_sub::CertStore;
use crate::pub_sub::PrivateKeyFile;
use crate::NetworkPeerId;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TlsAuthConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Clone)]
pub struct NetworkConfig<PeerId: NetworkPeerId>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    pub bind: SocketAddr,
    pub my_cert: CertFile,
    pub my_key: PrivateKeyFile,
    pub peer_certs: CertStore,
    pub subscribe: Vec<Url>,
    pub gossip: Arc<Mutex<Chitchat>>,
    pub nodes: HashMap<PeerId, SocketAddr>,
}

impl<PeerId: NetworkPeerId> Debug for NetworkConfig<PeerId>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkConfig").field("bind", &self.bind).finish()
    }
}

impl<PeerId: NetworkPeerId> NetworkConfig<PeerId>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    pub fn new(
        bind: SocketAddr,
        my_cert: CertFile,
        my_key: PrivateKeyFile,
        peer_certs: CertStore,
        subscribe: Vec<Url>,
        gossip: Arc<Mutex<Chitchat>>,
    ) -> Self {
        tracing::info!("Creating new network configuration with bind: {}", bind);
        Self { bind, my_cert, my_key, subscribe, gossip, peer_certs, nodes: HashMap::new() }
    }

    pub fn pub_sub_config(&self) -> crate::pub_sub::Config {
        let mut subscribe = self.subscribe.clone();
        if subscribe.is_empty() {
            for addr in self.nodes.values() {
                if let Ok(url) = Url::parse(&format!("https://{addr}")) {
                    subscribe.push(url);
                }
            }
        }
        crate::pub_sub::Config {
            bind: self.bind,
            my_cert: self.my_cert.clone(),
            my_key: self.my_key.clone(),
            peer_certs: self.peer_certs.clone(),
            subscribe,
        }
    }

    pub async fn refresh_alive_nodes(&mut self, filter_self: bool) {
        let chitchat_guard = self.gossip.lock().await;
        let peers = get_live_peers(&chitchat_guard, filter_self);
        tracing::info!("Received gossip live peers: {:?}", peers);
        for (id, address) in peers {
            self.nodes.insert(id, address);
        }
        tracing::info!("Final live peers: {:?}", self.nodes);
    }
}
