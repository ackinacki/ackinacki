// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::net::SocketAddr;

use url::Url;

use crate::pub_sub::CertFile;
use crate::pub_sub::CertStore;
use crate::pub_sub::PrivateKeyFile;
use crate::tls::TlsConfig;

#[derive(Clone)]
pub struct NetworkConfig {
    pub bind: SocketAddr,
    pub my_cert: CertFile,
    pub my_key: PrivateKeyFile,
    pub peer_certs: CertStore,
    pub subscribe: Vec<Vec<Url>>,
    pub proxies: Vec<Url>,
}

impl Debug for NetworkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkConfig").field("bind", &self.bind).finish()
    }
}

impl NetworkConfig {
    pub fn new(
        bind: SocketAddr,
        my_cert: CertFile,
        my_key: PrivateKeyFile,
        peer_certs: CertStore,
        subscribe: Vec<Vec<Url>>,
        proxies: Vec<Url>,
    ) -> Self {
        tracing::info!("Creating new network configuration with bind: {}", bind);
        Self { bind, my_cert, my_key, subscribe, peer_certs, proxies }
    }

    pub fn tls_config(&self) -> TlsConfig {
        TlsConfig {
            my_cert: self.my_cert.clone(),
            my_key: self.my_key.clone(),
            peer_certs: self.peer_certs.clone(),
        }
    }
}
