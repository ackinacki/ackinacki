// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;

use transport_layer::NetCredential;
use transport_layer::TlsCertCache;

use crate::pub_sub::CertFile;
use crate::pub_sub::CertStore;
use crate::pub_sub::PrivateKeyFile;

#[derive(Clone, PartialEq)]
pub struct NetworkConfig {
    pub bind: SocketAddr,
    pub credential: NetCredential,
    pub subscribe: Vec<Vec<SocketAddr>>,
    pub proxies: Vec<SocketAddr>,
}

impl Debug for NetworkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkConfig").field("bind", &self.bind).finish()
    }
}

impl NetworkConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bind: SocketAddr,
        my_cert: CertFile,
        my_key: PrivateKeyFile,
        my_ed_keys: &[transport_layer::SigningKey],
        peer_certs: CertStore,
        peer_ed_pubkeys: HashSet<transport_layer::VerifyingKey>,
        subscribe: Vec<Vec<SocketAddr>>,
        proxies: Vec<SocketAddr>,
        tls_cert_cache: Option<TlsCertCache>,
    ) -> anyhow::Result<Self> {
        tracing::info!("Creating new network configuration with bind: {}", bind);
        let (my_certs, my_key) = my_cert.resolve(&my_key, my_ed_keys, tls_cert_cache)?;
        let credential = NetCredential {
            my_certs,
            my_key,
            trusted_ed_pubkeys: peer_ed_pubkeys,
            trusted_cert_hashes: peer_certs.cert_hashes(),
        };
        Ok(Self { bind, credential, subscribe, proxies })
    }
}
