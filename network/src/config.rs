// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;

use serde::de::SeqAccess;
use serde::de::Visitor;
use serde::ser::SerializeSeq;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use transport_layer::NetCredential;
use transport_layer::TlsCertCache;

use crate::pub_sub::CertFile;
use crate::pub_sub::CertStore;
use crate::pub_sub::PrivateKeyFile;

#[derive(Clone, Eq, PartialEq, Default)]
pub struct SocketAddrSet(Vec<SocketAddr>);

impl SocketAddrSet {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn with_addr(addr: SocketAddr) -> Self {
        Self(vec![addr])
    }

    pub fn to_vec(&self) -> Vec<SocketAddr> {
        self.0.clone()
    }

    pub fn retain(&mut self, filter: impl FnMut(&SocketAddr) -> bool) {
        self.0.retain(filter);
    }

    pub fn insert(&mut self, addr: SocketAddr) -> bool {
        match self.0.binary_search(&addr) {
            Ok(_) => false,
            Err(pos) => {
                self.0.insert(pos, addr);
                true
            }
        }
    }

    pub fn extend(&mut self, addrs: impl IntoIterator<Item = SocketAddr>) {
        for addr in addrs.into_iter() {
            self.insert(addr);
        }
    }

    pub fn contains(&self, addr: &SocketAddr) -> bool {
        self.0.binary_search(addr).is_ok()
    }

    pub fn remove(&mut self, addr: &SocketAddr) -> bool {
        match self.0.binary_search(addr) {
            Ok(pos) => {
                self.0.remove(pos);
                true
            }
            Err(_) => false,
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &SocketAddr> {
        self.0.iter()
    }
}

impl Debug for SocketAddrSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl IntoIterator for SocketAddrSet {
    type IntoIter = std::vec::IntoIter<SocketAddr>;
    type Item = SocketAddr;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a SocketAddrSet {
    type IntoIter = std::slice::Iter<'a, SocketAddr>;
    type Item = &'a SocketAddr;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl FromIterator<SocketAddr> for SocketAddrSet {
    fn from_iter<T: IntoIterator<Item = SocketAddr>>(iter: T) -> Self {
        let mut inner: Vec<_> = iter.into_iter().collect();
        inner.sort_unstable();
        inner.dedup();
        Self(inner)
    }
}

impl From<Vec<SocketAddr>> for SocketAddrSet {
    fn from(mut value: Vec<SocketAddr>) -> Self {
        value.sort_unstable();
        value.dedup();
        Self(value)
    }
}

impl From<SocketAddrSet> for Vec<SocketAddr> {
    fn from(value: SocketAddrSet) -> Self {
        value.0
    }
}

impl Serialize for SocketAddrSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for addr in &self.0 {
            seq.serialize_element(addr)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for SocketAddrSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SocketSetVisitor;

        impl<'de> Visitor<'de> for SocketSetVisitor {
            type Value = SocketAddrSet;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a list of SocketAddr")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<SocketAddrSet, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(addr) = seq.next_element::<SocketAddr>()? {
                    vec.push(addr);
                }
                vec.sort_unstable();
                vec.dedup();
                Ok(SocketAddrSet(vec))
            }
        }

        deserializer.deserialize_seq(SocketSetVisitor)
    }
}

#[derive(Clone, PartialEq)]
pub struct NetworkConfig {
    pub bind: SocketAddr,
    pub credential: NetCredential,
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
        trusted_certs: CertStore,
        trusted_pubkeys: HashSet<transport_layer::VerifyingKey>,
        tls_cert_cache: Option<TlsCertCache>,
    ) -> anyhow::Result<Self> {
        tracing::info!("Creating new network configuration with bind: {}", bind);
        let (my_certs, my_key) = my_cert.resolve(&my_key, my_ed_keys, tls_cert_cache)?;
        let credential = NetCredential {
            my_certs,
            my_key,
            trusted_pubkeys,
            trusted_cert_hashes: trusted_certs.cert_hashes(),
        };
        Ok(Self { bind, credential })
    }
}
