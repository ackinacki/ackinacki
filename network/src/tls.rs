use serde::Deserialize;
use serde::Serialize;
use transport_layer::NetCredential;

use crate::host_id_prefix;
use crate::pub_sub::CertFile;
use crate::pub_sub::CertStore;
use crate::pub_sub::PrivateKeyFile;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TlsConfig {
    pub my_cert: CertFile,
    pub my_key: PrivateKeyFile,
    pub peer_certs: CertStore,
}

impl TlsConfig {
    pub fn is_debug(&self) -> bool {
        self.my_cert.is_debug()
    }

    pub fn my_host_id(&self) -> String {
        self.my_cert.resolve_host_id(&self.my_key)
    }

    pub fn my_host_id_prefix(&self) -> String {
        host_id_prefix(&self.my_cert.resolve_host_id(&self.my_key)).to_string()
    }

    pub fn credential(&self) -> NetCredential {
        let (my_certs, my_key) = self.my_cert.resolve(&self.my_key).unwrap();
        NetCredential { my_certs, my_key, cert_validation: Default::default() }
    }
}
