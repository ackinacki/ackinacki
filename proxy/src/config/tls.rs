use std::path::Path;
use std::path::PathBuf;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use serde::Deserialize;
use serde::Serialize;
use wtransport::tls::Certificate;
use wtransport::tls::PrivateKey;
use wtransport::tls::Sha256Digest;

#[derive(Debug, Clone)]
pub struct CertificateWrapper {
    pub path: PathBuf,
    pub cert: Certificate,
}

impl CertificateWrapper {
    pub fn try_new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let cert_der = CertificateDer::from_pem_file(&path)?;
        let cert = Certificate::from_der(cert_der.to_vec())?;
        let path = path.as_ref().to_path_buf();

        Ok(CertificateWrapper { path, cert })
    }

    pub fn hash(&self) -> Sha256Digest {
        self.cert.hash()
    }
}

impl Serialize for CertificateWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.path.to_string_lossy().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CertificateWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;
        Self::try_new(&path).map_err(serde::de::Error::custom)
    }
}

impl From<&CertificateWrapper> for CertificateDer<'static> {
    fn from(value: &CertificateWrapper) -> Self {
        Self::from(value.cert.der().to_vec())
    }
}

impl From<CertificateWrapper> for CertificateDer<'static> {
    fn from(value: CertificateWrapper) -> Self {
        Self::from(value.cert.der().to_vec())
    }
}

#[derive(Debug)]
pub struct PrivateKeyWrapper {
    pub path: PathBuf,
    pub key: PrivateKey,
}

impl PrivateKeyWrapper {
    pub fn try_new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let key_der = PrivateKeyDer::from_pem_file(&path)?;
        let key = PrivateKey::from_der_pkcs8(key_der.secret_der().to_vec());
        let path = path.as_ref().to_path_buf();

        Ok(PrivateKeyWrapper { path, key })
    }
}

impl Clone for PrivateKeyWrapper {
    fn clone(&self) -> Self {
        Self { path: self.path.clone(), key: self.key.clone_key() }
    }
}

impl Serialize for PrivateKeyWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.path.to_string_lossy().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PrivateKeyWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;
        Self::try_new(&path).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<&PrivateKeyWrapper> for PrivateKeyDer<'static> {
    type Error = anyhow::Error;

    fn try_from(value: &PrivateKeyWrapper) -> Result<Self, Self::Error> {
        Self::try_from(value.key.secret_der().to_vec()).map_err(|err| anyhow::anyhow!("{err}"))
    }
}

impl TryFrom<PrivateKeyWrapper> for PrivateKeyDer<'static> {
    type Error = anyhow::Error;

    fn try_from(value: PrivateKeyWrapper) -> Result<Self, Self::Error> {
        Self::try_from(value.key.secret_der().to_vec()).map_err(|err| anyhow::anyhow!("{err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_certificate_wrapper() {
        let cert = CertificateWrapper::try_new("certs/server.ca.pem").unwrap();
        assert_eq!(cert.path, Path::new("certs/server.ca.pem"));
    }

    #[test]
    fn test_private_key_wrapper() {
        let key = PrivateKeyWrapper::try_new("certs/server.key.pem").unwrap();
        assert_eq!(key.path, Path::new("certs/server.key.pem"));
    }
}
