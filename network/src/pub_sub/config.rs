use std::path::Path;
use std::path::PathBuf;

use ::serde::Deserialize;
use ::serde::Serialize;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use wtransport::tls::Certificate;
use wtransport::tls::PrivateKey;
use wtransport::tls::Sha256Digest;

pub static DEBUG_CERTIFICATE: &[u8] = include_bytes!("../../certs/debug.ca.pem");
pub static DEBUG_PRIVATE_KEY: &[u8] = include_bytes!("../../certs/debug.key.pem");

#[derive(Debug, Clone)]
pub struct CertFile {
    pub path: PathBuf,
    cert: Certificate,
    is_debug: bool,
}

fn is_debug_cert_path(path: impl AsRef<Path>) -> bool {
    path.as_ref().to_str().map(|x| x.trim().is_empty()).unwrap_or_default()
}

impl CertFile {
    pub fn try_new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        if is_debug_cert_path(&path) {
            return Self::debug();
        }

        let cert_der = CertificateDer::from_pem_file(&path).inspect_err(|err| {
            tracing::error!(
                "Failed to load TLS certificate from {}: {err}",
                path.as_ref().display()
            );
        })?;
        let cert = Certificate::from_der(cert_der.to_vec())?;
        let path = path.as_ref().to_path_buf();
        tracing::trace!("Loaded TLS certificate from {:?}", path);
        Ok(Self { path, cert, is_debug: false })
    }

    pub fn debug() -> anyhow::Result<Self> {
        let cert_der = CertificateDer::from_pem_slice(DEBUG_CERTIFICATE)?;
        let cert = Certificate::from_der(cert_der.to_vec())?;

        tracing::warn!("Loaded TLS debug cert");
        Ok(Self { path: PathBuf::default(), cert, is_debug: true })
    }

    pub fn resolve(
        &self,
        key: &PrivateKeyFile,
    ) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        Ok((vec![From::from(self.resolve_cert_ref(key).der().to_vec())], TryFrom::try_from(key)?))
    }

    pub(crate) fn resolve_host_id(&self, key: &PrivateKeyFile) -> String {
        hex::encode(self.resolve_cert_ref(key).hash().as_ref())
    }

    fn resolve_cert_ref<'a>(&'a self, key: &'a PrivateKeyFile) -> &'a Certificate {
        match &key.debug_cert {
            Some(debug_cert) if self.is_debug => &debug_cert.cert,
            _ => &self.cert,
        }
    }

    pub fn is_debug(&self) -> bool {
        self.is_debug
    }

    pub fn hash(&self) -> Sha256Digest {
        self.cert.hash()
    }

    pub fn try_load_certs(paths: &[PathBuf]) -> anyhow::Result<Vec<Self>> {
        let mut certs = Vec::new();
        for path in paths {
            if path.is_dir() {
                for entry in std::fs::read_dir(path)? {
                    let path = entry?.path();
                    if path
                        .file_name()
                        .and_then(|x| x.to_str())
                        .map(|x| x.ends_with(".ca.pem"))
                        .unwrap_or_default()
                    {
                        certs.push(Self::try_new(&path)?);
                    }
                }
            } else {
                certs.push(Self::try_new(path)?);
            }
        }
        Ok(certs)
    }
}

#[derive(Debug, Clone)]
pub struct CertStore {
    pub paths: Vec<PathBuf>,
    certs: Vec<Certificate>,
    is_debug: bool,
}

impl CertStore {
    pub fn try_new(paths: &[PathBuf]) -> anyhow::Result<Self> {
        if paths.is_empty() {
            return Self::debug();
        }
        let mut certs = Vec::new();
        for path in paths {
            if path.is_dir() {
                for entry in std::fs::read_dir(path)? {
                    let path = entry?.path();
                    if path
                        .file_name()
                        .and_then(|x| x.to_str())
                        .map(|x| x.ends_with(".ca.pem"))
                        .unwrap_or_default()
                    {
                        certs.push(CertFile::try_new(&path)?.cert);
                    }
                }
            } else {
                certs.push(CertFile::try_new(path)?.cert);
            }
        }
        Ok(Self { paths: paths.to_vec(), certs, is_debug: false })
    }

    pub fn debug() -> anyhow::Result<Self> {
        let cert_der = CertificateDer::from_pem_slice(DEBUG_CERTIFICATE)?;
        let cert = Certificate::from_der(cert_der.to_vec())?;
        tracing::warn!("Loaded TLS debug cert store");
        Ok(Self { paths: vec![], certs: vec![cert], is_debug: true })
    }

    pub fn is_debug(&self) -> bool {
        self.is_debug
    }

    pub fn build_root_cert_store(&self) -> anyhow::Result<rustls::RootCertStore> {
        let mut root_store = rustls::RootCertStore::empty();
        for cert in &self.certs {
            root_store.add(cert.der().into())?;
        }
        Ok(root_store)
    }
}

impl Serialize for CertFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.path.to_string_lossy().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CertFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;
        Self::try_new(&path).map_err(serde::de::Error::custom)
    }
}

impl Serialize for CertStore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.paths.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CertStore {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let paths = Vec::<PathBuf>::deserialize(deserializer)?;
        Self::try_new(&paths).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug)]
pub struct PrivateKeyFile {
    pub path: PathBuf,
    pub key: PrivateKey,
    debug_cert: Option<CertFile>,
}

impl PrivateKeyFile {
    pub fn try_new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        if is_debug_cert_path(&path) {
            return Self::debug();
        }

        let key_der = PrivateKeyDer::from_pem_file(&path)?;
        let key = PrivateKey::from_der_pkcs8(key_der.secret_der().to_vec());
        let path = path.as_ref().to_path_buf();

        Ok(Self { path, key, debug_cert: None })
    }

    pub fn debug() -> anyhow::Result<Self> {
        let cert_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
        let key = PrivateKey::from_der_pkcs8(cert_key.key_pair.serialize_der());
        let cert = CertFile {
            path: PathBuf::default(),
            cert: Certificate::from_der(cert_key.cert.der().to_vec())?,
            is_debug: true,
        };
        tracing::warn!("Loaded TLS debug private key");
        Ok(Self { path: PathBuf::default(), key, debug_cert: Some(cert) })
    }

    pub fn is_debug(&self) -> bool {
        self.debug_cert.is_some()
    }
}

impl Clone for PrivateKeyFile {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            key: self.key.clone_key(),
            debug_cert: self.debug_cert.clone(),
        }
    }
}

impl Serialize for PrivateKeyFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.path.to_string_lossy().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PrivateKeyFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;
        Self::try_new(&path).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<&PrivateKeyFile> for PrivateKeyDer<'static> {
    type Error = anyhow::Error;

    fn try_from(value: &PrivateKeyFile) -> Result<Self, Self::Error> {
        Self::try_from(value.key.secret_der().to_vec()).map_err(|err| anyhow::anyhow!("{err}"))
    }
}

impl TryFrom<PrivateKeyFile> for PrivateKeyDer<'static> {
    type Error = anyhow::Error;

    fn try_from(value: PrivateKeyFile) -> Result<Self, Self::Error> {
        Self::try_from(value.key.secret_der().to_vec()).map_err(|err| anyhow::anyhow!("{err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_certificate_wrapper() {
        let cert = CertFile::try_new("certs/debug.ca.pem").unwrap();
        assert_eq!(cert.path, Path::new("certs/debug.ca.pem"));
    }

    #[test]
    fn test_private_key_wrapper() {
        let key = PrivateKeyFile::try_new("certs/debug.key.pem").unwrap();
        assert_eq!(key.path, Path::new("certs/debug.key.pem"));
    }
}
