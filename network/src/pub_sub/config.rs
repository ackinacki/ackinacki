use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use ::serde::Deserialize;
use ::serde::Serialize;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use transport_layer::generate_self_signed_cert;
use transport_layer::CertHash;
use transport_layer::TlsCertCache;

pub static DEBUG_CERTIFICATE: &[u8] = include_bytes!("../../certs/debug.ca.pem");
pub static DEBUG_PRIVATE_KEY: &[u8] = include_bytes!("../../certs/debug.key.pem");

#[derive(Default, Debug, Clone)]
pub struct CertFile {
    pub path: PathBuf,
    cert: Option<CertificateDer<'static>>,
}

fn is_empty_path(path: impl AsRef<Path>) -> bool {
    path.as_ref().to_str().map(|x| x.trim().is_empty()).unwrap_or_default()
}

impl CertFile {
    pub fn try_new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        if is_empty_path(&path) {
            return Ok(Self { path: PathBuf::default(), cert: None });
        }

        let cert = CertificateDer::from_pem_file(&path).inspect_err(|err| {
            tracing::error!(
                "Failed to load TLS certificate from {}: {err}",
                path.as_ref().display()
            );
        })?;
        let path = path.as_ref().to_path_buf();
        tracing::trace!("Loaded TLS certificate from {:?}", path);
        Ok(Self { path, cert: Some(cert) })
    }

    pub fn resolve(
        &self,
        key: &PrivateKeyFile,
        ed_keys: &[transport_layer::SigningKey],
        tls_cert_cache: Option<TlsCertCache>,
    ) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        let (key, cert) = if let (Some(cert), Some(key)) = (&self.cert, &key.key) {
            tracing::info!("Loaded TLS certificate from {}", self.path.to_string_lossy());
            (key.clone_key(), cert.clone())
        } else if let Some(cert_cache) = tls_cert_cache {
            let (key, cert) = cert_cache.get_key_cert(None, key.key.as_ref(), ed_keys)?;
            tracing::info!(
                "Reused previously generated TLS certificate{}: {}, key: {}",
                if !ed_keys.is_empty() { " with ed signature(s)" } else { "" },
                CertHash::from(&cert),
                hex::encode(key.secret_der()),
            );
            (key, cert)
        } else {
            let (key, cert) = generate_self_signed_cert(None, ed_keys)?;
            tracing::info!(
                "Generated self signed TLS certificate{}: {}, key: {}",
                if !ed_keys.is_empty() { " with ed signature(s)" } else { "" },
                CertHash::from(&cert),
                hex::encode(key.secret_der()),
            );
            (key, cert)
        };
        Ok((vec![cert], key))
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

#[derive(Default, Debug, Clone)]
pub struct CertStore {
    pub paths: Vec<PathBuf>,
    pub certs: Vec<CertificateDer<'static>>,
}

impl CertStore {
    pub fn try_new(paths: &[PathBuf]) -> anyhow::Result<Self> {
        if paths.is_empty() {
            return Ok(Self::default());
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
                        if let Some(cert) = CertFile::try_new(&path)?.cert {
                            certs.push(cert);
                        }
                    }
                }
            } else if let Some(cert) = CertFile::try_new(path)?.cert {
                certs.push(cert);
            }
        }
        Ok(Self { paths: paths.to_vec(), certs })
    }

    pub(crate) fn cert_hashes(&self) -> HashSet<CertHash> {
        self.certs.iter().map(CertHash::from).collect()
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

#[derive(Default, Debug)]
pub struct PrivateKeyFile {
    pub path: PathBuf,
    pub key: Option<PrivateKeyDer<'static>>,
}

impl PrivateKeyFile {
    pub fn try_new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if is_empty_path(&path) {
            return Ok(Self { path, key: None });
        }

        let key = PrivateKeyDer::from_pem_file(&path)?;

        Ok(Self { path, key: Some(key) })
    }
}

impl Clone for PrivateKeyFile {
    fn clone(&self) -> Self {
        Self { path: self.path.clone(), key: self.key.as_ref().map(|x| x.clone_key()) }
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
