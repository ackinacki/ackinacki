use std::fmt::Display;
use std::net::SocketAddr;
use std::str::FromStr;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPort {
    host: String,
    port: Option<u16>,
}

impl HostPort {
    pub fn new(host: String, port: Option<u16>) -> Self {
        Self { host, port }
    }

    pub fn with_default_port(mut self, default_port: u16) -> Self {
        if self.port.is_none() {
            self.port = Some(default_port);
        }
        self
    }
}

impl From<SocketAddr> for HostPort {
    fn from(addr: SocketAddr) -> Self {
        Self { host: addr.ip().to_string(), port: Some(addr.port()) }
    }
}
impl Serialize for HostPort {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HostPort {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Display for HostPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.port {
            Some(port) => write!(f, "{}:{}", self.host, port),
            None => write!(f, "{}", self.host),
        }
    }
}

impl FromStr for HostPort {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (host, port) = if let Some((host, port)) = s.split_once(':') {
            (
                host,
                Some(port.parse().map_err(|err| anyhow::anyhow!("Invalid port [{port}]: {err}"))?),
            )
        } else {
            (s, None)
        };
        let host = host.trim().to_string();
        if host.is_empty() {
            anyhow::bail!("Hostname must not be empty");
        }
        if host.contains("://") {
            anyhow::bail!("Hostname must not contain protocol");
        }
        Ok(Self { host, port })
    }
}
