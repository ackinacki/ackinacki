// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;

use anyhow::Context;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use url::Url;

pub mod channel;
pub mod cli;
pub mod config;
mod direct_sender;
pub mod message;
pub mod metrics;
pub mod network;
pub mod pub_sub;
pub mod resolver;
pub mod socket_addr;
mod tls;
pub mod transfer;
pub mod unix_signals;
pub use tls::TlsConfig;

const DEFAULT_PUBLISHER_PORT: u16 = 8500;

#[derive(Copy, Clone)]
pub enum DeliveryPhase {
    OutgoingBuffer,
    OutgoingTransfer,
    IncomingTransfer,
    IncomingBuffer,
}

pub enum SendMode {
    Broadcast,
    Direct,
}

impl SendMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            SendMode::Broadcast => "broadcast",
            SendMode::Direct => "direct",
        }
    }

    pub fn is_broadcast(&self) -> bool {
        match self {
            SendMode::Broadcast => true,
            SendMode::Direct => false,
        }
    }
}

impl Display for SendMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn host_id_prefix(s: &str) -> &str {
    s.split_at_checked(6).map(|(first, _)| first).unwrap_or(s)
}

pub(crate) fn detailed(err: &impl Debug) -> String {
    format!("{:#?}", err).replace("\n", "").replace("\r", "")
}

pub fn try_socket_addr_from_url(url: &Url) -> Option<SocketAddr> {
    let Some(host) = url.host_str() else {
        tracing::error!(url = url.to_string(), "URL must have a host");
        return None;
    };
    let host_port = format!("{}:{}", host, url.port().unwrap_or(DEFAULT_PUBLISHER_PORT));
    let mut addrs_iter = match host_port.to_socket_addrs() {
        Ok(iter) => iter,
        Err(err) => {
            tracing::error!(url = url.to_string(), "Failed to resolve host: {err}");
            return None;
        }
    };
    match addrs_iter.next() {
        Some(addr) => Some(addr),
        None => {
            tracing::error!(url = url.to_string(), "Failed to resolve host: no addresses found");
            None
        }
    }
}

pub fn try_url_from_socket_addr(addr: &SocketAddr) -> Option<Url> {
    format!("https://{}", addr)
        .parse::<Url>()
        .inspect_err(|err| {
            tracing::error!(url = addr.to_string(), "Failed to parse URL: {err}");
        })
        .ok()
}

pub fn try_parse_url(
    s: impl AsRef<str>,
    default_protocol: &str,
    default_port: u16,
) -> anyhow::Result<Url> {
    let s = if !s.as_ref().contains("://") {
        format!("{}://{}", default_protocol, s.as_ref())
    } else {
        s.as_ref().to_string()
    };
    let mut url = match Url::parse(&s) {
        Ok(parsed) => parsed,
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            // Handle the case when the URL is missing a scheme (e.g., "example.com")
            Url::parse(&format!("{default_protocol}://{s}"))?
        }
        Err(e) => return Err(anyhow::anyhow!("Failed to parse URL: {}", e)),
    };

    // Ensure the URL has a host
    if url.host().is_none() {
        return Err(anyhow::anyhow!("URL must have a host: '{}'", url.as_ref()));
    }

    // Set the default port if not already specified
    if url.port().is_none() {
        url.set_port(Some(default_port))
            .map_err(|_| anyhow::anyhow!("Invalid port: {}", default_port))?;
    }

    Ok(url)
}

#[derive(Clone, Debug)]
pub struct PublisherConfig {
    pub urls: Vec<Url>,
}
pub fn publisher_url_from_socket_addr(value: SocketAddr) -> Url {
    parse_publisher_url(value.to_string().as_str()).unwrap()
}

pub fn parse_publisher_url(s: impl AsRef<str>) -> anyhow::Result<Url> {
    try_parse_url(s, "https", DEFAULT_PUBLISHER_PORT)
}

pub fn deserialize_publisher_url<'de, D>(deserializer: D) -> Result<Url, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_publisher_url(&s).map_err(serde::de::Error::custom)
}

pub fn deserialize_optional_publisher_url<'de, D>(deserializer: D) -> Result<Option<Url>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(if let Some(s) = Option::<String>::deserialize(deserializer)? {
        Some(parse_publisher_url(&s).map_err(serde::de::Error::custom)?)
    } else {
        None
    })
}

pub fn serialize_subscribe<S>(value: &Vec<Vec<Url>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut urls = Vec::new();
    for publisher in value {
        for url in publisher {
            urls.push(url.clone());
        }
    }
    urls.serialize(serializer)
}

pub fn deserialize_subscribe<'de, D>(deserializer: D) -> Result<Vec<Vec<Url>>, D::Error>
where
    D: Deserializer<'de>,
{
    let urls = Vec::<String>::deserialize(deserializer)?;
    let mut publishers = Vec::new();
    for url in urls {
        if !url.is_empty() {
            publishers.push(vec![parse_publisher_url(url)
                .context("subscribe urls")
                .map_err(serde::de::Error::custom)?]);
        }
    }
    Ok(publishers)
}

pub fn deserialize_publisher_urls<'de, D>(deserializer: D) -> Result<Vec<Url>, D::Error>
where
    D: Deserializer<'de>,
{
    let urls = Vec::<String>::deserialize(deserializer)?;
    let mut publishers = Vec::new();
    for url in urls {
        if !url.is_empty() {
            publishers.push(
                parse_publisher_url(url)
                    .context("publisher urls")
                    .map_err(serde::de::Error::custom)?,
            );
        }
    }
    Ok(publishers)
}

pub fn extract_msg_type(s: impl AsRef<str>) -> String {
    s.as_ref()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .split_once("::")
        .map(|x| x.1)
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_socket_addr() {
        try_socket_addr_from_url(&Url::parse("https://172.19.0.5:8500/").unwrap()).unwrap();
    }

    #[test]
    fn test_extract_msg_type() {
        let input = "MsgType::MyMessage (1, 22222)";
        let result = extract_msg_type(input);
        assert_eq!(result, "MyMessage");

        let input = "MsgTypeMyMessage (1, 22222)";
        let result = extract_msg_type(input);
        assert_eq!(result, "");
    }
}
