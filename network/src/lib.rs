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

pub mod channel;
pub mod cli;
pub mod config;
mod direct_sender;
pub use direct_sender::*;
pub mod message;
pub mod metrics;
pub mod network;
pub mod pub_sub;
pub mod resolver;
#[cfg(test)]
pub mod tests;
pub mod transfer;
pub mod unix_signals;

const ACKI_NACKI_DIRECT_PROTOCOL: &str = "acki-nacki-direct";
const ACKI_NACKI_SUBSCRIPTION_FROM_NODE_PROTOCOL: &str = "acki-nacki-subscription-from-node";
const ACKI_NACKI_SUBSCRIPTION_FROM_PROXY_PROTOCOL: &str = "acki-nacki-subscription-from-proxy";
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
    format!("{err:#?}").replace("\n", "").replace("\r", "")
}

pub fn try_parse_socket_addr(s: impl AsRef<str>, default_port: u16) -> anyhow::Result<SocketAddr> {
    let s = s.as_ref();
    let s = if s.contains(":") { s.to_string() } else { format!("{s}:{default_port}") };
    let addr = s
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("failed to parse socket address: {s}"))?;
    Ok(addr)
}

#[derive(Clone, Debug)]
pub struct PublisherConfig {
    pub addrs: Vec<SocketAddr>,
}
pub fn parse_publisher_addr(s: impl AsRef<str>) -> anyhow::Result<SocketAddr> {
    try_parse_socket_addr(s, DEFAULT_PUBLISHER_PORT)
}

pub fn deserialize_publisher_addr<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_publisher_addr(&s).map_err(serde::de::Error::custom)
}

pub fn deserialize_optional_publisher_addr<'de, D>(
    deserializer: D,
) -> Result<Option<SocketAddr>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(if let Some(s) = Option::<String>::deserialize(deserializer)? {
        Some(parse_publisher_addr(&s).map_err(serde::de::Error::custom)?)
    } else {
        None
    })
}

pub fn serialize_subscribe<S>(
    value: &Vec<Vec<SocketAddr>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut addrs = Vec::new();
    for publisher in value {
        for addr in publisher {
            addrs.push(*addr);
        }
    }
    addrs.serialize(serializer)
}

pub fn deserialize_subscribe<'de, D>(deserializer: D) -> Result<Vec<Vec<SocketAddr>>, D::Error>
where
    D: Deserializer<'de>,
{
    let addrs = Vec::<String>::deserialize(deserializer)?;
    let mut publishers = Vec::new();
    for addr in addrs {
        if !addr.is_empty() {
            publishers.push(vec![parse_publisher_addr(addr)
                .context("subscribe addrs")
                .map_err(serde::de::Error::custom)?]);
        }
    }
    Ok(publishers)
}

pub fn deserialize_publisher_addrs<'de, D>(deserializer: D) -> Result<Vec<SocketAddr>, D::Error>
where
    D: Deserializer<'de>,
{
    let addrs = Vec::<String>::deserialize(deserializer)?;
    let mut publishers = Vec::new();
    for addr in addrs {
        if !addr.is_empty() {
            publishers.push(
                parse_publisher_addr(addr)
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
mod unit_tests {
    use super::*;
    #[test]
    fn test_socket_addr() {
        try_parse_socket_addr("172.19.0.5:8500", 1).unwrap();
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
