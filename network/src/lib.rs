use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub mod cli;
pub mod config;
mod direct_sender;
mod gossip;
pub mod network;
pub mod pub_sub;
pub mod socket_addr;
mod tls;
pub mod unix_signals;

pub trait NetworkPeerId:
    FromStr + Display + Clone + Eq + Hash + Debug + Send + Sync + 'static
where
    <Self as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<Self as FromStr>::Err>,
{
}

impl<T> NetworkPeerId for T
where
    T: FromStr + Display + Clone + Eq + Hash + Debug + Send + Sync + 'static,
    <T as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<Self as FromStr>::Err>,
{
}

pub trait NetworkMessage:
    serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone
{
}

impl<T> NetworkMessage for T where
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone
{
}
pub(crate) fn detailed(err: &impl Debug) -> String {
    format!("{:#?}", err).replace("\n", "").replace("\r", "")
}
