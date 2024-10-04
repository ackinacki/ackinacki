// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::ops::Deref;

use serde::Deserialize;
use serde::Serialize;

/// Gets or converts given structure to at least one [`SocketAddr`]
pub trait ToOneSocketAddr {
    /// Attempts to convert the implementing type into a [`SocketAddr`].
    ///
    /// Returns a `Result` indicating success or failure. If successful, it
    /// returns the converted `SocketAddr`. If unsuccessful, it returns an
    /// `Err` containing an `anyhow::Error` explaining the reason for the
    /// failure.
    ///
    /// # Examples
    ///
    /// ```
    /// use network::socket_addr::ToOneSocketAddr;
    ///
    /// let addr_str = "127.0.0.1:8080";
    ///
    /// match addr_str.try_to_socket_addr() {
    ///     Ok(socket_addr) => println!("Converted to SocketAddr: {:?}", socket_addr),
    ///     Err(err) => eprintln!("Error converting to SocketAddr: {}", err),
    /// }
    /// ```
    fn try_to_socket_addr(&self) -> anyhow::Result<SocketAddr>;

    /// Converts the implementing type into a [`SocketAddr`].
    ///
    /// Returns the converted `SocketAddr` if successful. In case of failure, it
    /// panics with a descriptive error message.
    ///
    /// # Panics
    ///
    /// Panics with the message "socket address parsed" if the conversion fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use network::socket_addr::ToOneSocketAddr;
    ///
    /// let addr_str = "127.0.0.1:8080";
    ///
    /// let socket_addr = addr_str.to_socket_addr();
    /// println!("Converted to SocketAddr: {:?}", socket_addr);
    /// ```
    fn to_socket_addr(&self) -> SocketAddr;
}

impl ToOneSocketAddr for str {
    /// Attempts to convert the string slice into a [`SocketAddr`].
    ///
    /// Returns a `Result` indicating success or failure. If successful, it
    /// returns the converted `SocketAddr`. If unsuccessful, it returns an
    /// `Err` containing an `anyhow::Error` explaining the reason for the
    /// failure.
    ///
    /// # Examples
    ///
    /// ```
    /// use network::socket_addr::ToOneSocketAddr;
    ///
    /// let addr_str = "127.0.0.1:8080";
    ///
    /// match addr_str.try_to_socket_addr() {
    ///     Ok(socket_addr) => println!("Converted to SocketAddr: {:?}", socket_addr),
    ///     Err(err) => eprintln!("Error converting to SocketAddr: {}", err),
    /// }
    /// ```
    fn try_to_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        self.to_socket_addrs()?.next().ok_or_else(|| {
            anyhow::anyhow!("Socket addr has been parsed, but iterator of results is empty")
        })
    }

    /// Converts the string slice into a [`SocketAddr`].
    ///
    /// Returns the converted `SocketAddr` if successful. In case of failure, it
    /// panics with a descriptive error message.
    ///
    /// # Panics
    ///
    /// Panics with the message "socket address parsed" if the conversion fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use network::socket_addr::ToOneSocketAddr;
    ///
    /// let addr_str = "127.0.0.1:8080";
    ///
    /// let socket_addr = addr_str.to_socket_addr();
    /// println!("Converted to SocketAddr: {:?}", socket_addr);
    /// ```
    fn to_socket_addr(&self) -> SocketAddr {
        tracing::trace!("to_socket_addr({self})");
        self.try_to_socket_addr().expect("socket address parsed")
    }
}

impl ToOneSocketAddr for String {
    /// Attempts to convert the `String` into a [`SocketAddr`].
    ///
    /// Returns a `Result` indicating success or failure. If successful, it
    /// returns the converted `SocketAddr`. If unsuccessful, it returns an
    /// `Err` containing an `anyhow::Error` explaining the reason for the
    /// failure.
    ///
    /// # Examples
    ///
    /// ```
    /// use network::socket_addr::ToOneSocketAddr;
    ///
    /// let addr_str = String::from("127.0.0.1:8080");
    ///
    /// match addr_str.try_to_socket_addr() {
    ///     Ok(socket_addr) => println!("Converted to SocketAddr: {:?}", socket_addr),
    ///     Err(err) => eprintln!("Error converting to SocketAddr: {}", err),
    /// }
    /// ```
    fn try_to_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        self.as_str().try_to_socket_addr()
    }

    /// Converts the `String` into a [`SocketAddr`].
    ///
    /// Returns the converted `SocketAddr` if successful. In case of failure, it
    /// panics with a descriptive error message.
    ///
    /// # Panics
    ///
    /// Panics with the message "socket address parsed" if the conversion fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use network::socket_addr::ToOneSocketAddr;
    ///
    /// let addr_str = String::from("127.0.0.1:8080");
    ///
    /// let socket_addr = addr_str.to_socket_addr();
    /// println!("Converted to SocketAddr: {:?}", socket_addr);
    /// ```
    fn to_socket_addr(&self) -> SocketAddr {
        self.as_str().to_socket_addr()
    }
}

/// A wrapper around a string slice that implements [`ToOneSocketAddr`].
///
/// It can be IPv4:port, IPv6:port, or hostname:port.
///
/// In case of hostname, it will be resolved to a single IPv4 or IPv6 address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StringSocketAddr(String);

impl Deref for StringSocketAddr {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for StringSocketAddr {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<StringSocketAddr> for String {
    fn from(value: StringSocketAddr) -> Self {
        value.0
    }
}

impl Display for StringSocketAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fmt::Display::fmt(self.deref(), f)
    }
}
