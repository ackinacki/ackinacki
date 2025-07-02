#![warn(unreachable_pub)]
#![warn(clippy::use_self)]

pub mod buffer;
pub mod connection;
pub mod listener;
pub mod send_stream;
pub mod stream;

pub(super) use connection::Connection;
pub(super) use listener::Listener;
