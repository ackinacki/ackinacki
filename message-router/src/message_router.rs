// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use ext_messages_auth::auth::Token;
use parking_lot::Mutex;

use crate::bp_resolver::BPResolver;
use crate::defaults::DEFAULT_BK_API_MESSAGES_PATH;
use crate::defaults::DEFAULT_BM_API_MESSAGES_PATH;
use crate::KeyPair;

lazy_static::lazy_static!(
    static ref NODE_URL_PATH: String = std::env::var("NODE_URL_PATH")
        .unwrap_or(DEFAULT_BK_API_MESSAGES_PATH.into());

    static ref ROUTER_URL_PATH: String = std::env::var("ROUTER_URL_PATH")
        .unwrap_or(DEFAULT_BM_API_MESSAGES_PATH.into());
);

pub struct MessageRouterConfig {
    pub bp_resolver: Arc<Mutex<dyn BPResolver>>,
    pub owner_wallet_pubkey: Option<String>,
    pub signing_keys: Option<KeyPair>,
}

#[derive(Clone)]
pub struct MessageRouter {
    pub bind: SocketAddr,
    pub owner_wallet_pubkey: Option<String>,
    pub signing_keys: Option<KeyPair>,
    pub bp_resolver: Arc<Mutex<dyn BPResolver>>,
}

impl Display for MessageRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageRouter bind={}", self.bind)
    }
}

impl MessageRouter {
    pub fn new(bind: String, config: MessageRouterConfig) -> Self {
        let bind = bind.to_socket_addrs().expect("msg").next().unwrap();
        let owner_wallet_pubkey = config.owner_wallet_pubkey;
        let signing_keys = config.signing_keys;
        let bp_resolver = config.bp_resolver;
        let message_router = Self { bind, owner_wallet_pubkey, signing_keys, bp_resolver };

        tracing::info!("Starting MessageRouter: {message_router}");

        message_router
    }

    pub fn dump(&self) {
        tracing::debug!("msg_router: {}", self);
    }
}

impl MessageRouter {
    pub fn issue_token(&self) -> Option<Token> {
        if let Some(keys) = &self.signing_keys {
            if let Some(issuer_pubkey) = &self.owner_wallet_pubkey {
                Token::new(
                    &keys.secret,
                    ext_messages_auth::auth::TokenIssuer::Bm(issuer_pubkey.to_string()),
                )
                .ok()
            } else {
                None
            }
        } else {
            None
        }
    }
}
