// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use ed25519_dalek::Signer;
use hex::FromHex;
use serde::Serialize;

use crate::now_plus_n_secs;
use crate::KeyPair;

lazy_static::lazy_static!(
    static ref TOKEN_TTL: u64 = std::env::var("TOKEN_TTL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300);
);

#[derive(Clone, Serialize, Debug)]
pub struct Token {
    pub unsigned: String, // UNIX timestamp in ms
    pub signature: String,
    pub verifying_key: String,
}

impl Token {
    pub fn new_token(keys: &KeyPair) -> anyhow::Result<Self> {
        let unsigned = now_plus_n_secs(*TOKEN_TTL);
        let message_bytes = unsigned.as_bytes();

        let secret_bytes = <[u8; 32]>::from_hex(&keys.secret)?;
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);

        let signature: ed25519_dalek::Signature = signing_key.sign(message_bytes);

        Ok(Self {
            unsigned,
            signature: hex::encode(signature.to_bytes()),
            verifying_key: keys.public.clone(),
        })
    }
}
