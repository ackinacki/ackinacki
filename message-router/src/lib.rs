// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub mod bp_resolver;
mod defaults;
pub mod message_router;
pub mod process_ext_messages;

pub use bp_resolver::MockBPResolver;
pub use defaults::DEFAULT_BK_API_MESSAGES_PATH;
pub use defaults::DEFAULT_BM_API_MESSAGES_PATH;
use serde::Deserialize;

// todo prevent printing the secret key into the log
#[derive(Clone, Deserialize)]
pub struct KeyPair {
    pub public: String,
    pub secret: String,
}

/// TODO: unify encoding in tvm-client so that both (send_message/send_messages) paths use the same format.
pub fn decode_msg_id(id_value: &serde_json::Value) -> Result<String, String> {
    let id_str = id_value.as_str().ok_or("Invalid JSON format")?;

    // Primary: 64 hex chars = 32-byte hash (UInt256)
    if id_str.len() == 64 && id_str.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(id_str.to_lowercase());
    }

    // Fallback: base64-encoded hash bytes (from send_messages)
    let decoded = tvm_types::base64_decode(id_str).map_err(|e| e.to_string())?;
    Ok(hex::encode(&decoded))
}

pub fn read_keys_from_file(path: &str) -> Result<KeyPair, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let keys: KeyPair = serde_json::from_reader(reader)?;
    Ok(keys)
}
