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

pub fn base64_id_decode(base64_str: &serde_json::Value) -> Result<String, String> {
    let base64_str = base64_str.as_str().ok_or("Invalid JSON format")?;
    let base64_decoded = tvm_types::base64_decode(base64_str).map_err(|err| err.to_string())?;
    Ok(hex::encode(&base64_decoded))
}

pub fn read_keys_from_file(path: &str) -> Result<KeyPair, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let keys: KeyPair = serde_json::from_reader(reader)?;
    Ok(keys)
}
