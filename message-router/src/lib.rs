// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub mod bp_resolver;
mod defaults;
pub mod message_router;

pub use defaults::DEFAULT_NODE_URL_PORT;

pub fn base64_id_decode(base64_str: &serde_json::Value) -> Result<String, String> {
    let base64_str = base64_str.as_str().ok_or("Invalid JSON format")?;
    let base64_decoded = tvm_types::base64_decode(base64_str).map_err(|err| err.to_string())?;
    Ok(hex::encode(&base64_decoded))
}
