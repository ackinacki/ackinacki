// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tvm_block::Deserializable;
use tvm_block::Message;
use tvm_types::base64_decode;

pub fn parse_message(id: &str, message_b64: &str) -> Result<Message, String> {
    tracing::trace!(target: "http_server", "parse_message {id}");
    let message_bytes = base64_decode(message_b64)
        .map_err(|e| format!("Error decoding base64-encoded message: {e}"))?;

    let message_cell = tvm_types::boc::read_single_root_boc(message_bytes).map_err(|e| {
        tracing::error!(target: "http_server", "Error deserializing message: {}", e);
        format!("Error deserializing message: {e}")
    })?;

    Message::construct_from_cell(message_cell)
        .map_err(|e| format!("Error parsing message's cells tree: {e}"))
}

pub fn extract_ext_msg_sent_time(headers: &salvo::http::HeaderMap) -> Option<u64> {
    headers.get("X-EXT-MSG-SENT")?.to_str().ok()?.parse().ok()
}
