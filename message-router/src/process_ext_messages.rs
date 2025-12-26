// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use telemetry_utils::now_ms;
use transport_layer::HostPort;

use crate::base64_id_decode;
use crate::defaults::DEFAULT_BK_API_MESSAGES_PATH;
use crate::defaults::DEFAULT_BK_API_PROTO;
use crate::defaults::DEFAULT_BK_API_TIMEOUT;
use crate::defaults::DEFAULT_BM_API_MESSAGES_PATH;
use crate::message_router::MessageRouter;

lazy_static::lazy_static!(
    static ref NODE_URL_PATH: String = std::env::var("NODE_URL_PATH")
        .unwrap_or(DEFAULT_BK_API_MESSAGES_PATH.into());

    static ref ROUTER_URL_PATH: String = std::env::var("ROUTER_URL_PATH")
        .unwrap_or(DEFAULT_BM_API_MESSAGES_PATH.into());
);

pub async fn run(
    node_requests: serde_json::Value,
    message_router: Arc<MessageRouter>,
) -> anyhow::Result<serde_json::Value> {
    let Some(nrs) = node_requests.as_array() else {
        tracing::error!(target: "message_router", "bad request: {node_requests}");
        let error = serde_json::json!(http_server::ExtMsgResponse::new_with_error(
            "BAD_REQUEST".into(),
            "Incorrect request".into(),
            None,
        ));
        return Ok(error);
    };
    let thread_id = nrs
        .first()
        .and_then(|f| f.get("thread_id"))
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or(String::from(
            "00000000000000000000000000000000000000000000000000000000000000000000",
        ));

    let ids: HashMap<serde_json::Value, String> = nrs
        .iter()
        .filter_map(|nr| base64_id_decode(&nr["id"]).ok().map(|id| (nr["id"].clone(), id)))
        .collect();

    tracing::debug!(target: "message_router", "Ext messages received: {:?}", nrs.iter().map(|nr| format!("{}", nr["id"])));

    let recipients = message_router.bp_resolver.lock().resolve(Some(thread_id.clone()));
    tracing::trace!(target: "message_router", "Resolved BPs (thread={:?}): {:?}", thread_id, recipients);
    let mut result = serde_json::json!({});
    if recipients.is_empty() {
        let error = serde_json::json!(http_server::ExtMsgResponse::new_with_error(
            "INTERNAL_ERROR".into(),
            "Failed to obtain any Block Producer addresses".into(),
            None,
        ));
        return Ok(error);
    }

    let mut nrs = nrs.clone();
    let ext_message_token = json!(message_router.issue_token());
    tracing::trace!(target: "message_router", "add token to request: {:?}", ext_message_token);
    for nr in &mut nrs {
        nr["ext_message_token"] = ext_message_token.clone();
    }

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(DEFAULT_BK_API_TIMEOUT)).build()?;

    for recipient in recipients {
        let url = construct_url(&recipient);
        tracing::debug!(target: "message_router", "Forwarding requests to: {url}");

        let request = client.post(&url).header("X-EXT-MSG-SENT", now_ms().to_string()).json(&nrs);

        result = match request.send().await {
            Ok(response) => {
                let body = response.text().await;
                match body {
                    Ok(body_str) => {
                        tracing::debug!(target: "message_router", "response body (src={}): {:?}", recipient, body_str);
                        let mut response_json: serde_json::Value = serde_json::from_str(&body_str)?;

                        response_json["ext_message_token"] = json!(message_router.issue_token());
                        tracing::trace!(target: "message_router", "add token to response: {:?}", response_json["ext_message_token"]);
                        return Ok(response_json);
                    }
                    Err(err) => {
                        tracing::error!(target: "message_router", "redirection to {url} failed: {err}");
                        let err_data = http_server::ExtMsgErrorData::new(
                            vec![recipient.clone()],
                            ids.get(&nrs[0]["id"]).unwrap().to_string(),
                            None,
                            Some(thread_id.clone()),
                        );
                        serde_json::json!(http_server::ExtMsgResponse::new_with_error(
                            "INTERNAL_ERROR".into(),
                            format!("Failed to parse the response from the Block Producer: {err}"),
                            Some(err_data),
                        ))
                    }
                }
            }
            Err(err) => {
                tracing::error!(target: "message_router", "redirection to {url} failed: {err}");
                let err_data = http_server::ExtMsgErrorData::new(
                    vec![recipient.clone()],
                    ids.get(&nrs[0]["id"]).unwrap().to_string(),
                    None,
                    Some(thread_id.clone()),
                );
                serde_json::json!(http_server::ExtMsgResponse::new_with_error(
                    "INTERNAL_ERROR".into(),
                    format!("The message redirection to the Block Producer has failed: {err}"),
                    Some(err_data),
                ))
            }
        }
    }

    Ok(result)
}

fn construct_url(host_port: &HostPort) -> String {
    format!("{DEFAULT_BK_API_PROTO}://{}{}", host_port, *NODE_URL_PATH)
}
