// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::error::Error as _;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use telemetry_utils::now_ms;
use transport_layer::HostPort;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ExtMsgRunResponse {
    pub result: Option<ExtMsgRunResult>,
    pub error: Option<ExtMsgRunError>,
    pub ext_message_token: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ExtMsgRunResult {
    pub message_hash: String,
    pub block_hash: String,
    pub tx_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_timestamp: Option<u32>,
    pub ext_out_msgs: Vec<String>,
    pub aborted: bool,
    pub exit_code: i32,
    pub producers: Vec<HostPort>,
    pub current_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ExtMsgRunError {
    pub code: String,
    pub message: String,
    pub data: Option<ExtMsgRunErrorData>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ExtMsgRunErrorData {
    pub producers: Vec<HostPort>,
    pub message_hash: String,
    pub exit_code: Option<i32>,
    pub current_time: String,
    pub thread_id: Option<String>,
}

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
) -> anyhow::Result<ExtMsgRunResponse> {
    let Some(nrs) = node_requests.as_array() else {
        tracing::error!(target: "message_router", "bad request: {node_requests}");
        let error = ExtMsgRunResponse {
            result: None,
            error: Some(ExtMsgRunError {
                code: "BAD_REQUEST".to_string(),
                message: "Incorrect request".to_string(),
                data: None,
            }),
            ext_message_token: None,
        };
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
    let mut result = ExtMsgRunResponse::default();
    if recipients.is_empty() {
        let error = ExtMsgRunResponse {
            result: None,
            error: Some(ExtMsgRunError {
                code: "INTERNAL_ERROR".to_string(),
                message: "Failed to obtain any Block Producer addresses".to_string(),
                data: None,
            }),
            ext_message_token: None,
        };
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
                let status = response.status();
                let body = response.text().await;
                match body {
                    Ok(body_str) => {
                        if !status.is_success() {
                            tracing::error!(
                                target: "message_router",
                                "redirection to {url} failed: http_status={} response_body={:?}",
                                status,
                                body_str
                            );
                            let err_data = ExtMsgRunErrorData {
                                producers: vec![recipient.clone()],
                                message_hash: ids.get(&nrs[0]["id"]).unwrap().to_string(),
                                exit_code: None,
                                current_time: now_ms().to_string(),
                                thread_id: Some(thread_id.clone()),
                            };
                            ExtMsgRunResponse {
                                result: None,
                                error: Some(ExtMsgRunError {
                                    code: "INTERNAL_ERROR".to_string(),
                                    message: format!(
                                        "Block Producer returned non-success HTTP status {}: {}",
                                        status, body_str
                                    ),
                                    data: Some(err_data),
                                }),
                                ext_message_token: None,
                            }
                        } else {
                            tracing::debug!(target: "message_router", "response body (src={}): {:?}", recipient, body_str);
                            let mut response_struct: ExtMsgRunResponse =
                                serde_json::from_str(&body_str)?;

                            response_struct.ext_message_token =
                                Some(json!(message_router.issue_token()));
                            tracing::trace!(target: "message_router", "add token to response: {:?}", response_struct.ext_message_token);
                            return Ok(response_struct);
                        }
                    }
                    Err(err) => {
                        tracing::error!(target: "message_router", "redirection to {url} failed: failed to parse the response from the BP: {err}");
                        let err_data = ExtMsgRunErrorData {
                            producers: vec![recipient.clone()],
                            message_hash: ids.get(&nrs[0]["id"]).unwrap().to_string(),
                            exit_code: None,
                            current_time: now_ms().to_string(),
                            thread_id: Some(thread_id.clone()),
                        };
                        ExtMsgRunResponse {
                            result: None,
                            error: Some(ExtMsgRunError {
                                code: "INTERNAL_ERROR".to_string(),
                                message: format!(
                                    "Failed to parse the response from the Block Producer: {err}"
                                ),
                                data: Some(err_data),
                            }),
                            ext_message_token: None,
                        }
                    }
                }
            }
            Err(err) => {
                let source = err.source().map(ToString::to_string);
                tracing::error!(
                    target: "message_router",
                    "redirection to {url} failed: err={} status={:?} timeout={} connect={} request={} body={} url={:?} source={:?}",
                    err,
                    err.status(),
                    err.is_timeout(),
                    err.is_connect(),
                    err.is_request(),
                    err.is_body(),
                    err.url(),
                    source
                );
                let err_data = ExtMsgRunErrorData {
                    producers: vec![recipient.clone()],
                    message_hash: ids.get(&nrs[0]["id"]).unwrap().to_string(),
                    exit_code: None,
                    current_time: now_ms().to_string(),
                    thread_id: Some(thread_id.clone()),
                };
                ExtMsgRunResponse {
                    result: None,
                    error: Some(ExtMsgRunError {
                        code: "INTERNAL_ERROR".to_string(),
                        message: format!(
                            "The message redirection to the Block Producer has failed: {err}"
                        ),
                        data: Some(err_data),
                    }),
                    ext_message_token: None,
                }
            }
        }
    }

    Ok(result)
}

fn construct_url(host_port: &HostPort) -> String {
    format!("{DEFAULT_BK_API_PROTO}://{}{}", host_port, *NODE_URL_PATH)
}
