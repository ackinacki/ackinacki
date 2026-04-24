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

use crate::decode_msg_id;
use crate::defaults::DEFAULT_BK_API_MESSAGES_PATH;
use crate::defaults::DEFAULT_BK_API_PROTO;
use crate::defaults::DEFAULT_BK_API_TIMEOUT;
use crate::defaults::DEFAULT_BM_API_MESSAGES_PATH;
use crate::defaults::DEFAULT_RETRY_INITIAL_DELAY_MS;
use crate::defaults::DEFAULT_RETRY_MAX_ATTEMPTS;
use crate::message_router::MessageRouter;

lazy_static::lazy_static!(
    static ref NODE_URL_PATH: String = std::env::var("NODE_URL_PATH")
        .unwrap_or(DEFAULT_BK_API_MESSAGES_PATH.into());

    static ref ROUTER_URL_PATH: String = std::env::var("ROUTER_URL_PATH")
        .unwrap_or(DEFAULT_BM_API_MESSAGES_PATH.into());
);

fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
}

fn is_retryable_request_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout()
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(DEFAULT_RETRY_INITIAL_DELAY_MS * 2u64.pow(attempt))
}

async fn send_with_retry(
    client: &reqwest::Client,
    url: &str,
    nrs: &[serde_json::Value],
) -> Result<reqwest::Response, reqwest::Error> {
    for attempt in 0..DEFAULT_RETRY_MAX_ATTEMPTS {
        if attempt > 0 {
            let delay = retry_delay(attempt - 1);
            tracing::warn!(
                target: "message_router",
                "Retrying request to {url} (attempt {}/{DEFAULT_RETRY_MAX_ATTEMPTS}) after {delay:?}",
                attempt + 1,
            );
            tokio::time::sleep(delay).await;
        }

        let request = client.post(url).header("X-EXT-MSG-SENT", now_ms().to_string()).json(nrs);

        let can_retry = attempt + 1 < DEFAULT_RETRY_MAX_ATTEMPTS;

        match request.send().await {
            Ok(response) if can_retry && is_retryable_status(response.status()) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                tracing::warn!(
                    target: "message_router",
                    "Retryable HTTP status {status} from {url} (attempt {}/{DEFAULT_RETRY_MAX_ATTEMPTS}): {body}",
                    attempt + 1,
                );
            }
            Ok(response) => return Ok(response),
            Err(err) if can_retry && is_retryable_request_error(&err) => {
                tracing::warn!(
                    target: "message_router",
                    "Retryable transport error for {url} (attempt {}/{DEFAULT_RETRY_MAX_ATTEMPTS}): {err}",
                    attempt + 1,
                );
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("retry loop must return on the last attempt")
}

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
        .filter_map(|nr| decode_msg_id(&nr["id"]).ok().map(|id| (nr["id"].clone(), id)))
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

        result = match send_with_retry(&client, &url, &nrs).await {
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
                        message: match &source {
                            Some(src) => format!(
                                "The message redirection to the Block Producer has failed: {err} (source: {src})"
                            ),
                            None => format!(
                                "The message redirection to the Block Producer has failed: {err}"
                            ),
                        },
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;

    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;

    async fn start_mock_server(responses: Vec<(u16, &str)>) -> (String, Arc<AtomicU32>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let responses: Vec<(u16, String)> =
            responses.into_iter().map(|(s, b)| (s, b.to_string())).collect();

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let idx = counter_clone.fetch_add(1, Ordering::SeqCst) as usize;
                let (status, body) = if idx < responses.len() {
                    responses[idx].clone()
                } else {
                    (200, "{}".to_string())
                };

                let mut buf = vec![0u8; 8192];
                let _ = stream.read(&mut buf).await;

                let status_text = match status {
                    200 => "OK",
                    400 => "Bad Request",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    502 => "Bad Gateway",
                    503 => "Service Unavailable",
                    504 => "Gateway Timeout",
                    _ => "Unknown",
                };

                let response = format!(
                    "HTTP/1.1 {status} {status_text}\r\n\
                     Content-Length: {}\r\n\
                     Content-Type: application/json\r\n\
                     Connection: close\r\n\r\n\
                     {body}",
                    body.len(),
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        (format!("http://127.0.0.1:{}", addr.port()), counter)
    }

    fn test_client() -> reqwest::Client {
        reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap()
    }

    fn test_nrs() -> Vec<serde_json::Value> {
        vec![serde_json::json!({"id": "test"})]
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_server_errors() {
        let (url, counter) = start_mock_server(vec![
            (503, "temporary error"),
            (502, "bad gateway"),
            (200, r#"{"ok": true}"#),
        ])
        .await;

        let response = send_with_retry(&test_client(), &url, &test_nrs()).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_on_429() {
        let (url, counter) =
            start_mock_server(vec![(429, "rate limited"), (200, r#"{"ok": true}"#)]).await;

        let response = send_with_retry(&test_client(), &url, &test_nrs()).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_no_retry_on_client_error() {
        let (url, counter) = start_mock_server(vec![(400, r#"{"error": "bad request"}"#)]).await;

        let response = send_with_retry(&test_client(), &url, &test_nrs()).await.unwrap();

        assert_eq!(response.status(), 400);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_exhausted_returns_last_response() {
        let (url, counter) =
            start_mock_server(vec![(503, "error 1"), (503, "error 2"), (503, "error 3")]).await;

        let response = send_with_retry(&test_client(), &url, &test_nrs()).await.unwrap();

        assert_eq!(response.status(), 503);
        assert_eq!(counter.load(Ordering::SeqCst), DEFAULT_RETRY_MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn test_no_retry_on_success() {
        let (url, counter) = start_mock_server(vec![(200, r#"{"ok": true}"#)]).await;

        let response = send_with_retry(&test_client(), &url, &test_nrs()).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_on_connection_error() {
        // Bind and immediately drop to get a port nobody listens on
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let url = format!("http://127.0.0.1:{port}");
        let start = std::time::Instant::now();
        let result = send_with_retry(&test_client(), &url, &test_nrs()).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(result.unwrap_err().is_connect());
        // Backoff delays: 200ms + 400ms = 600ms minimum across retries
        assert!(
            elapsed >= Duration::from_millis(500),
            "Expected retries with backoff, elapsed: {elapsed:?}"
        );
    }
}
