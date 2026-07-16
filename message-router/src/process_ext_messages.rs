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
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub dapp_id: String,
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
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub dapp_id: String,
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
    // Validate the new wire fields before any network work. This mirrors the BK
    // middleware so that an invalid client request is rejected without a round
    // trip to a Block Producer.
    let (validated_account_id, validated_dapp_id) = match nrs.first() {
        Some(first) => {
            let raw_account = first.get("account_id").and_then(|v| v.as_str()).unwrap_or("");
            let raw_dapp = first.get("dapp_id").and_then(|v| v.as_str()).unwrap_or("");
            let account_id = match crate::validation::parse_hex32(raw_account, "account_id") {
                Ok(v) => v,
                Err(msg) => {
                    return Ok(ExtMsgRunResponse {
                        result: None,
                        error: Some(ExtMsgRunError {
                            code: "BAD_REQUEST".to_string(),
                            message: msg,
                            data: None,
                        }),
                        ext_message_token: None,
                    });
                }
            };
            let dapp_id = match crate::validation::parse_hex32(raw_dapp, "dapp_id") {
                Ok(v) => v,
                Err(msg) => {
                    return Ok(ExtMsgRunResponse {
                        result: None,
                        error: Some(ExtMsgRunError {
                            code: "BAD_REQUEST".to_string(),
                            message: msg,
                            data: None,
                        }),
                        ext_message_token: None,
                    });
                }
            };
            (account_id, dapp_id)
        }
        None => {
            return Ok(ExtMsgRunResponse {
                result: None,
                error: Some(ExtMsgRunError {
                    code: "BAD_REQUEST".to_string(),
                    message: "Empty request".to_string(),
                    data: None,
                }),
                ext_message_token: None,
            });
        }
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
        .filter_map(|nr| match decode_msg_id(&nr["id"]) {
            Ok(id) => Some((nr["id"].clone(), id)),
            Err(err) => {
                // The id is dropped from the map here; log the raw value so an
                // operator can identify the offending request without DEBUG logs.
                tracing::warn!(
                    target: "message_router",
                    "skipping ext message with undecodable id={}: {err}",
                    nr["id"]
                );
                None
            }
        })
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

    // Message hash of the first request, used to annotate error responses. `ids`
    // only holds entries whose id decoded successfully, so fall back to an empty
    // hash instead of panicking when the client sent an undecodable id.
    let first_message_hash =
        nrs.first().and_then(|nr| ids.get(&nr["id"])).cloned().unwrap_or_default();

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
                                message_hash: first_message_hash.clone(),
                                exit_code: None,
                                current_time: now_ms().to_string(),
                                thread_id: Some(thread_id.clone()),
                                account_id: validated_account_id.clone(),
                                dapp_id: validated_dapp_id.clone(),
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

                            if let Some(result) = response_struct.result.as_mut() {
                                result.account_id = validated_account_id.clone();
                                result.dapp_id = validated_dapp_id.clone();
                            }
                            if let Some(data) =
                                response_struct.error.as_mut().and_then(|error| error.data.as_mut())
                            {
                                data.account_id = validated_account_id.clone();
                                data.dapp_id = validated_dapp_id.clone();
                            }
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
                            message_hash: first_message_hash.clone(),
                            exit_code: None,
                            current_time: now_ms().to_string(),
                            thread_id: Some(thread_id.clone()),
                            account_id: validated_account_id.clone(),
                            dapp_id: validated_dapp_id.clone(),
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
                    message_hash: first_message_hash.clone(),
                    exit_code: None,
                    current_time: now_ms().to_string(),
                    thread_id: Some(thread_id.clone()),
                    account_id: validated_account_id.clone(),
                    dapp_id: validated_dapp_id.clone(),
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

    async fn start_mock_bp_server(status: u16, body: &str) -> HostPort {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = HostPort::from(listener.local_addr().unwrap());
        let body = body.to_string();

        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0u8; 8192];
            let _ = stream.read(&mut buf).await;
            let status_text = match status {
                200 => "OK",
                400 => "Bad Request",
                500 => "Internal Server Error",
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
        });

        addr
    }

    fn test_client() -> reqwest::Client {
        reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap()
    }

    fn test_nrs() -> Vec<serde_json::Value> {
        vec![serde_json::json!({"id": "test"})]
    }

    fn nrs_with_fields(account_id: &str, dapp_id: &str) -> serde_json::Value {
        serde_json::json!([
            {
                "id": "aGVsbG8=",
                "body": "AAAA",
                "thread_id": null,
                "account_id": account_id,
                "dapp_id": dapp_id,
            }
        ])
    }

    fn make_router() -> Arc<crate::message_router::MessageRouter> {
        use std::net::IpAddr;
        use std::net::Ipv4Addr;
        use std::net::SocketAddr;

        use parking_lot::Mutex;

        let mut mock = crate::MockBPResolver::new();
        // The validator should reject before we ever consult the resolver. Mark it as
        // never-called so the test fails loudly if validation regresses.
        mock.expect_resolve().never();
        let resolver = Arc::new(Mutex::new(mock));
        Arc::new(crate::message_router::MessageRouter {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            owner_wallet_pubkey: None,
            signing_keys: None,
            bp_resolver: resolver,
        })
    }

    fn make_router_with_recipient(
        recipient: HostPort,
    ) -> Arc<crate::message_router::MessageRouter> {
        use std::net::IpAddr;
        use std::net::Ipv4Addr;
        use std::net::SocketAddr;

        use parking_lot::Mutex;

        let mut mock = crate::MockBPResolver::new();
        mock.expect_resolve().return_const(vec![recipient]);
        let resolver = Arc::new(Mutex::new(mock));
        Arc::new(crate::message_router::MessageRouter {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            owner_wallet_pubkey: None,
            signing_keys: None,
            bp_resolver: resolver,
        })
    }

    const VALID_HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// A `tracing` writer that appends every log line into a shared buffer so a
    /// test can assert on emitted records.
    #[derive(Clone)]
    struct SharedBuf(Arc<parking_lot::Mutex<Vec<u8>>>);

    impl std::io::Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl tracing_subscriber::fmt::MakeWriter<'_> for SharedBuf {
        type Writer = SharedBuf;

        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }

    #[tokio::test]
    async fn run_rejects_missing_account_id_before_proxying() {
        let nrs = serde_json::json!([{"id":"aGVsbG8=", "body":"AAAA", "dapp_id": VALID_HEX}]);
        let resp = run(nrs, make_router()).await.unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, "BAD_REQUEST");
        assert!(err.message.contains("account_id"));
    }

    #[tokio::test]
    async fn run_rejects_invalid_dapp_id_format() {
        let nrs = nrs_with_fields(VALID_HEX, "not-a-hex");
        let resp = run(nrs, make_router()).await.unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, "BAD_REQUEST");
        assert!(err.message.contains("dapp_id"));
    }

    #[tokio::test]
    async fn run_rejects_empty_request() {
        let resp = run(serde_json::json!([]), make_router()).await.unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, "BAD_REQUEST");
        assert_eq!(err.message, "Empty request");
    }

    #[tokio::test]
    async fn run_injects_validated_ids_into_success_response() {
        let recipient = start_mock_bp_server(
            200,
            r#"{
                "result": {
                    "message_hash": "hash",
                    "block_hash": "block",
                    "tx_hash": "tx",
                    "ext_out_msgs": [],
                    "aborted": false,
                    "exit_code": 0,
                    "producers": [],
                    "current_time": "1"
                },
                "error": null
            }"#,
        )
        .await;
        let resp =
            run(nrs_with_fields(VALID_HEX, VALID_HEX), make_router_with_recipient(recipient))
                .await
                .unwrap();

        let result = resp.result.expect("success result expected");
        assert_eq!(result.account_id, VALID_HEX);
        assert_eq!(result.dapp_id, VALID_HEX);
    }

    #[tokio::test]
    async fn run_does_not_panic_when_first_id_undecodable_and_bp_fails() {
        // Regression: a first message whose `id` cannot be decoded is dropped from
        // the `ids` map. When the BP round-trip then fails, the error path used to
        // `ids.get(..).unwrap()` and panic, crashing the whole process. It must
        // instead return a structured error and log the offending raw id.
        let recipient = start_mock_bp_server(400, r#"{"error":"bad"}"#).await;
        let nrs = serde_json::json!([
            {
                "id": "!!!not-decodable!!!",
                "body": "AAAA",
                "thread_id": null,
                "account_id": VALID_HEX,
                "dapp_id": VALID_HEX,
            }
        ]);

        // Capture WARN+ records so we can assert the undecodable id is logged.
        let buf = Arc::new(parking_lot::Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(SharedBuf(buf.clone()))
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .without_time()
            .finish();

        let guard = tracing::subscriber::set_default(subscriber);
        let resp = run(nrs, make_router_with_recipient(recipient)).await.unwrap();
        drop(guard);

        assert!(resp.result.is_none());
        let err = resp.error.expect("error expected");
        assert_eq!(err.code, "INTERNAL_ERROR");
        // The message hash is unknown (id did not decode), so it degrades to empty.
        assert_eq!(err.data.expect("error data expected").message_hash, "");

        // The raw, undecodable id must be surfaced at WARN level for operators.
        let logs = String::from_utf8_lossy(&buf.lock().clone()).into_owned();
        assert!(logs.contains("undecodable id"), "expected warn about undecodable id, got: {logs}");
        assert!(
            logs.contains("!!!not-decodable!!!"),
            "warn should include the raw id, got: {logs}"
        );
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
