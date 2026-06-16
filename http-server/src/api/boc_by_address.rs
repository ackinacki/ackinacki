// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;
use std::time::Instant;

use ext_messages_auth::auth::AccountRequest;
use node_types::ThreadIdentifier;
use salvo::prelude::*;
use serde::Serialize;
use tokio::sync::oneshot;
use tvm_types::base64_encode;

use crate::api::validation::parse_hex32;
use crate::ResolvingResult;
use crate::WebServer;

#[derive(Serialize, Debug)]
struct AccountResponse {
    boc: String,
    account_id: String,
    dapp_id: Option<String>,
    state_timestamp: u64,
}

pub struct BocByAddressHandler<TBPResolver, TSeqnoGetter>(
    PhantomData<TBPResolver>,
    PhantomData<TSeqnoGetter>,
);

impl<TBPResolver, TSeqnoGetter> BocByAddressHandler<TBPResolver, TSeqnoGetter> {
    pub fn new() -> Self {
        Self(PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TBPResolver, TSeqnoGetter> Handler for BocByAddressHandler<TBPResolver, TSeqnoGetter>
where
    TBPResolver: Clone + Send + Sync + 'static + FnMut(ThreadIdentifier) -> ResolvingResult,
    TSeqnoGetter: Clone + Send + Sync + 'static + Fn() -> anyhow::Result<u32>,
{
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let raw_account_id = match req.query::<&str>("account_id") {
            Some(s) if !s.trim().is_empty() => s.trim().to_owned(),
            _ => {
                res.status_code(StatusCode::BAD_REQUEST);
                res.render("account_id parameter required");
                return;
            }
        };
        let raw_dapp_id = match req.query::<&str>("dapp_id") {
            Some(s) if !s.trim().is_empty() => s.trim().to_owned(),
            _ => {
                res.status_code(StatusCode::BAD_REQUEST);
                res.render("dapp_id parameter required");
                return;
            }
        };
        let account_id = match parse_hex32(&raw_account_id, "account_id") {
            Ok(v) => v,
            Err(msg) => {
                tracing::warn!(target: "http_server", account_id = %raw_account_id, "{msg}");
                res.status_code(StatusCode::BAD_REQUEST);
                res.render(msg);
                return;
            }
        };
        let request_dapp_id = match parse_hex32(&raw_dapp_id, "dapp_id") {
            Ok(v) => v,
            Err(msg) => {
                tracing::warn!(target: "http_server", dapp_id = %raw_dapp_id, "{msg}");
                res.status_code(StatusCode::BAD_REQUEST);
                res.render(msg);
                return;
            }
        };

        let moment = Instant::now();

        let Ok(web_server) = depot.obtain::<WebServer<TBPResolver, TSeqnoGetter>>() else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Internal server error: Web Server state not found");
            return;
        };

        let (response_tx, response_rx) = oneshot::channel();
        // Address the account by both dApp and account id. The node parses this
        // `dapp::account` routing directly; sending a bare account id would fall
        // back to redirect routing and ignore the requested dApp.
        let address = format!("{request_dapp_id}::{account_id}");
        let request = AccountRequest { address, response: response_tx };

        if web_server.account_request_sender.send(request).await.is_err() {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Internal server error: account request dispatch failed");
            return;
        }

        let Ok(account_response) = response_rx.await else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Internal server error: unable to retrieve the account");
            return;
        };

        let (http_code, payload_or_err) =
            match account_response.and_then(|(account, dapp_id, ts)| {
                let boc = account.write_bytes().map_err(|e| anyhow::anyhow!("{e}"))?;
                Ok((base64_encode(&boc), dapp_id.map(|id| id.to_hex_string()), ts))
            }) {
                Ok((boc, dapp_id, state_timestamp)) => {
                    let payload = AccountResponse {
                        boc,
                        account_id: account_id.clone(),
                        dapp_id,
                        state_timestamp,
                    };
                    (StatusCode::OK, Ok(payload))
                }
                Err(e) => (StatusCode::NOT_FOUND, Err(format!("Original error: {e}"))),
            };

        res.status_code(http_code);
        match payload_or_err {
            Ok(payload) => res.render(Json(payload)),
            Err(err) => res.render(err),
        }

        if let Some(m) = &web_server.metrics {
            let millis = moment.elapsed().as_millis().min(u64::MAX as u128) as u64;
            m.report_boc_by_address_response(millis, http_code.as_u16());
        }
    }
}

#[cfg(test)]
mod tests {
    use node_types::ThreadIdentifier;
    use salvo::prelude::*;
    use salvo::test::TestClient;
    use telemetry_utils::mpsc::instrumented_channel;
    use telemetry_utils::mpsc::InstrumentedChannelMetrics;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    use super::AccountResponse;
    use super::BocByAddressHandler;
    use crate::ExtMsgFeedback;
    use crate::NotQueuedExtMessage;
    use crate::ResolvingResult;
    use crate::WebServer;

    const VALID_ACCOUNT_ID: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const VALID_DAPP_ID: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    struct NoopMetrics;

    impl InstrumentedChannelMetrics for NoopMetrics {
        fn report_channel(&self, _channel: &'static str, _delta: isize) {}
    }

    type TestBpResolver = fn(ThreadIdentifier) -> ResolvingResult;
    type TestSeqnoGetter = fn() -> anyhow::Result<u32>;

    fn test_bp_resolver(_thread_id: ThreadIdentifier) -> ResolvingResult {
        ResolvingResult::new(false, vec![])
    }

    fn test_seqno_getter() -> anyhow::Result<u32> {
        Ok(0)
    }

    #[test]
    fn account_response_serializes_unknown_dapp_id_as_null() {
        let response = AccountResponse {
            boc: "QkND".to_string(),
            account_id: "aa".repeat(32),
            dapp_id: None,
            state_timestamp: 42,
        };
        let v = serde_json::to_value(&response).unwrap();
        let obj = v.as_object().unwrap();
        assert!(obj.contains_key("boc"));
        assert!(obj.contains_key("account_id"));
        assert!(obj.contains_key("dapp_id"));
        assert_eq!(obj["dapp_id"], serde_json::Value::Null);
        assert!(obj.contains_key("state_timestamp"));
        assert_eq!(obj["state_timestamp"], serde_json::json!(42));
    }

    #[tokio::test]
    async fn handler_dispatches_account_request_with_dapp_and_account_address() {
        let (incoming_tx, _incoming_rx) = instrumented_channel::<(
            NotQueuedExtMessage,
            oneshot::Sender<ExtMsgFeedback>,
        )>(None::<NoopMetrics>, "test-boc-by-address");
        let (account_request_tx, mut account_request_rx) = mpsc::channel(1);

        let web_server: WebServer<TestBpResolver, TestSeqnoGetter> = WebServer::new(
            "127.0.0.1:0",
            ".",
            incoming_tx,
            account_request_tx,
            test_bp_resolver,
            test_seqno_getter,
            None,
            None,
            None,
        );
        let service = Service::new(
            Router::with_path("account")
                .hoop(affix_state::inject(web_server))
                .get(BocByAddressHandler::<TestBpResolver, TestSeqnoGetter>::new()),
        );

        let request_url = format!(
            "http://127.0.0.1/account?account_id={VALID_ACCOUNT_ID}&dapp_id={VALID_DAPP_ID}",
        );
        let response_task =
            tokio::spawn(async move { TestClient::get(request_url).send(&service).await });

        let account_request =
            account_request_rx.recv().await.expect("handler should dispatch account request");
        assert_eq!(account_request.address, format!("{VALID_DAPP_ID}::{VALID_ACCOUNT_ID}"));
        account_request
            .response
            .send(Err(anyhow::anyhow!("test response")))
            .expect("handler should still be awaiting account response");

        let res = response_task.await.expect("handler task should complete");
        assert_eq!(res.status_code, Some(StatusCode::NOT_FOUND));
    }
}
