// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;
use std::time::Instant;

use ext_messages_auth::auth::AccountRequest;
use node_types::ThreadIdentifier;
use salvo::prelude::*;
use tokio::sync::oneshot;
use tvm_types::base64_encode;

use crate::ResolvingResult;
use crate::WebServer;
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
        let raw_addr: &str = match req.query::<&str>("address") {
            Some(s) if !s.trim().is_empty() => s.trim(),
            _ => {
                res.status_code(StatusCode::BAD_REQUEST);
                res.render("Address parameter required");
                return;
            }
        };
        let address = raw_addr.strip_prefix("0:").unwrap_or(raw_addr).to_owned();

        let moment = Instant::now();

        let Ok(web_server) = depot.obtain::<WebServer<TBPResolver, TSeqnoGetter>>() else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Internal server error: Web Server state not found");
            return;
        };

        let (response_tx, response_rx) = oneshot::channel();
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
                Ok((boc, dapp_id_opt, state_timestamp)) => {
                    let payload = serde_json::json!({
                        "boc": boc,
                        "dapp_id": dapp_id_opt,
                        "state_timestamp": state_timestamp,
                    });
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
