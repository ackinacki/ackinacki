// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;
use std::time::Instant;

use salvo::prelude::*;

use crate::ResolvingResult;
use crate::WebServer;
pub struct BocByAddressHandler<
    TMesssage,
    TMsgConverter,
    TBPResolver,
    TBocByAddrGetter,
    TSeqnoGetter,
>(
    PhantomData<TMesssage>,
    PhantomData<TMsgConverter>,
    PhantomData<TBPResolver>,
    PhantomData<TBocByAddrGetter>,
    PhantomData<TSeqnoGetter>,
);

impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
    BocByAddressHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData, PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter> Handler
    for BocByAddressHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
where
    TMessage: Clone + Send + Sync + 'static + std::fmt::Debug,
    TMsgConverter: Clone
        + Send
        + Sync
        + 'static
        + Fn(tvm_block::Message, [u8; 34]) -> anyhow::Result<TMessage>,
    TBPResolver: Clone + Send + Sync + 'static + FnMut([u8; 34]) -> ResolvingResult,
    TBocByAddrGetter:
        Clone + Send + Sync + 'static + Fn(String) -> anyhow::Result<(String, Option<String>)>,
    TSeqnoGetter: Clone + Send + Sync + 'static + Fn() -> anyhow::Result<u32>,
{
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let address: String = req.query("address").unwrap_or_default();
        let address = address.trim_start_matches("0:").to_string();

        if address.is_empty() {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render("Address parameter required");
            return;
        }
        let moment = Instant::now();

        let Ok(web_server) = depot.obtain::<WebServer<
            TMessage,
            TMsgConverter,
            TBPResolver,
            TBocByAddrGetter,
            TSeqnoGetter,
        >>() else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Internal server error: Web Server state not found");
            return;
        };

        // This code is a bit repetitive, but it's easy to understand.
        let http_code = match (web_server.get_boc_by_addr)(address) {
            Ok((boc, None)) => {
                res.render(Json(serde_json::json!({
                    "boc": boc,
                })));
                StatusCode::OK
            }
            Ok((boc, Some(dapp_id))) => {
                res.render(Json(serde_json::json!({
                    "boc": boc,
                    "dapp_id": dapp_id
                })));
                StatusCode::OK
            }
            Err(e) => {
                res.status_code(StatusCode::NOT_FOUND);
                res.render(format!("Original error: {e}"));
                StatusCode::NOT_FOUND
            }
        };

        web_server.metrics.as_ref().inspect(|m| {
            m.report_boc_by_address_response(
                moment.elapsed().as_millis() as u64,
                http_code.as_u16(),
            )
        });
    }
}
