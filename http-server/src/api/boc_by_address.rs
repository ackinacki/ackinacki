// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
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
    TBMLicensePubkeyLoader,
    TBocByAddrGetter,
>(
    PhantomData<TMesssage>,
    PhantomData<TMsgConverter>,
    PhantomData<TBPResolver>,
    PhantomData<TBMLicensePubkeyLoader>,
    PhantomData<TBocByAddrGetter>,
);

impl<TMessage, TMsgConverter, TBPResolver, TBMLicensePubkeyLoader, TBocByAddrGetter>
    BocByAddressHandler<
        TMessage,
        TMsgConverter,
        TBPResolver,
        TBMLicensePubkeyLoader,
        TBocByAddrGetter,
    >
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData, PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver, TBMLicensePubkeyLoader, TBocByAddrGetter> Handler
    for BocByAddressHandler<
        TMessage,
        TMsgConverter,
        TBPResolver,
        TBMLicensePubkeyLoader,
        TBocByAddrGetter,
    >
where
    TMessage: Clone + Send + Sync + 'static + std::fmt::Debug,
    TMsgConverter: Clone
        + Send
        + Sync
        + 'static
        + Fn(tvm_block::Message, [u8; 34]) -> anyhow::Result<TMessage>,
    TBPResolver: Clone + Send + Sync + 'static + FnMut([u8; 34]) -> ResolvingResult,
    TBMLicensePubkeyLoader: Send + Sync + Clone + 'static + Fn(String) -> Option<String>,
    TBocByAddrGetter: Clone + Send + Sync + 'static + Fn(String) -> anyhow::Result<String>,
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
            TBMLicensePubkeyLoader,
            TBocByAddrGetter,
        >>() else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Internal server error: Web Server state not found");
            return;
        };

        let http_code = match (web_server.get_boc_by_addr)(address) {
            Ok(boc) => {
                let response = serde_json::json!({
                    "boc": boc,
                });
                res.render(Json(response));
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
