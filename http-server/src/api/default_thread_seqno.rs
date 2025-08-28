// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;

use salvo::prelude::*;

use crate::ResolvingResult;
use crate::WebServer;
pub struct LastSeqnoHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter> {
    _marker: PhantomData<(TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter)>,
}

impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
    LastSeqnoHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
{
    pub fn new() -> Self {
        Self { _marker: PhantomData }
    }
}
#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter> Handler
    for LastSeqnoHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
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
        _req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
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

        match (web_server.get_default_thread_seqno)() {
            Ok(seq_no) => {
                let response = serde_json::json!({
                    "last_seq_no": seq_no,
                });
                res.render(Json(response));
            }
            Err(e) => {
                res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                res.render(format!("Original error: {e}"));
            }
        };
    }
}
