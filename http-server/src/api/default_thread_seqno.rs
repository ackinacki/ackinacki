// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;

use node_types::ThreadIdentifier;
use salvo::prelude::*;

use crate::ResolvingResult;
use crate::WebServer;
pub struct LastSeqnoHandler<TBPResolver, TSeqnoGetter> {
    _marker: PhantomData<(TBPResolver, TSeqnoGetter)>,
}

impl<TBPResolver, TSeqnoGetter> LastSeqnoHandler<TBPResolver, TSeqnoGetter> {
    pub fn new() -> Self {
        Self { _marker: PhantomData }
    }
}
#[async_trait]
impl<TBPResolver, TSeqnoGetter> Handler for LastSeqnoHandler<TBPResolver, TSeqnoGetter>
where
    TBPResolver: Clone + Send + Sync + 'static + FnMut(ThreadIdentifier) -> ResolvingResult,
    TSeqnoGetter: Clone + Send + Sync + 'static + Fn() -> anyhow::Result<u32>,
{
    async fn handle(
        &self,
        _req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let Ok(web_server) = depot.obtain::<WebServer<TBPResolver, TSeqnoGetter>>() else {
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
