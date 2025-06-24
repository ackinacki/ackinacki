use std::collections::HashSet;
use std::marker::PhantomData;
use std::ops::Deref;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use salvo::async_trait;
use salvo::http::StatusCode;
use salvo::prelude::Json;
use salvo::Depot;
use salvo::FlowCtrl;
use salvo::Handler;
use salvo::Request;
use salvo::Response;
use serde::Serialize;

use crate::ResolvingResult;
use crate::WebServer;

#[derive(Clone, Debug)]
pub struct BlockKeeperSetUpdate {
    pub node_ids: HashSet<String>,
}

pub struct BkSetSnapshot {
    update_time: SystemTime,
    node_ids: HashSet<String>,
}

impl Default for BkSetSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl BkSetSnapshot {
    pub fn new() -> Self {
        Self { update_time: UNIX_EPOCH, node_ids: HashSet::new() }
    }

    pub fn update(&mut self, bk_set: BlockKeeperSetUpdate) {
        let mut has_changes = false;
        for node_id in bk_set.node_ids {
            if self.node_ids.insert(node_id) {
                has_changes = true;
            }
        }
        if has_changes {
            self.update_time = SystemTime::now();
        }
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct BkSetResponse {
    result: Option<BkSetResult>,
    error: Option<BkSetError>,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct BkInfo {
    pub node_id: String,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct BkSetResult {
    bk_set: Vec<BkInfo>,
}

impl From<&BkSetSnapshot> for BkSetResult {
    fn from(value: &BkSetSnapshot) -> Self {
        Self { bk_set: value.node_ids.iter().map(|x| BkInfo { node_id: x.clone() }).collect() }
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct BkSetError {
    code: String,
    message: String,
}

pub struct BkSetHandler<
    TMessage,
    TMsgConverter,
    TBPResolver,
    TBMLicensePubkeyLoader,
    TBocByAddrGetter,
>(
    PhantomData<TMessage>,
    PhantomData<TMsgConverter>,
    PhantomData<TBPResolver>,
    PhantomData<TBMLicensePubkeyLoader>,
    PhantomData<TBocByAddrGetter>,
);

impl<TMessage, TMsgConverter, TBPResolver, TBMLicensePubkeyLoader, TBocByAddrGetter>
    BkSetHandler<TMessage, TMsgConverter, TBPResolver, TBMLicensePubkeyLoader, TBocByAddrGetter>
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData, PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver, TBMLicensePubkeyLoader, TBocByAddrGetter> Handler
    for BkSetHandler<TMessage, TMsgConverter, TBPResolver, TBMLicensePubkeyLoader, TBocByAddrGetter>
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
        tracing::info!(target: "http_server", "Rest service: request got!");

        let Ok(web_server_state) = depot.obtain::<WebServer<
            TMessage,
            TMsgConverter,
            TBPResolver,
            TBMLicensePubkeyLoader,
            TBocByAddrGetter,
        >>() else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            render_error_response(
                res,
                "INTERNAL_SERVER_ERROR",
                Some("Web Server state is not found"),
            );
            return;
        };
        let Ok(if_modified_since) =
            req.header("If-Modified-Since").map(httpdate::parse_http_date).transpose()
        else {
            res.status_code(StatusCode::BAD_REQUEST);
            return;
        };
        let (result, update_time) = {
            let bk_set = web_server_state.bk_set.read();
            if let Some(if_modified_since) = if_modified_since {
                if bk_set.update_time <= if_modified_since {
                    res.status_code(StatusCode::NOT_MODIFIED);
                    return;
                }
            }
            (BkSetResult::from(bk_set.deref()), bk_set.update_time)
        };
        res.status_code(StatusCode::OK);
        let _ = res.add_header("Last-Modified", httpdate::fmt_http_date(update_time), true);
        res.render(Json(result));
    }
}

fn render_error_response(res: &mut Response, code: &str, message: Option<&str>) {
    res.render(Json(BkSetResponse {
        result: None,
        error: Some(BkSetError {
            code: code.to_string(),
            message: message.unwrap_or(code).to_string(),
        }),
    }));
}
