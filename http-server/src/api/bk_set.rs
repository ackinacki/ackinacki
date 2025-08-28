// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

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
use serde::Deserialize;
use serde::Serialize;

use crate::ResolvingResult;
use crate::WebServer;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct BlockKeeperSetUpdate {
    pub seq_no: u32,
    #[serde(with = "bk_vec_serde")]
    pub current: Vec<(String, [u8; 32])>,
    #[serde(with = "bk_vec_serde")]
    pub future: Vec<(String, [u8; 32])>,
}

mod bk_vec_serde {
    use hex::encode;
    use serde::ser::SerializeSeq;
    use serde::Serializer;

    pub fn serialize<S>(vec: &Vec<(String, [u8; 32])>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(vec.len()))?;
        for (s, bytes) in vec {
            seq.serialize_element(&(s, encode(bytes)))?;
        }
        seq.end()
    }
}

pub struct BkSetSnapshot {
    update_time: SystemTime,
    seq_no: u32,
    nodes: Vec<(String, [u8; 32])>,
    future_nodes: Vec<(String, [u8; 32])>,
}

impl Default for BkSetSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl BkSetSnapshot {
    pub fn new() -> Self {
        Self { update_time: UNIX_EPOCH, seq_no: 0, nodes: vec![], future_nodes: vec![] }
    }

    pub fn update(&mut self, bk_update: BlockKeeperSetUpdate) {
        self.seq_no = bk_update.seq_no;
        self.nodes = bk_update.current;
        self.future_nodes = bk_update.future;
        self.update_time = SystemTime::now();
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct BkSetResponse {
    result: Option<BkSetResult>,
    error: Option<BkSetError>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct BkInfo {
    pub node_id: String,
    pub node_owner_pk: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct BkSetResult {
    pub bk_set: Vec<BkInfo>,
    pub future_bk_set: Vec<BkInfo>,
    pub seq_no: u32,
}

impl From<&BkSetSnapshot> for BkSetResult {
    fn from(value: &BkSetSnapshot) -> Self {
        Self {
            bk_set: value
                .nodes
                .iter()
                .map(|(node_id, node_owner_pk)| BkInfo {
                    node_id: node_id.clone(),
                    node_owner_pk: hex::encode(node_owner_pk),
                })
                .collect(),

            future_bk_set: value
                .future_nodes
                .iter()
                .map(|(node_id, node_owner_pk)| BkInfo {
                    node_id: node_id.clone(),
                    node_owner_pk: hex::encode(node_owner_pk),
                })
                .collect(),

            seq_no: value.seq_no,
        }
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct BkSetError {
    code: String,
    message: String,
}

pub struct BkSetHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>(
    PhantomData<TMessage>,
    PhantomData<TMsgConverter>,
    PhantomData<TBPResolver>,
    PhantomData<TBocByAddrGetter>,
    PhantomData<TSeqnoGetter>,
);

impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
    BkSetHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData, PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter> Handler
    for BkSetHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
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
        tracing::info!(target: "http_server", "Rest service: request got!");

        let Ok(web_server_state) = depot.obtain::<WebServer<
            TMessage,
            TMsgConverter,
            TBPResolver,
            TBocByAddrGetter,
            TSeqnoGetter,
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
