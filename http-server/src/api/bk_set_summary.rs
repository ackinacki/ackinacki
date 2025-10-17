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

use crate::ApiBk;
use crate::ApiBkSet;
use crate::ResolvingResult;
use crate::WebServer;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct BkSetSummary {
    pub seq_no: u64,
    #[serde(with = "bk_vec_serde")]
    pub current: Vec<(String, [u8; 32], u64)>,
    #[serde(with = "bk_vec_serde")]
    pub future: Vec<(String, [u8; 32], u64)>,
}

impl BkSetSummary {
    pub fn new(bk_set: &ApiBkSet) -> Self {
        fn collect_node_id_owner_pk(bk_set: &[ApiBk]) -> Vec<(String, [u8; 32], u64)> {
            bk_set
                .iter()
                .map(|x| {
                    let node_id = x.owner_address;
                    let node_as_string = hex::encode(node_id.0);
                    (node_as_string, x.owner_pubkey.0, x.epoch_finish_seq_no.unwrap_or_default())
                })
                .collect()
        }
        Self {
            seq_no: bk_set.seq_no,
            current: collect_node_id_owner_pk(&bk_set.current),
            future: collect_node_id_owner_pk(&bk_set.future),
        }
    }
}

mod bk_vec_serde {
    use hex::encode;
    use serde::ser::SerializeSeq;
    use serde::Serializer;

    pub fn serialize<S>(
        vec: &Vec<(String, [u8; 32], u64)>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(vec.len()))?;
        for (s, bytes, epoch_start_seq_no) in vec {
            seq.serialize_element(&(s, encode(bytes), epoch_start_seq_no))?;
        }
        seq.end()
    }
}

pub struct BkSetSummarySnapshot {
    update_time: SystemTime,
    seq_no: u64,
    nodes: Vec<(String, [u8; 32], u64)>,
    future_nodes: Vec<(String, [u8; 32], u64)>,
}

impl Default for BkSetSummarySnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl BkSetSummarySnapshot {
    pub fn new() -> Self {
        Self { update_time: UNIX_EPOCH, seq_no: 0, nodes: vec![], future_nodes: vec![] }
    }

    pub fn replace(&mut self, bk_update: BkSetSummary) {
        self.seq_no = bk_update.seq_no;
        self.nodes = bk_update.current;
        self.future_nodes = bk_update.future;
        self.update_time = SystemTime::now();
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct BkSetSummaryResponse {
    result: Option<BkSetSummaryResult>,
    error: Option<BkSetSummaryError>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct BkSummary {
    pub node_id: String,
    pub node_owner_pk: String,
    pub epoch_start_seq_no: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct BkSetSummaryResult {
    pub bk_set: Vec<BkSummary>,
    pub future_bk_set: Vec<BkSummary>,
    pub seq_no: u64,
}

impl From<&BkSetSummarySnapshot> for BkSetSummaryResult {
    fn from(value: &BkSetSummarySnapshot) -> Self {
        Self {
            bk_set: value
                .nodes
                .iter()
                .map(|(node_id, node_owner_pk, epoch_start_seq_no)| BkSummary {
                    node_id: node_id.clone(),
                    node_owner_pk: hex::encode(node_owner_pk),
                    epoch_start_seq_no: *epoch_start_seq_no,
                })
                .collect(),

            future_bk_set: value
                .future_nodes
                .iter()
                .map(|(node_id, node_owner_pk, epoch_start_seq_no)| BkSummary {
                    node_id: node_id.clone(),
                    node_owner_pk: hex::encode(node_owner_pk),
                    epoch_start_seq_no: *epoch_start_seq_no,
                })
                .collect(),

            seq_no: value.seq_no,
        }
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct BkSetSummaryError {
    code: String,
    message: String,
}

pub struct BkSetSummaryHandler<TMessage, TMsgConverter, TBPResolver, TSeqnoGetter>(
    PhantomData<TMessage>,
    PhantomData<TMsgConverter>,
    PhantomData<TBPResolver>,
    PhantomData<TSeqnoGetter>,
);

impl<TMessage, TMsgConverter, TBPResolver, TSeqnoGetter>
    BkSetSummaryHandler<TMessage, TMsgConverter, TBPResolver, TSeqnoGetter>
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver, TSeqnoGetter> Handler
    for BkSetSummaryHandler<TMessage, TMsgConverter, TBPResolver, TSeqnoGetter>
where
    TMessage: Clone + Send + Sync + 'static + std::fmt::Debug,
    TMsgConverter: Clone
        + Send
        + Sync
        + 'static
        + Fn(tvm_block::Message, [u8; 34]) -> anyhow::Result<TMessage>,
    TBPResolver: Clone + Send + Sync + 'static + FnMut([u8; 34]) -> ResolvingResult,
    TSeqnoGetter: Clone + Send + Sync + 'static + Fn() -> anyhow::Result<u32>,
{
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let Ok(web_server_state) =
            depot.obtain::<WebServer<TMessage, TMsgConverter, TBPResolver, TSeqnoGetter>>()
        else {
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
            let bk_set = web_server_state.bk_set_summary.read();
            if let Some(if_modified_since) = if_modified_since {
                if bk_set.update_time <= if_modified_since {
                    res.status_code(StatusCode::NOT_MODIFIED);
                    return;
                }
            }
            (BkSetSummaryResult::from(bk_set.deref()), bk_set.update_time)
        };
        res.status_code(StatusCode::OK);
        let _ = res.add_header("Last-Modified", httpdate::fmt_http_date(update_time), true);
        res.render(Json(result));
    }
}

fn render_error_response(res: &mut Response, code: &str, message: Option<&str>) {
    res.render(Json(BkSetSummaryResponse {
        result: None,
        error: Some(BkSetSummaryError {
            code: code.to_string(),
            message: message.unwrap_or(code).to_string(),
        }),
    }));
}
