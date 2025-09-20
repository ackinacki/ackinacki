// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::marker::PhantomData;
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
use serde::de::Error;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::ResolvingResult;
use crate::WebServer;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ApiUInt256(pub [u8; 32]);

impl Serialize for ApiUInt256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'de> Deserialize<'de> for ApiUInt256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let decoded = hex::decode(&s).map_err(D::Error::custom)?;
        if decoded.len() != 32 {
            return Err(Error::custom(format!(
                "expected 32 bytes (64 hex chars), got {}",
                decoded.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&decoded);
        Ok(ApiUInt256(arr))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApiPubKey(pub [u8; 48]);

impl Serialize for ApiPubKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'de> Deserialize<'de> for ApiPubKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let decoded = hex::decode(&s).map_err(D::Error::custom)?;
        if decoded.len() != 48 {
            return Err(Error::custom(format!(
                "expected 48 bytes (96 hex chars), got {}",
                decoded.len()
            )));
        }
        let mut arr = [0u8; 48];
        arr.copy_from_slice(&decoded);
        Ok(ApiPubKey(arr))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ApiBkSet {
    pub seq_no: u64,
    pub current: Vec<ApiBk>,
    pub future: Vec<ApiBk>,
}

impl ApiBkSet {
    pub fn update(&mut self, other: &ApiBkSet) -> bool {
        if other.seq_no < self.seq_no {
            return false;
        }
        self.seq_no = other.seq_no;
        Self::update_bks(&mut self.current, &other.current, other.seq_no);
        Self::update_bks(&mut self.future, &other.future, other.seq_no);
        true
    }

    fn update_bks(old_bks: &mut Vec<ApiBk>, new_bks: &[ApiBk], seq_no: u64) {
        let mut new_bks =
            HashMap::<ApiUInt256, &ApiBk>::from_iter(new_bks.iter().map(|x| (x.owner_address, x)));
        for old_bk in &mut *old_bks {
            if let Some(new_bk) = new_bks.remove(&old_bk.owner_address) {
                *old_bk = new_bk.clone();
                old_bk.ttl_seq_no = None;
            } else if old_bk.ttl_seq_no.is_none() {
                old_bk.ttl_seq_no = Some(seq_no + old_bk.wait_step);
            }
        }
        old_bks.extend(new_bks.into_values().cloned());
        old_bks.retain(|old_bk| !old_bk.expired(seq_no));
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ApiBk {
    pub pubkey: ApiPubKey,
    pub epoch_finish_seq_no: Option<u64>,
    pub wait_step: u64,
    pub status: ApiBkStatus,
    pub address: String,
    pub stake: String,
    pub owner_address: ApiUInt256,
    pub signer_index: usize,
    pub owner_pubkey: ApiUInt256,

    #[serde(skip)]
    pub ttl_seq_no: Option<u64>,
}

impl ApiBk {
    pub fn expired(&self, seq_no: u64) -> bool {
        if let Some(ttl) = self.ttl_seq_no {
            ttl < seq_no
        } else {
            false
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ApiBkStatus {
    PreEpoch,
    Active,
    CalledToFinish,
    Expired,
}

pub struct ApiBkSetSnapshot {
    update_time: SystemTime,
    bk_set: ApiBkSet,
}

impl Default for ApiBkSetSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiBkSetSnapshot {
    pub fn new() -> Self {
        Self {
            update_time: UNIX_EPOCH,
            bk_set: ApiBkSet { seq_no: 0, current: Vec::new(), future: Vec::new() },
        }
    }

    pub fn replace(&mut self, bk_set: ApiBkSet) {
        self.bk_set = bk_set;
        self.update_time = SystemTime::now();
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ApiBkSetResponse {
    result: Option<ApiBkSet>,
    error: Option<ApiBkSetError>,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ApiBkSetError {
    code: String,
    message: String,
}

pub struct ApiBkSetHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>(
    PhantomData<TMessage>,
    PhantomData<TMsgConverter>,
    PhantomData<TBPResolver>,
    PhantomData<TBocByAddrGetter>,
    PhantomData<TSeqnoGetter>,
);

impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
    ApiBkSetHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
{
    pub fn new() -> Self {
        Self(PhantomData, PhantomData, PhantomData, PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter> Handler
    for ApiBkSetHandler<TMessage, TMsgConverter, TBPResolver, TBocByAddrGetter, TSeqnoGetter>
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
            let snapshot = web_server_state.bk_set.read();
            if let Some(if_modified_since) = if_modified_since {
                if snapshot.update_time <= if_modified_since {
                    res.status_code(StatusCode::NOT_MODIFIED);
                    return;
                }
            }
            (snapshot.bk_set.clone(), snapshot.update_time)
        };
        res.status_code(StatusCode::OK);
        let _ = res.add_header("Last-Modified", httpdate::fmt_http_date(update_time), true);
        res.render(Json(result));
    }
}

fn render_error_response(res: &mut Response, code: &str, message: Option<&str>) {
    res.render(Json(ApiBkSetResponse {
        result: None,
        error: Some(ApiBkSetError {
            code: code.to_string(),
            message: message.unwrap_or(code).to_string(),
        }),
    }));
}
