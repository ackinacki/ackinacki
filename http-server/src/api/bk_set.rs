// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::marker::PhantomData;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use node_types::ThreadIdentifier;
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

use super::bk_set_update::render_error_response;
use crate::ApiBk;
use crate::ResolvingResult;
use crate::WebServer;

/// Exact BK set of one block: the signer set the block was produced and signed with.
/// Unlike `ApiBkSet` it is never merged with previous sets, so removed BKs disappear at once.
///
/// Field order matters: the prod haproxy health check matches `"seq_no":0}` in the body,
/// so `seq_no` must stay the last serialized field. The check sees only as much of the body as
/// its buffer holds (haproxy `tune.bufsize`), which must fit the whole response.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ApiExactBkSet {
    /// Hex-encoded thread identifier (68 lowercase hex chars).
    pub thread_id: String,
    pub bk_set: Vec<ApiBk>,
    pub future_bk_set: Vec<ApiBk>,
    pub seq_no: u64,
}

pub struct ExactBkSetSnapshot {
    update_time: SystemTime,
    set: ApiExactBkSet,
}

impl Default for ExactBkSetSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl ExactBkSetSnapshot {
    pub fn new() -> Self {
        Self {
            update_time: UNIX_EPOCH,
            set: ApiExactBkSet {
                seq_no: 0,
                thread_id: String::new(),
                bk_set: Vec::new(),
                future_bk_set: Vec::new(),
            },
        }
    }

    /// `seq_no` of the currently served snapshot.
    pub fn seq_no(&self) -> u64 {
        self.set.seq_no
    }

    /// Stores `set` unless it is older than the current one. Returns whether it was stored.
    pub fn replace(&mut self, set: ApiExactBkSet) -> bool {
        if set.seq_no < self.set.seq_no {
            return false;
        }
        self.set = set;
        self.update_time = SystemTime::now();
        true
    }
}

/// Tracks consecutive rejected exact BK set updates, so a node catching up logs the first
/// rejection loudly and the rest quietly.
#[derive(Default)]
pub struct RejectionSeries {
    run: Option<RejectedRun>,
}

#[derive(Debug, PartialEq)]
pub struct RejectedRun {
    pub count: u64,
    pub first_seq_no: u64,
    pub last_seq_no: u64,
}

impl RejectionSeries {
    /// Returns true when this rejection starts a new series.
    pub fn on_rejected(&mut self, update_seq_no: u64) -> bool {
        match &mut self.run {
            Some(run) => {
                run.count += 1;
                run.last_seq_no = update_seq_no;
                false
            }
            None => {
                self.run = Some(RejectedRun {
                    count: 1,
                    first_seq_no: update_seq_no,
                    last_seq_no: update_seq_no,
                });
                true
            }
        }
    }

    /// Ends the series, returning it when there was one.
    pub fn on_accepted(&mut self) -> Option<RejectedRun> {
        self.run.take()
    }
}

pub struct BkSetHandler<TBPResolver, TSeqnoGetter>(
    PhantomData<TBPResolver>,
    PhantomData<TSeqnoGetter>,
);

impl<TBPResolver, TSeqnoGetter> BkSetHandler<TBPResolver, TSeqnoGetter> {
    pub fn new() -> Self {
        Self(PhantomData, PhantomData)
    }
}

#[async_trait]
impl<TBPResolver, TSeqnoGetter> Handler for BkSetHandler<TBPResolver, TSeqnoGetter>
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
        let Ok(web_server_state) = depot.obtain::<WebServer<TBPResolver, TSeqnoGetter>>() else {
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
            let snapshot = web_server_state.exact_bk_set.read();
            if let Some(if_modified_since) = if_modified_since {
                if snapshot.update_time <= if_modified_since {
                    res.status_code(StatusCode::NOT_MODIFIED);
                    return;
                }
            }
            (snapshot.set.clone(), snapshot.update_time)
        };
        res.status_code(StatusCode::OK);
        let _ = res.add_header("Last-Modified", httpdate::fmt_http_date(update_time), true);
        res.render(Json(result));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiBkStatus;
    use crate::ApiPubKey;
    use crate::ApiUInt256;

    #[test]
    fn rejection_series_starts_once_and_reports_the_run() {
        let mut series = RejectionSeries::default();
        assert_eq!(series.on_accepted(), None);
        assert!(series.on_rejected(10));
        assert!(!series.on_rejected(11));
        assert!(!series.on_rejected(12));
        assert_eq!(
            series.on_accepted(),
            Some(RejectedRun { count: 3, first_seq_no: 10, last_seq_no: 12 })
        );
        assert_eq!(series.on_accepted(), None);
        assert!(series.on_rejected(20));
    }

    fn bk(index: usize) -> ApiBk {
        ApiBk {
            pubkey: ApiPubKey([index as u8; 48]),
            epoch_finish_seq_no: Some(7),
            wait_step: 3,
            status: ApiBkStatus::Active,
            address: "0:aa".to_string(),
            stake: "1".to_string(),
            owner_address: ApiUInt256([index as u8; 32]),
            signer_index: index,
            owner_pubkey: ApiUInt256([index as u8 + 1; 32]),
            protocol_version_support: "1.0.0".to_string(),
            ttl_seq_no: Some(5),
        }
    }

    fn exact(seq_no: u64) -> ApiExactBkSet {
        ApiExactBkSet {
            seq_no,
            thread_id: "00".repeat(34),
            bk_set: vec![bk(1)],
            future_bk_set: vec![],
        }
    }

    #[test]
    fn exact_snapshot_never_regresses() {
        let mut snapshot = ExactBkSetSnapshot::new();
        assert!(snapshot.replace(exact(10)));
        assert!(!snapshot.replace(exact(9)));
        assert_eq!(snapshot.set.seq_no, 10);
        assert!(snapshot.replace(exact(11)));
        assert_eq!(snapshot.set.seq_no, 11);
    }

    #[test]
    fn exact_bk_set_json_ends_with_seq_no() {
        // The prod haproxy check matches `"seq_no":0}` in the body.
        let body = serde_json::to_string(&exact(0)).unwrap();
        assert!(body.ends_with("\"seq_no\":0}"), "{body}");
        let body = serde_json::to_string(&exact(12345)).unwrap();
        assert!(body.ends_with("\"seq_no\":12345}"), "{body}");
    }

    #[test]
    fn exact_bk_set_json_shape() {
        let value = serde_json::to_value(exact(10)).unwrap();
        let top = value.as_object().unwrap();
        let mut keys: Vec<_> = top.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["bk_set", "future_bk_set", "seq_no", "thread_id"]);
        assert_eq!(top["thread_id"].as_str().unwrap().len(), 68);
        let element = top["bk_set"][0].as_object().unwrap();
        for key in ["signer_index", "pubkey", "owner_address"] {
            assert!(element.contains_key(key), "missing {key}");
        }
        for key in ["ttl_seq_no", "node_id", "node_owner_pk", "epoch_start_seq_no"] {
            assert!(!element.contains_key(key), "unexpected {key}");
        }
    }
}
