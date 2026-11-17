// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
//! Background service that keeps table `latest_bk_set` in sync with the BK set of thread 0.
//!
//! It polls `/v2/bk_set` on a BK node, validates the response, queues it until the
//! anchor block (the block with the same `seq_no`) is stored in the local archive, checks the
//! set against the block's Poseidon commitment and replaces the table content.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use database::sqlite::sqlite_helper::LatestBkSetWriteCounters;
use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::AnchorLookup;
use database::sqlite::ArchLatestBkSet;
use network::truncate_chars;
use tokio::time::MissedTickBehavior;

use crate::application::metrics::Metrics;
use crate::application::metrics::ERR_BK_SET_ANCHOR;
use crate::application::metrics::ERR_BK_SET_COMMITMENT;
use crate::application::metrics::ERR_BK_SET_EVICTED;
use crate::application::metrics::ERR_BK_SET_INVALID;
use crate::application::metrics::ERR_BK_SET_POLL;
use crate::application::metrics::ERR_BK_SET_RESTORE;
use crate::application::metrics::ERR_BK_SET_STALLED;
use crate::application::metrics::ERR_BK_SET_STORE;
use crate::domain::latest_bk_set::check_commitment;
use crate::domain::latest_bk_set::parse_response;
use crate::domain::latest_bk_set::validate;
use crate::domain::latest_bk_set::BkSetRejection;
use crate::domain::latest_bk_set::BkSetResponse;
use crate::domain::latest_bk_set::CandidateQueue;
use crate::domain::latest_bk_set::Evicted;
use crate::domain::latest_bk_set::PushOutcome;
use crate::domain::models::AppState;

/// Smallest candidate queue, used for poll intervals of a minute and longer.
const CANDIDATE_QUEUE_CAPACITY: usize = 10;
/// A candidate whose anchor block has not appeared by then is dropped.
const CANDIDATE_MAX_AGE: Duration = Duration::from_secs(600);
const RESOLVE_PERIOD: Duration = Duration::from_secs(5);
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// Consecutive polls answered with a not newer `seq_no` before the poller moves to the next BK.
const STALL_POLLS: u32 = 3;
/// Responses above this size are rejected without being read to the end.
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Time of polls the queue must cover while the anchor block is not in the archive yet: as long
/// as a candidate may wait before it expires.
const QUEUE_COVERED_SECS: f64 = CANDIDATE_MAX_AGE.as_secs() as f64;

const MAX_LOGGED_STR_LEN: usize = 80;
const MAX_LOGGED_ERR_LEN: usize = 300;

pub trait LatestBkSetStore {
    fn find_anchor_block(&self, thread_id: &str, seq_no: u64) -> anyhow::Result<AnchorLookup>;
    /// The snapshot stored in `latest_bk_set`; `None` when the table is empty.
    fn load_latest_bk_set(&self) -> anyhow::Result<Option<ArchLatestBkSet>>;
    /// Queues the snapshot for the writer thread; `Ok` does not mean it was written.
    fn put_latest_bk_set(&self, item: ArchLatestBkSet) -> anyhow::Result<()>;
    fn rotation_count(&self) -> u64;
    /// What the writer thread has done with the queued snapshots so far.
    fn latest_bk_set_writes(&self) -> LatestBkSetWriteCounters;
}

impl LatestBkSetStore for SqliteHelper {
    fn find_anchor_block(&self, thread_id: &str, seq_no: u64) -> anyhow::Result<AnchorLookup> {
        SqliteHelper::find_anchor_block(self, thread_id, seq_no)
    }

    fn load_latest_bk_set(&self) -> anyhow::Result<Option<ArchLatestBkSet>> {
        SqliteHelper::load_latest_bk_set(self)
    }

    fn put_latest_bk_set(&self, item: ArchLatestBkSet) -> anyhow::Result<()> {
        SqliteHelper::put_latest_bk_set(self, item)
    }

    fn rotation_count(&self) -> u64 {
        SqliteHelper::rotation_count(self)
    }

    fn latest_bk_set_writes(&self) -> LatestBkSetWriteCounters {
        SqliteHelper::latest_bk_set_writes(self)
    }
}

/// What one resolve tick queued for the writer thread. Whether it was stored is seen later
/// through `LatestBkSetStore::latest_bk_set_writes`.
#[derive(Debug, Default, PartialEq)]
pub struct ResolveReport {
    /// `seq_no` of a new snapshot queued for writing.
    pub written: Option<u64>,
    /// `seq_no` of the last snapshot queued again after a DB rotation.
    pub rewritten: Option<u64>,
    /// `(seq_no, reason, supplier)`
    pub rejected: Vec<(u64, BkSetRejection, String)>,
    /// `(seq_no, supplier)` of candidates dropped for waiting too long for their anchor block.
    pub expired: Vec<(u64, String)>,
    /// The writer queue refused a snapshot; the rest of the report is still valid.
    pub put_error: Option<String>,
}

/// Why the poller leaves `current`, the endpoint under the cursor, after a resolve tick: it
/// supplied a rejected or expired candidate. A candidate from an endpoint the poller has already
/// left is not a reason to leave the new one.
fn advance_reason(report: &ResolveReport, current: &str) -> Option<AdvanceReason> {
    if let Some((seq_no, r, _)) = report.rejected.iter().find(|(_, _, s)| s == current) {
        return Some(AdvanceReason::CandidateRejected { seq_no: *seq_no, reason: describe(r) });
    }
    report
        .expired
        .iter()
        .find(|(_, s)| s == current)
        .map(|(seq_no, _)| AdvanceReason::Expired { seq_no: *seq_no })
}

/// Index of the cursor in a list of `len` endpoints; the list may have shrunk since.
fn cursor_index(cursor: usize, len: usize) -> Option<usize> {
    (len > 0).then(|| cursor % len)
}

/// The endpoint after `idx`; tolerates a list that shrank on a config reload.
fn next_cursor(idx: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        (idx + 1) % len
    }
}

/// Queue capacity that holds every poll of `QUEUE_COVERED_SECS`, at least
/// `CANDIDATE_QUEUE_CAPACITY`: with the default minute interval it is 10, with a one second
/// interval it is 600, so a BM lagging behind the node by tens of seconds still finds its anchor.
/// Expects a non-zero `poll_interval` (the CLI accepts 1..=3600 s); for zero the division gives
/// infinity and the capacity saturates at `usize::MAX` instead of panicking.
fn queue_capacity(poll_interval: Duration) -> usize {
    ((QUEUE_COVERED_SECS / poll_interval.as_secs_f64()).ceil() as usize)
        .max(CANDIDATE_QUEUE_CAPACITY)
}

pub struct PollerState {
    queue: CandidateQueue,
    /// Last snapshot queued for writing, or restored from the DB at start; queued again after a
    /// DB rotation. Whether the writer thread stored it is seen through the write counters.
    last_written: Option<ArchLatestBkSet>,
    last_rotation_count: u64,
}

impl PollerState {
    /// `last_written` is the snapshot already stored in `latest_bk_set`: older or equal
    /// responses are not newer, so a restart behind a lagging BK does not roll it back, and a DB
    /// rotation rewrites it even if nothing new arrives.
    pub fn with_capacity(capacity: usize, last_written: Option<ArchLatestBkSet>) -> Self {
        Self {
            queue: CandidateQueue::new(capacity, CANDIDATE_MAX_AGE),
            last_written,
            // The counter of a fresh process starts at 0: a rotation before the first tick counts.
            last_rotation_count: 0,
        }
    }

    /// Validates the response against the `seq_no` of the last snapshot queued for writing (or
    /// the restored one) and queues it, remembering `supplier`. Returns the candidate evicted
    /// from the full queue. A `seq_no` not above the newest queued candidate is `NotNewer` too:
    /// it is no progress, and then `last` is the `seq_no` of that candidate.
    pub fn on_response(
        &mut self,
        resp: BkSetResponse,
        supplier: &str,
        now: Instant,
    ) -> Result<Option<Evicted>, BkSetRejection> {
        let last = self.last_written.as_ref().map(|w| w.seq_no);
        let validated = validate(resp, last)?;
        let got = validated.seq_no();
        match self.queue.push(validated, supplier, now) {
            PushOutcome::Queued { evicted } => Ok(evicted),
            PushOutcome::NotNewerThanQueued => Err(BkSetRejection::NotNewer {
                got,
                last: self.queue.newest_seq_no().unwrap_or(got),
            }),
        }
    }

    /// Restarts the age clock of every queued candidate (see `CandidateQueue::restart_ages`).
    fn restart_queue_ages(&mut self, now: Instant) {
        self.queue.restart_ages(now);
    }

    /// Fails only when the anchor lookup fails; the queue is then left as it was. A snapshot the
    /// writer queue refused is `put_error` of the report, which keeps what else the tick found.
    pub fn resolve_once(
        &mut self,
        store: &impl LatestBkSetStore,
        now_unix_secs: u64,
        now: Instant,
    ) -> Result<ResolveReport, AnchorLookupError> {
        // Read before any put: a rotation finishing after this point is seen on the next tick,
        // so a write it dropped is repeated.
        let rotation_count = store.rotation_count();
        let resolution = self
            .queue
            .resolve(
                |t, s| store.find_anchor_block(t, s).context("anchor lookup failed"),
                |set, anchor| check_commitment(set, anchor.block_merkle_leaves.as_deref()),
                now,
            )
            .map_err(AnchorLookupError)?;
        let mut report = ResolveReport {
            rejected: resolution.rejected,
            expired: resolution.expired,
            ..Default::default()
        };
        if let Some((set, anchor)) = resolution.hit {
            let (thread_id, seq_no, signers) = set.into_parts();
            let item = ArchLatestBkSet {
                thread_id,
                seq_no,
                block_id: anchor.block_id,
                height: anchor.height,
                chain_order: anchor.chain_order,
                updated_at: now_unix_secs,
                signers,
            };
            match store.put_latest_bk_set(item.clone()) {
                Ok(()) => {
                    report.written = Some(item.seq_no);
                    self.last_written = Some(item);
                }
                Err(err) => {
                    report.put_error = Some(error_text(&err));
                    return Ok(report);
                }
            }
        }
        if report.written.is_none() && self.last_rotation_count != rotation_count {
            if let Some(item) = self.last_written.clone() {
                let seq_no = item.seq_no;
                match store.put_latest_bk_set(item) {
                    Ok(()) => report.rewritten = Some(seq_no),
                    Err(err) => {
                        // Not remembered: the rewrite is tried again on the next tick.
                        report.put_error = Some(error_text(&err));
                        return Ok(report);
                    }
                }
            }
        }
        self.last_rotation_count = rotation_count;
        Ok(report)
    }
}

/// The only error of `PollerState::resolve_once`. `Poller::on_tick` reads every `Err` as a failed
/// anchor lookup (candidates stop ageing, `bk_set_anchor`), so another fallible step in
/// `resolve_once` must report through `ResolveReport` (like `put_error`), not through `?`; with
/// this type a stray `?` on an `anyhow::Error` does not compile.
#[derive(Debug)]
pub struct AnchorLookupError(anyhow::Error);

impl std::fmt::Display for AnchorLookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl std::error::Error for AnchorLookupError {}

/// `None` means "do not count as an error".
fn rejection_error_kind(r: &BkSetRejection) -> Option<&'static str> {
    match r {
        // Normal after a failover to a lagging BK.
        BkSetRejection::NotNewer { .. } => None,
        BkSetRejection::MissingLeaves
        | BkSetRejection::CommitmentMismatch
        | BkSetRejection::CommitmentError(_) => Some(ERR_BK_SET_COMMITMENT),
        BkSetRejection::BadAnchor(_) => Some(ERR_BK_SET_ANCHOR),
        _ => Some(ERR_BK_SET_INVALID),
    }
}

fn truncate(s: &str, max_chars: usize) -> String {
    let cut = truncate_chars(s, max_chars);
    if cut.len() < s.len() {
        format!("{cut}...")
    } else {
        cut.to_string()
    }
}

/// Bounded error text with the cause chain: serde errors embed the offending input.
fn error_text(err: &anyhow::Error) -> String {
    truncate(&format!("{err:#}"), MAX_LOGGED_ERR_LEN)
}

/// Bounded text for logs: `InvalidThreadId` carries a string taken from the response.
fn describe(r: &BkSetRejection) -> String {
    match r {
        BkSetRejection::InvalidThreadId(s) => {
            format!("InvalidThreadId({:?})", truncate(s, MAX_LOGGED_STR_LEN))
        }
        other => format!("{other:?}"),
    }
}

/// A response of `endpoint` that has just been refused.
fn log_rejection(endpoint: &str, r: &BkSetRejection) {
    let text = describe(r);
    if rejection_error_kind(r).is_some() {
        tracing::warn!(endpoint, "bk_set response rejected: {text}");
    } else {
        tracing::debug!(endpoint, "bk_set response skipped: {text}");
    }
}

/// A queued candidate of `supplier` that failed the check against its anchor block.
fn log_candidate_rejection(seq_no: u64, supplier: &str, r: &BkSetRejection) {
    let text = describe(r);
    if rejection_error_kind(r).is_some() {
        tracing::warn!(seq_no, supplier, "bk_set candidate rejected: {text}");
    } else {
        tracing::debug!(seq_no, supplier, "bk_set candidate skipped: {text}");
    }
}

fn check_body_len(content_length: Option<u64>) -> anyhow::Result<()> {
    match content_length {
        Some(len) if len > MAX_RESPONSE_BYTES as u64 => {
            anyhow::bail!("response is {len} bytes, the limit is {MAX_RESPONSE_BYTES}")
        }
        _ => Ok(()),
    }
}

/// Reads the body, giving up as soon as it exceeds `MAX_RESPONSE_BYTES`.
async fn read_limited_body(mut resp: reqwest::Response) -> anyhow::Result<Vec<u8>> {
    check_body_len(resp.content_length())?;
    let mut body = Vec::new();
    while let Some(chunk) = resp.chunk().await? {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            anyhow::bail!("response body exceeds the limit of {MAX_RESPONSE_BYTES} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Tries the endpoints starting from `start_idx` (wrapping around) and returns the one that
/// answered. The endpoint list is read once, so a reload during the fetch does not change the
/// answer. Does not touch the pool's default endpoint.
async fn fetch_bk_set(
    app_state: &AppState,
    client: &reqwest::Client,
    start_idx: usize,
) -> anyhow::Result<FetchedBkSet> {
    let endpoints = app_state.bk_api_pool.endpoints();
    if endpoints.is_empty() {
        anyhow::bail!("no BK API endpoints configured");
    }
    let len = endpoints.len();
    let mut last_error = None;
    for step in 0..len {
        let idx = (start_idx % len + step) % len;
        let endpoint = &endpoints[idx];
        let url = format!("http://{endpoint}/v2/bk_set");
        let result: anyhow::Result<BkSetResponse> = async {
            let resp = client
                .get(&url)
                .bearer_auth(&app_state.bk_api_token)
                .timeout(FETCH_TIMEOUT)
                .send()
                .await?;
            let status = resp.status();
            if !status.is_success() {
                anyhow::bail!("status {status}");
            }
            let body = read_limited_body(resp).await?;
            parse_response(&body)
        }
        .await;
        match result {
            Ok(response) => {
                return Ok(FetchedBkSet {
                    idx,
                    endpoint: endpoint.to_string(),
                    failed_over: step > 0,
                    response,
                });
            }
            Err(err) => {
                let text = error_text(&err);
                // The last failure is reported by the caller through the returned error.
                if step + 1 < len {
                    tracing::warn!("bk_set from {endpoint} failed, trying next: {text}");
                }
                last_error = Some(format!("{endpoint}: {text}"));
            }
        }
    }
    anyhow::bail!("all BK API endpoints failed, last: {}", last_error.unwrap_or_default())
}

fn now_unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Why the poller moves to the next BK API endpoint; it is logged with the move.
#[derive(Clone, Debug, PartialEq)]
pub enum AdvanceReason {
    /// `STALL_POLLS` polls in a row were not newer; `got` and `last` are those of the last one
    /// (see `BkSetRejection::NotNewer`).
    NotNewer { got: u64, last: u64 },
    /// The response was refused for a reason other than being not newer.
    InvalidResponse(String),
    /// A queued candidate of the current endpoint failed the check against its anchor block.
    CandidateRejected { seq_no: u64, reason: String },
    /// A candidate of the current endpoint waited too long for its anchor block.
    Expired { seq_no: u64 },
    /// A candidate of the current endpoint was pushed out of the full queue.
    Evicted { seq_no: u64 },
}

impl std::fmt::Display for AdvanceReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotNewer { got, last } => {
                write!(f, "not newer {STALL_POLLS} polls in a row (got {got}, last {last})")
            }
            Self::InvalidResponse(reason) => write!(f, "invalid response: {reason}"),
            Self::CandidateRejected { seq_no, reason } => {
                write!(f, "candidate {seq_no} rejected: {reason}")
            }
            Self::Expired { seq_no } => write!(f, "candidate {seq_no} expired"),
            Self::Evicted { seq_no } => write!(f, "candidate {seq_no} evicted"),
        }
    }
}

/// Something the poller wants done outside of its own state; `run` executes it.
#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    /// Count `bm_errors{kind}`.
    Error(&'static str),
    /// Set `bm_latest_bk_set_seqno`.
    Gauge { seq_no: u64, thread_id: String },
    /// Move the cursor from endpoint `from` to the next one by calling `Poller::advance`. The
    /// caller must do that before it calls the next handler: `advance` is also where the stall
    /// count restarts, and a handler asking for a move does not reset it.
    Advance { from: String, reason: AdvanceReason },
}

/// An answer of the BK API: the endpoint that gave it and the parsed response.
pub struct FetchedBkSet {
    pub idx: usize,
    pub endpoint: String,
    /// The endpoint under the cursor failed and a later one answered.
    pub failed_over: bool,
    pub response: BkSetResponse,
}

/// The poller without I/O: handlers take what happened and return what to do about it.
pub struct Poller {
    state: PollerState,
    cursor: usize,
    /// Consecutive not newer answers since the cursor last moved. It cannot tell a stuck BK from
    /// a halted chain: while the chain is halted every BK answers not newer and the cursor keeps
    /// rotating.
    not_newer: u32,
    writes: LatestBkSetWriteCounters,
    /// The last resolve tick failed on the anchor lookup. Candidate age does not count meanwhile
    /// (every failed tick restarts it), so a capacity eviction says nothing about the BK and
    /// must not move the cursor.
    lookup_failing: bool,
}

impl Poller {
    /// Restores the snapshot stored in the DB. A snapshot that cannot be read is an `Error`
    /// effect, and the poller starts empty rather than giving up.
    pub fn start(store: &impl LatestBkSetStore, poll_interval: Duration) -> (Self, Vec<Effect>) {
        let mut effects = Vec::new();
        let restored = match store.load_latest_bk_set() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::warn!(
                    "cannot read latest_bk_set, starting without it: {}",
                    error_text(&err)
                );
                effects.push(Effect::Error(ERR_BK_SET_RESTORE));
                None
            }
        };
        if let Some(snapshot) = &restored {
            tracing::info!(seq_no = snapshot.seq_no, "latest_bk_set restored from the DB");
            effects.push(Effect::Gauge {
                seq_no: snapshot.seq_no,
                thread_id: snapshot.thread_id.clone(),
            });
        }
        let poller = Self {
            state: PollerState::with_capacity(queue_capacity(poll_interval), restored),
            cursor: 0,
            not_newer: 0,
            writes: LatestBkSetWriteCounters::default(),
            lookup_failing: false,
        };
        (poller, effects)
    }

    /// Index of the endpoint to fetch from first.
    fn cursor(&self) -> usize {
        self.cursor
    }

    /// Moves the cursor to the next of `endpoints` and returns that endpoint (empty when there
    /// are none). The caller executes an `Effect::Advance` with this before the next handler
    /// call; this is where such a move restarts the stall count. A failover inside a fetch moves
    /// the cursor without an `Advance`, and `on_fetch` restarts the count for it.
    fn advance(&mut self, endpoints: &[String]) -> String {
        self.cursor = next_cursor(self.cursor, endpoints.len());
        self.not_newer = 0;
        endpoints.get(self.cursor).cloned().unwrap_or_default()
    }

    /// The outcome of one poll. Validation decodes up to 299 BLS keys: run it in `block_in_place`.
    pub fn on_fetch(&mut self, result: anyhow::Result<FetchedBkSet>, now: Instant) -> Vec<Effect> {
        let fetched = match result {
            Ok(fetched) => fetched,
            Err(err) => {
                tracing::warn!("failed to fetch bk_set: {}", error_text(&err));
                return vec![Effect::Error(ERR_BK_SET_POLL)];
            }
        };
        let mut effects = Vec::new();
        if fetched.failed_over {
            // Count from scratch on the new BK.
            self.not_newer = 0;
        }
        self.cursor = fetched.idx;
        let endpoint = fetched.endpoint;
        let advance = match self.state.on_response(fetched.response, &endpoint, now) {
            Ok(None) => {
                self.not_newer = 0;
                None
            }
            Ok(Some(evicted)) => {
                self.not_newer = 0;
                tracing::warn!(
                    seq_no = evicted.seq_no,
                    supplier = evicted.supplier,
                    endpoint,
                    "bk set candidate evicted before its anchor block appeared"
                );
                effects.push(Effect::Error(ERR_BK_SET_EVICTED));
                // The cursor still points at the supplier: the endpoint keeps returning `seq_no`s
                // BM never stores (for instance a node of another network), so leave it.
                (!self.lookup_failing && evicted.supplier == endpoint)
                    .then_some(AdvanceReason::Evicted { seq_no: evicted.seq_no })
            }
            Err(r) => {
                log_rejection(&endpoint, &r);
                if let Some(kind) = rejection_error_kind(&r) {
                    effects.push(Effect::Error(kind));
                }
                match r {
                    BkSetRejection::NotNewer { got, last } => {
                        self.not_newer += 1;
                        (self.not_newer >= STALL_POLLS)
                            .then_some(AdvanceReason::NotNewer { got, last })
                    }
                    other => Some(AdvanceReason::InvalidResponse(describe(&other))),
                }
            }
        };
        if let Some(reason) = advance {
            effects.push(Effect::Advance { from: endpoint, reason });
        }
        effects
    }

    /// One resolve tick: matches queued candidates with the archive. Reads the DB: run it in
    /// `block_in_place`. `endpoints` is the current list, to tell who the cursor points at.
    pub fn on_tick(
        &mut self,
        store: &impl LatestBkSetStore,
        endpoints: &[String],
        now: Instant,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        match self.state.resolve_once(store, now_unix_secs(), now) {
            // `resolve_once` fails only on the anchor lookup (see `AnchorLookupError`).
            Err(AnchorLookupError(err)) => {
                self.lookup_failing = true;
                // Candidates cannot be checked, so they do not age: after the outage they get
                // the full `max_age` again instead of expiring on the first good tick.
                self.state.restart_queue_ages(now);
                tracing::warn!("failed to update latest_bk_set: {}", error_text(&err));
                effects.push(Effect::Error(ERR_BK_SET_ANCHOR));
            }
            Ok(report) => {
                self.lookup_failing = false;
                for (seq_no, r, supplier) in &report.rejected {
                    log_candidate_rejection(*seq_no, supplier, r);
                    if let Some(kind) = rejection_error_kind(r) {
                        effects.push(Effect::Error(kind));
                    }
                }
                for (seq_no, supplier) in &report.expired {
                    tracing::warn!(
                        seq_no,
                        supplier,
                        "bk set candidate expired before its anchor block appeared"
                    );
                    effects.push(Effect::Error(ERR_BK_SET_EVICTED));
                }
                if let Some(err) = &report.put_error {
                    tracing::warn!("failed to queue latest_bk_set: {err}");
                    effects.push(Effect::Error(ERR_BK_SET_STORE));
                }
                if let Some(seq_no) = report.written {
                    tracing::debug!(seq_no, "latest_bk_set queued for writing");
                }
                if let Some(seq_no) = report.rewritten {
                    tracing::info!(seq_no, "latest_bk_set queued for rewriting after DB rotation");
                }
                let current =
                    cursor_index(self.cursor, endpoints.len()).and_then(|i| endpoints.get(i));
                if let Some(current) = current {
                    if let Some(reason) = advance_reason(&report, current) {
                        effects.push(Effect::Advance { from: current.clone(), reason });
                    }
                }
            }
        }
        // Metrics follow what the writer thread confirmed, not what was queued: a write
        // queued on this tick is usually reported on the next one.
        let writes = store.latest_bk_set_writes();
        let failed = writes.failures.saturating_sub(self.writes.failures);
        if failed > 0 {
            tracing::warn!(failed, "latest_bk_set write failed in the sqlite writer");
            effects.extend((0..failed).map(|_| Effect::Error(ERR_BK_SET_STORE)));
        }
        let seq_no = writes.written_seq_no;
        if seq_no != self.writes.written_seq_no && seq_no != 0 {
            if let Some(w) = &self.state.last_written {
                tracing::info!(seq_no, "latest_bk_set written");
                effects.push(Effect::Gauge { seq_no, thread_id: w.thread_id.clone() });
            }
        }
        self.writes = writes;
        effects
    }
}

fn endpoint_list(app_state: &AppState) -> Vec<String> {
    app_state.bk_api_pool.endpoints().iter().map(ToString::to_string).collect()
}

/// What `execute` reports to; `Option<Metrics>` is the production implementation.
trait PollerMetrics {
    fn report_errors(&self, kind: &'static str);
    fn report_latest_bk_set_seqno(&self, seq_no: u64, thread_id: &str);
}

impl PollerMetrics for Option<Metrics> {
    fn report_errors(&self, kind: &'static str) {
        if let Some(m) = self {
            m.bm.report_errors(kind);
        }
    }

    fn report_latest_bk_set_seqno(&self, seq_no: u64, thread_id: &str) {
        if let Some(m) = self {
            m.bm.report_latest_bk_set_seqno(seq_no, thread_id);
        }
    }
}

/// `endpoints` is called only for `Advance`.
fn execute(
    effects: Vec<Effect>,
    poller: &mut Poller,
    endpoints: impl Fn() -> Vec<String>,
    metrics: &impl PollerMetrics,
) {
    for effect in effects {
        match effect {
            Effect::Error(kind) => metrics.report_errors(kind),
            Effect::Gauge { seq_no, thread_id } => {
                metrics.report_latest_bk_set_seqno(seq_no, &thread_id)
            }
            Effect::Advance { from, reason } => {
                let to = poller.advance(&endpoints());
                tracing::warn!(from, to, %reason, "bk_set: moving to the next BK API endpoint");
                metrics.report_errors(ERR_BK_SET_STALLED);
            }
        }
    }
}

pub async fn run(
    app_state: Arc<AppState>,
    store: SqliteHelper,
    metrics: Option<Metrics>,
    poll_interval: Duration,
) {
    let client = reqwest::Client::new();
    let (mut poller, effects) =
        tokio::task::block_in_place(|| Poller::start(&store, poll_interval));
    execute(effects, &mut poller, || endpoint_list(&app_state), &metrics);
    let mut poll = tokio::time::interval(poll_interval);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut resolve = tokio::time::interval(RESOLVE_PERIOD);
    resolve.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = poll.tick() => {
                let result = fetch_bk_set(&app_state, &client, poller.cursor()).await;
                let effects = tokio::task::block_in_place(|| poller.on_fetch(result, Instant::now()));
                execute(effects, &mut poller, || endpoint_list(&app_state), &metrics);
            }
            _ = resolve.tick() => {
                let endpoints = endpoint_list(&app_state);
                let effects = tokio::task::block_in_place(|| {
                    poller.on_tick(&store, &endpoints, Instant::now())
                });
                execute(effects, &mut poller, || endpoint_list(&app_state), &metrics);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::HashMap;

    use database::sqlite::AnchorBlock;
    use node::block_keeper_system::bk_set_poseidon_commitment;
    use serde_json::json;

    use super::*;
    use crate::domain::latest_bk_set::L2_RANGE;
    use crate::domain::latest_bk_set::MERKLE_LEAVES_LEN;

    const SUPPLIER: &str = "bk-a:8600";

    fn tid() -> String {
        format!("00{}", "ab".repeat(33))
    }

    fn key(i: u32) -> [u8; 48] {
        let mut ikm = [7u8; 32];
        ikm[..4].copy_from_slice(&i.to_le_bytes());
        let sk = gosh_blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap();
        sk.sk_to_pk().to_bytes()
    }

    fn signers() -> Vec<(u16, [u8; 48])> {
        vec![(0, key(1)), (3, key(2))]
    }

    fn response(seq_no: u64) -> BkSetResponse {
        let bk_set: Vec<_> = signers()
            .iter()
            .map(|(i, k)| json!({"signer_index": i, "pubkey": hex::encode(k)}))
            .collect();
        let v = json!({"seq_no": seq_no, "thread_id": tid(), "bk_set": bk_set});
        parse_response(v.to_string().as_bytes()).unwrap()
    }

    fn leaves(signers: &[(u16, [u8; 48])]) -> Vec<u8> {
        let mut l = vec![0u8; MERKLE_LEAVES_LEN];
        let c = bk_set_poseidon_commitment(signers).unwrap();
        l[L2_RANGE].copy_from_slice(c.as_slice());
        l
    }

    fn anchor(seq_no: u64, good: bool) -> AnchorBlock {
        let l = if good { leaves(&signers()) } else { leaves(&[(0, key(9))]) };
        AnchorBlock {
            block_id: format!("block-{seq_no}"),
            height: seq_no.to_be_bytes(),
            chain_order: format!("order-{seq_no}"),
            block_merkle_leaves: Some(l),
        }
    }

    #[derive(Default)]
    struct FakeStore {
        anchors: RefCell<HashMap<u64, AnchorBlock>>,
        puts: RefCell<Vec<ArchLatestBkSet>>,
        rotations: RefCell<u64>,
        /// One-shot: a rotation completes right after the next put (which it may have dropped).
        rotate_on_put: Cell<bool>,
        written_seq_no: Cell<u64>,
        write_failures: Cell<u64>,
        lookup_fails: Cell<bool>,
        db_snapshot: RefCell<Option<ArchLatestBkSet>>,
        load_fails: Cell<bool>,
        put_fails: Cell<bool>,
    }

    impl FakeStore {
        fn add_block(&self, seq_no: u64, good: bool) {
            self.anchors.borrow_mut().insert(seq_no, anchor(seq_no, good));
        }
    }

    impl LatestBkSetStore for FakeStore {
        fn find_anchor_block(&self, _: &str, seq_no: u64) -> anyhow::Result<AnchorLookup> {
            if self.lookup_fails.get() {
                anyhow::bail!("db is locked");
            }
            Ok(self
                .anchors
                .borrow()
                .get(&seq_no)
                .cloned()
                .map_or(AnchorLookup::Missing, AnchorLookup::Found))
        }

        fn load_latest_bk_set(&self) -> anyhow::Result<Option<ArchLatestBkSet>> {
            if self.load_fails.get() {
                anyhow::bail!("db is locked");
            }
            Ok(self.db_snapshot.borrow().clone())
        }

        fn put_latest_bk_set(&self, item: ArchLatestBkSet) -> anyhow::Result<()> {
            if self.put_fails.get() {
                anyhow::bail!("writer channel closed");
            }
            self.puts.borrow_mut().push(item);
            if self.rotate_on_put.take() {
                *self.rotations.borrow_mut() += 1;
            }
            Ok(())
        }

        fn rotation_count(&self) -> u64 {
            *self.rotations.borrow()
        }

        fn latest_bk_set_writes(&self) -> LatestBkSetWriteCounters {
            LatestBkSetWriteCounters {
                written_seq_no: self.written_seq_no.get(),
                failures: self.write_failures.get(),
            }
        }
    }

    fn stored_snapshot(seq_no: u64) -> ArchLatestBkSet {
        ArchLatestBkSet {
            thread_id: tid(),
            seq_no,
            block_id: format!("block-{seq_no}"),
            height: seq_no.to_be_bytes(),
            chain_order: format!("order-{seq_no}"),
            updated_at: 5,
            signers: signers(),
        }
    }

    fn state() -> PollerState {
        PollerState::with_capacity(CANDIDATE_QUEUE_CAPACITY, None)
    }

    fn restored(seq_no: u64) -> PollerState {
        PollerState::with_capacity(CANDIDATE_QUEUE_CAPACITY, Some(stored_snapshot(seq_no)))
    }

    fn written_state(store: &FakeStore, seq_no: u64, t: Instant) -> PollerState {
        let mut st = state();
        store.add_block(seq_no, true);
        st.on_response(response(seq_no), SUPPLIER, t).unwrap();
        assert_eq!(st.resolve_once(store, 1, t).unwrap().written, Some(seq_no));
        st
    }

    #[test]
    fn steady_lag_still_writes() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), SUPPLIER, t).unwrap();
        assert_eq!(st.resolve_once(&store, 1, t).unwrap(), ResolveReport::default());
        store.add_block(100, true);
        st.on_response(response(160), SUPPLIER, t).unwrap();
        let r = st.resolve_once(&store, 2, t).unwrap();
        assert_eq!(r.written, Some(100));
        store.add_block(160, true);
        let r = st.resolve_once(&store, 3, t).unwrap();
        assert_eq!(r.written, Some(160));
        let puts = store.puts.borrow();
        assert_eq!(puts.len(), 2);
        assert_eq!(puts[1].block_id, "block-160");
        assert_eq!(puts[1].updated_at, 3);
        assert_eq!(puts[1].signers, signers());
    }

    #[test]
    fn missing_block_is_superseded() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), SUPPLIER, t).unwrap();
        st.on_response(response(160), SUPPLIER, t).unwrap();
        store.add_block(160, true);
        assert_eq!(st.resolve_once(&store, 1, t).unwrap().written, Some(160));
        // 100 was dropped: it must not be written even if its block shows up later
        store.add_block(100, true);
        assert_eq!(st.resolve_once(&store, 2, t).unwrap(), ResolveReport::default());
        assert_eq!(store.puts.borrow().len(), 1);
    }

    #[test]
    fn commitment_mismatch_is_rejected_and_not_written() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), SUPPLIER, t).unwrap();
        store.add_block(100, false);
        let r = st.resolve_once(&store, 1, t).unwrap();
        assert_eq!(r.written, None);
        assert_eq!(
            r.rejected,
            vec![(100, BkSetRejection::CommitmentMismatch, SUPPLIER.to_string())]
        );
        assert!(store.puts.borrow().is_empty());
    }

    #[test]
    fn mismatch_on_newest_falls_back_to_older_valid_candidate() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), SUPPLIER, t).unwrap();
        st.on_response(response(160), SUPPLIER, t).unwrap();
        store.add_block(100, true);
        store.add_block(160, false);
        let r = st.resolve_once(&store, 1, t).unwrap();
        assert_eq!(r.written, Some(100));
        assert_eq!(
            r.rejected,
            vec![(160, BkSetRejection::CommitmentMismatch, SUPPLIER.to_string())]
        );
    }

    #[test]
    fn rewrites_once_after_rotation() {
        let store = FakeStore::default();
        let t = Instant::now();
        let mut st = written_state(&store, 100, t);
        *store.rotations.borrow_mut() = 1;
        let r = st.resolve_once(&store, 2, t).unwrap();
        assert_eq!(r.rewritten, Some(100));
        assert_eq!(store.puts.borrow().len(), 2);
        assert_eq!(store.puts.borrow()[0], store.puts.borrow()[1]);
        let r = st.resolve_once(&store, 3, t).unwrap();
        assert_eq!(r, ResolveReport::default());
        assert_eq!(store.puts.borrow().len(), 2);
    }

    #[test]
    fn rotation_right_after_a_write_triggers_a_rewrite() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        store.add_block(100, true);
        st.on_response(response(100), SUPPLIER, t).unwrap();
        store.rotate_on_put.set(true);
        assert_eq!(st.resolve_once(&store, 1, t).unwrap().written, Some(100));
        let r = st.resolve_once(&store, 2, t).unwrap();
        assert_eq!(r.rewritten, Some(100));
        assert_eq!(store.puts.borrow().len(), 2);
    }

    #[test]
    fn no_rewrite_without_rotation() {
        let store = FakeStore::default();
        let t = Instant::now();
        let mut st = written_state(&store, 100, t);
        assert_eq!(st.resolve_once(&store, 2, t).unwrap(), ResolveReport::default());
        assert_eq!(store.puts.borrow().len(), 1);
    }

    #[test]
    fn not_newer_response_is_rejected() {
        let store = FakeStore::default();
        let t = Instant::now();
        let mut st = written_state(&store, 160, t);
        assert_eq!(
            st.on_response(response(150), SUPPLIER, t),
            Err(BkSetRejection::NotNewer { got: 150, last: 160 })
        );
    }

    #[test]
    fn rejection_error_kind_maps_every_variant() {
        use BkSetRejection as R;
        assert_eq!(rejection_error_kind(&R::NotNewer { got: 1, last: 2 }), None);
        for r in [R::MissingLeaves, R::CommitmentMismatch, R::CommitmentError("x".into())] {
            assert_eq!(rejection_error_kind(&r), Some("bk_set_commitment"), "{r:?}");
        }
        assert_eq!(rejection_error_kind(&R::BadAnchor("x".into())), Some("bk_set_anchor"));
        for r in [
            R::MissingThreadId,
            R::InvalidThreadId("x".into()),
            R::Empty,
            R::TooManySigners(300),
            R::SignerIndexOutOfRange(70000),
            R::DuplicateSignerIndex(1),
            R::InvalidPubkey(1),
            R::SeqNoOutOfRange(u64::MAX),
        ] {
            assert_eq!(rejection_error_kind(&r), Some("bk_set_invalid"), "{r:?}");
        }
    }

    #[test]
    fn restored_snapshot_rejects_older_responses() {
        let mut st = restored(100);
        let t = Instant::now();
        assert_eq!(
            st.on_response(response(100), SUPPLIER, t),
            Err(BkSetRejection::NotNewer { got: 100, last: 100 })
        );
        assert_eq!(
            st.on_response(response(99), SUPPLIER, t),
            Err(BkSetRejection::NotNewer { got: 99, last: 100 })
        );
        assert_eq!(st.on_response(response(101), SUPPLIER, t), Ok(None));
        assert_eq!(st.queue.len(), 1);
        // Nothing restored: anything is accepted.
        assert_eq!(state().on_response(response(1), SUPPLIER, t), Ok(None));
    }

    #[test]
    fn restored_snapshot_is_replaced_by_a_new_write() {
        let store = FakeStore::default();
        let mut st = restored(10);
        let t = Instant::now();
        store.add_block(100, true);
        st.on_response(response(100), SUPPLIER, t).unwrap();
        assert_eq!(st.resolve_once(&store, 1, t).unwrap().written, Some(100));
        assert_eq!(
            st.on_response(response(50), SUPPLIER, t),
            Err(BkSetRejection::NotNewer { got: 50, last: 100 })
        );
    }

    #[test]
    fn restored_snapshot_is_rewritten_after_a_rotation_without_a_new_write() {
        let store = FakeStore::default();
        let mut st = restored(100);
        let t = Instant::now();
        // Nothing happened yet: nothing to rewrite.
        assert_eq!(st.resolve_once(&store, 2, t).unwrap(), ResolveReport::default());
        assert!(store.puts.borrow().is_empty());
        *store.rotations.borrow_mut() = 1;
        let r = st.resolve_once(&store, 3, t).unwrap();
        assert_eq!(r.rewritten, Some(100));
        assert_eq!(*store.puts.borrow(), vec![stored_snapshot(100)]);
        assert_eq!(st.resolve_once(&store, 4, t).unwrap(), ResolveReport::default());
    }

    #[test]
    fn rotation_before_the_first_tick_still_rewrites_the_restored_snapshot() {
        let store = FakeStore::default();
        let mut st = restored(100);
        *store.rotations.borrow_mut() = 1;
        assert_eq!(st.resolve_once(&store, 2, Instant::now()).unwrap().rewritten, Some(100));
    }

    #[test]
    fn capacity_eviction_of_an_unstored_candidate_reports_its_supplier() {
        let mut st = state();
        let t = Instant::now();
        for i in 0..CANDIDATE_QUEUE_CAPACITY as u64 {
            assert_eq!(st.on_response(response(200 + i), "bk-a:8600", t), Ok(None));
        }
        let ev = st.on_response(response(300), "bk-a:8600", t).unwrap().unwrap();
        assert_eq!(ev.supplier, "bk-a:8600");
    }

    #[test]
    fn failed_put_keeps_the_rejections_of_the_tick() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), "bk-a:8600", t).unwrap();
        st.on_response(response(160), "bk-b:8600", t).unwrap();
        store.add_block(100, true);
        store.add_block(160, false);
        store.put_fails.set(true);
        let r = st.resolve_once(&store, 1, t).unwrap();
        assert_eq!(r.written, None);
        assert_eq!(
            r.rejected,
            vec![(160, BkSetRejection::CommitmentMismatch, "bk-b:8600".to_string())]
        );
        assert!(r.put_error.as_deref().unwrap().contains("writer channel closed"));
    }

    #[test]
    fn repeated_seq_no_still_queued_counts_as_not_newer() {
        let mut st = state();
        let t = Instant::now();
        assert_eq!(st.on_response(response(100), SUPPLIER, t), Ok(None));
        // Same seq_no while the candidate waits for its block: no progress for the stall counter.
        let r = st.on_response(response(100), SUPPLIER, t);
        assert_eq!(r, Err(BkSetRejection::NotNewer { got: 100, last: 100 }));
        assert_eq!(st.queue.len(), 1);
    }

    #[test]
    fn expired_candidates_are_reported() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), SUPPLIER, t).unwrap();
        let r = st.resolve_once(&store, 1, t + CANDIDATE_MAX_AGE).unwrap();
        assert!(r.expired.is_empty());
        let r = st.resolve_once(&store, 2, t + CANDIDATE_MAX_AGE + Duration::from_secs(1)).unwrap();
        assert_eq!(r.expired, vec![(100, SUPPLIER.to_string())]);
        assert_eq!(r.written, None);
        assert_eq!(st.queue.len(), 0);
    }

    #[test]
    fn lookup_failure_is_an_error_and_keeps_the_queue() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), SUPPLIER, t).unwrap();
        store.lookup_fails.set(true);
        let AnchorLookupError(err) = st.resolve_once(&store, 1, t).unwrap_err();
        assert_eq!(error_text(&err), "anchor lookup failed: db is locked");
        assert_eq!(st.queue.len(), 1);
    }

    #[test]
    fn put_failure_is_reported_not_returned_as_an_error() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        store.add_block(100, true);
        st.on_response(response(100), SUPPLIER, t).unwrap();
        store.put_fails.set(true);
        let r = st.resolve_once(&store, 1, t).unwrap();
        assert_eq!(r.written, None);
        assert!(r.put_error.is_some());
    }

    #[test]
    fn rejection_is_attributed_to_the_supplier() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), "bk-a:8600", t).unwrap();
        st.on_response(response(160), "bk-b:8600", t).unwrap();
        store.add_block(100, true);
        store.add_block(160, false);
        let r = st.resolve_once(&store, 1, t).unwrap();
        assert_eq!(r.written, Some(100));
        assert_eq!(
            r.rejected,
            vec![(160, BkSetRejection::CommitmentMismatch, "bk-b:8600".to_string())]
        );
        // Advance only when the cursor still points at a supplier of a rejected candidate.
        assert_eq!(
            advance_reason(&r, "bk-b:8600"),
            Some(AdvanceReason::CandidateRejected {
                seq_no: 160,
                reason: "CommitmentMismatch".to_string()
            })
        );
        assert_eq!(advance_reason(&r, "bk-a:8600"), None);
        assert_eq!(advance_reason(&ResolveReport::default(), "bk-b:8600"), None);
    }

    #[test]
    fn expiry_is_attributed_and_overflow_names_the_evicted_supplier() {
        let store = FakeStore::default();
        let mut st = state();
        let t = Instant::now();
        st.on_response(response(100), "bk-a:8600", t).unwrap();
        let r = st.resolve_once(&store, 1, t + CANDIDATE_MAX_AGE * 2).unwrap();
        assert_eq!(r.expired, vec![(100, "bk-a:8600".to_string())]);
        assert_eq!(advance_reason(&r, "bk-a:8600"), Some(AdvanceReason::Expired { seq_no: 100 }));
        assert_eq!(advance_reason(&r, "bk-b:8600"), None);

        let mut st = state();
        for i in 0..CANDIDATE_QUEUE_CAPACITY as u64 {
            assert_eq!(st.on_response(response(200 + i), "bk-a:8600", t), Ok(None));
        }
        let ev = st.on_response(response(300), "bk-b:8600", t).unwrap().unwrap();
        assert_eq!(ev, Evicted { seq_no: 200, supplier: "bk-a:8600".to_string() });
        // Ignored as not newer than the queue.
        assert!(st.on_response(response(300), "bk-c:8600", t).is_err());
        assert_eq!(st.queue.len(), CANDIDATE_QUEUE_CAPACITY);
    }

    #[test]
    fn cursor_index_tolerates_a_shrunk_list() {
        assert_eq!(cursor_index(1, 3), Some(1));
        assert_eq!(cursor_index(7, 3), Some(1));
        assert_eq!(cursor_index(0, 0), None);
    }

    #[test]
    fn body_limit_rejects_oversize() {
        assert!(check_body_len(Some(MAX_RESPONSE_BYTES as u64 + 1)).is_err());
        assert!(check_body_len(Some(MAX_RESPONSE_BYTES as u64)).is_ok());
        assert!(check_body_len(None).is_ok());
    }

    #[test]
    fn cursor_wraps_and_survives_a_shrunk_endpoint_list() {
        assert_eq!(next_cursor(0, 3), 1);
        assert_eq!(next_cursor(2, 3), 0);
        assert_eq!(next_cursor(0, 1), 0);
        // The list shrank on a config reload.
        assert_eq!(next_cursor(7, 3), 2);
        assert_eq!(next_cursor(5, 0), 0);
    }

    #[test]
    fn logged_thread_id_is_bounded() {
        let text = describe(&BkSetRejection::InvalidThreadId("x".repeat(10_000)));
        assert!(text.len() < 200);
    }

    #[test]
    fn logged_error_has_cause_chain_and_is_bounded() {
        let err = anyhow::anyhow!("inner cause").context("outer");
        assert_eq!(error_text(&err), "outer: inner cause");
        let long = anyhow::anyhow!("invalid type: string \"{}\"", "x".repeat(10_000));
        assert!(error_text(&long).chars().count() <= MAX_LOGGED_ERR_LEN + 3);
    }

    const A: &str = "bk-a:8600";
    const B: &str = "bk-b:8600";

    fn endpoints() -> Vec<String> {
        vec![A.to_string(), B.to_string()]
    }

    /// A fetch answered by endpoint `idx`, the one under the cursor unless `failed_over`.
    fn fetched(idx: usize, failed_over: bool, seq_no: u64) -> anyhow::Result<FetchedBkSet> {
        let endpoint = endpoints()[idx].clone();
        Ok(FetchedBkSet { idx, endpoint, failed_over, response: response(seq_no) })
    }

    fn start(store: &FakeStore) -> Poller {
        let (poller, effects) = Poller::start(store, Duration::from_secs(60));
        assert!(effects.is_empty());
        poller
    }

    fn advance(from: &str, reason: AdvanceReason) -> Effect {
        Effect::Advance { from: from.to_string(), reason }
    }

    fn not_newer_reason(got: u64, last: u64) -> AdvanceReason {
        AdvanceReason::NotNewer { got, last }
    }

    #[test]
    fn failover_resets_stall_counter() {
        let store = FakeStore::default();
        *store.db_snapshot.borrow_mut() = Some(stored_snapshot(100));
        let (mut p, _) = Poller::start(&store, Duration::from_secs(60));
        let now = Instant::now();
        assert!(p.on_fetch(fetched(0, false, 50), now).is_empty());
        assert!(p.on_fetch(fetched(0, false, 50), now).is_empty());
        // Failed over to B inside the fetch: counting starts from scratch.
        assert!(p.on_fetch(fetched(1, true, 50), now).is_empty());
        assert_eq!(p.cursor(), 1);
        assert!(p.on_fetch(fetched(1, false, 50), now).is_empty());
        assert_eq!(
            p.on_fetch(fetched(1, false, 50), now),
            vec![advance(B, not_newer_reason(50, 100))]
        );
    }

    #[test]
    fn three_not_newer_advance_with_reason() {
        let store = FakeStore::default();
        *store.db_snapshot.borrow_mut() = Some(stored_snapshot(100));
        let (mut p, _) = Poller::start(&store, Duration::from_secs(60));
        let now = Instant::now();
        assert!(p.on_fetch(fetched(0, false, 90), now).is_empty());
        assert!(p.on_fetch(fetched(0, false, 95), now).is_empty());
        assert_eq!(
            p.on_fetch(fetched(0, false, 99), now),
            vec![advance(A, not_newer_reason(99, 100))]
        );
        assert_eq!(p.advance(&endpoints()), B);
        assert_eq!(p.cursor(), 1);
        // The count restarts on the next endpoint.
        assert!(p.on_fetch(fetched(1, false, 90), now).is_empty());
        assert!(p.on_fetch(fetched(1, false, 90), now).is_empty());
        assert_eq!(
            p.on_fetch(fetched(1, false, 90), now),
            vec![advance(B, not_newer_reason(90, 100))]
        );
    }

    #[test]
    fn progress_and_a_move_for_another_reason_reset_the_stall_count() {
        let store = FakeStore::default();
        *store.db_snapshot.borrow_mut() = Some(stored_snapshot(100));
        let (mut p, _) = Poller::start(&store, Duration::from_secs(60));
        let now = Instant::now();
        assert!(p.on_fetch(fetched(0, false, 50), now).is_empty());
        assert!(p.on_fetch(fetched(0, false, 50), now).is_empty());
        // Progress: two more not newer answers are not enough.
        assert!(p.on_fetch(fetched(0, false, 101), now).is_empty());
        assert!(p.on_fetch(fetched(0, false, 50), now).is_empty());
        assert!(p.on_fetch(fetched(0, false, 50), now).is_empty());
        // The cursor moves for another reason (as on an expired candidate): count from scratch.
        assert_eq!(p.advance(&endpoints()), B);
        assert!(p.on_fetch(fetched(1, false, 50), now).is_empty());
        assert!(p.on_fetch(fetched(1, false, 50), now).is_empty());
        assert_eq!(
            p.on_fetch(fetched(1, false, 50), now),
            vec![advance(B, not_newer_reason(50, 100))]
        );
    }

    #[test]
    fn fetch_error_is_a_poll_error() {
        let store = FakeStore::default();
        let mut p = start(&store);
        assert_eq!(
            p.on_fetch(Err(anyhow::anyhow!("all down")), Instant::now()),
            vec![Effect::Error(ERR_BK_SET_POLL)]
        );
    }

    #[test]
    fn invalid_response_advances_with_reason() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let mut bad = response(100);
        bad.bk_set.clear();
        let r =
            Ok(FetchedBkSet { idx: 0, endpoint: A.to_string(), failed_over: false, response: bad });
        assert_eq!(
            p.on_fetch(r, Instant::now()),
            vec![
                Effect::Error(ERR_BK_SET_INVALID),
                advance(A, AdvanceReason::InvalidResponse("Empty".to_string()))
            ]
        );
    }

    #[test]
    fn executed_advance_after_an_invalid_response_restarts_the_stall_count() {
        let store = FakeStore::default();
        *store.db_snapshot.borrow_mut() = Some(stored_snapshot(100));
        let (mut p, _) = Poller::start(&store, Duration::from_secs(60));
        let now = Instant::now();
        assert!(p.on_fetch(fetched(0, false, 50), now).is_empty());
        assert!(p.on_fetch(fetched(0, false, 50), now).is_empty());
        let mut bad = response(101);
        bad.bk_set.clear();
        let r =
            Ok(FetchedBkSet { idx: 0, endpoint: A.to_string(), failed_over: false, response: bad });
        let effects = p.on_fetch(r, now);
        assert_eq!(
            effects.last(),
            Some(&advance(A, AdvanceReason::InvalidResponse("Empty".to_string())))
        );
        execute(effects, &mut p, endpoints, &None);
        assert_eq!(p.cursor(), 1);
        // The two not newer answers before the move do not count on the new endpoint.
        assert!(p.on_fetch(fetched(1, false, 50), now).is_empty());
        assert!(p.on_fetch(fetched(1, false, 50), now).is_empty());
        assert_eq!(
            p.on_fetch(fetched(1, false, 50), now),
            vec![advance(B, not_newer_reason(50, 100))]
        );
    }

    #[test]
    fn eviction_of_current_supplier_advances() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let now = Instant::now();
        for i in 0..CANDIDATE_QUEUE_CAPACITY as u64 {
            assert!(p.on_fetch(fetched(0, false, 200 + i), now).is_empty());
        }
        assert_eq!(
            p.on_fetch(fetched(0, false, 300), now),
            vec![
                Effect::Error(ERR_BK_SET_EVICTED),
                advance(A, AdvanceReason::Evicted { seq_no: 200 })
            ]
        );
    }

    #[test]
    fn eviction_of_another_supplier_does_not_advance() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let now = Instant::now();
        for i in 0..CANDIDATE_QUEUE_CAPACITY as u64 {
            assert!(p.on_fetch(fetched(0, false, 200 + i), now).is_empty());
        }
        // The cursor moved to B; the evicted candidate was supplied by A.
        assert_eq!(p.on_fetch(fetched(1, true, 300), now), vec![Effect::Error(ERR_BK_SET_EVICTED)]);
    }

    #[test]
    fn lookup_error_does_not_expire_or_advance() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let t = Instant::now();
        assert!(p.on_fetch(fetched(0, false, 100), t).is_empty());
        store.lookup_fails.set(true);
        for minutes in [1u64, 11, 25] {
            let now = t + Duration::from_secs(minutes * 60);
            assert_eq!(
                p.on_tick(&store, &endpoints(), now),
                vec![Effect::Error(ERR_BK_SET_ANCHOR)]
            );
        }
        // The queue is intact: once lookups work again the candidate is written.
        store.lookup_fails.set(false);
        store.add_block(100, true);
        let now = t + Duration::from_secs(26 * 60);
        assert!(p.on_tick(&store, &endpoints(), now).is_empty());
        assert_eq!(store.puts.borrow().len(), 1);
    }

    #[test]
    fn lookup_outage_does_not_age_candidates() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let t = Instant::now();
        let min = |m: u64| Duration::from_secs(m * 60);
        assert!(p.on_fetch(fetched(0, false, 100), t).is_empty());
        store.lookup_fails.set(true);
        assert_eq!(
            p.on_tick(&store, &endpoints(), t + min(11)),
            vec![Effect::Error(ERR_BK_SET_ANCHOR)]
        );
        // Lookups work again; the anchor block is still missing. The outage did not age the
        // candidate.
        store.lookup_fails.set(false);
        assert!(p.on_tick(&store, &endpoints(), t + min(11) + Duration::from_secs(5)).is_empty());
        // Ten minutes of healthy ticks later it expires, and the cursor moves.
        assert_eq!(
            p.on_tick(&store, &endpoints(), t + min(21) + Duration::from_secs(1)),
            vec![
                Effect::Error(ERR_BK_SET_EVICTED),
                advance(A, AdvanceReason::Expired { seq_no: 100 })
            ]
        );
    }

    #[test]
    fn lookup_outage_ages_no_candidate_and_counts_from_the_last_failed_tick() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let t = Instant::now();
        let min = |m: u64| Duration::from_secs(m * 60);
        let sec = Duration::from_secs;
        assert!(p.on_fetch(fetched(0, false, 100), t).is_empty());
        store.lookup_fails.set(true);
        let failed = vec![Effect::Error(ERR_BK_SET_ANCHOR)];
        assert_eq!(p.on_tick(&store, &endpoints(), t + min(11)), failed);
        // A candidate that arrives during the outage is aged from the outage too.
        assert!(p.on_fetch(fetched(0, false, 101), t + min(12)).is_empty());
        // The second failed tick is more than 10 minutes after the first one.
        assert_eq!(p.on_tick(&store, &endpoints(), t + min(25)), failed);
        // Recovery right after the last failed tick: neither candidate expires.
        store.lookup_fails.set(false);
        assert!(p.on_tick(&store, &endpoints(), t + min(25) + sec(5)).is_empty());
        assert!(p.on_tick(&store, &endpoints(), t + min(35)).is_empty());
        // After 10 minutes without a failed tick both are dropped.
        let effects = p.on_tick(&store, &endpoints(), t + min(35) + sec(6));
        let evicted = effects.iter().filter(|e| **e == Effect::Error(ERR_BK_SET_EVICTED)).count();
        assert_eq!(evicted, 2, "{effects:?}");
    }

    #[test]
    fn execute_advance_moves_the_cursor_and_counts_stalled() {
        #[derive(Default)]
        struct Recorder {
            errors: RefCell<Vec<&'static str>>,
            gauges: RefCell<Vec<(u64, String)>>,
        }
        impl PollerMetrics for Recorder {
            fn report_errors(&self, kind: &'static str) {
                self.errors.borrow_mut().push(kind);
            }

            fn report_latest_bk_set_seqno(&self, seq_no: u64, thread_id: &str) {
                self.gauges.borrow_mut().push((seq_no, thread_id.to_string()));
            }
        }
        let store = FakeStore::default();
        let mut p = start(&store);
        let rec = Recorder::default();
        let effects = vec![
            Effect::Error(ERR_BK_SET_POLL),
            Effect::Gauge { seq_no: 5, thread_id: tid() },
            advance(A, AdvanceReason::Expired { seq_no: 1 }),
        ];
        execute(effects, &mut p, endpoints, &rec);
        assert_eq!(*rec.errors.borrow(), vec![ERR_BK_SET_POLL, ERR_BK_SET_STALLED]);
        assert_eq!(*rec.gauges.borrow(), vec![(5, tid())]);
        assert_eq!(p.cursor(), 1);
        execute(vec![advance(B, AdvanceReason::Expired { seq_no: 2 })], &mut p, endpoints, &rec);
        assert_eq!(p.cursor(), 0);
    }

    #[test]
    fn rotation_rewrite_put_failure_is_retried() {
        let store = FakeStore::default();
        *store.db_snapshot.borrow_mut() = Some(stored_snapshot(100));
        let (mut p, effects) = Poller::start(&store, Duration::from_secs(60));
        assert_eq!(effects, vec![Effect::Gauge { seq_no: 100, thread_id: tid() }]);
        let t = Instant::now();
        *store.rotations.borrow_mut() += 1;
        store.put_fails.set(true);
        assert_eq!(p.on_tick(&store, &endpoints(), t), vec![Effect::Error(ERR_BK_SET_STORE)]);
        assert!(store.puts.borrow().is_empty());
        store.put_fails.set(false);
        assert!(p.on_tick(&store, &endpoints(), t).is_empty());
        assert_eq!(store.puts.borrow().len(), 1);
        assert_eq!(store.puts.borrow()[0].seq_no, 100);
        assert!(p.on_tick(&store, &endpoints(), t).is_empty());
        assert_eq!(store.puts.borrow().len(), 1);
    }

    #[test]
    fn eviction_while_lookups_fail_does_not_advance() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let t = Instant::now();
        let cap = CANDIDATE_QUEUE_CAPACITY as u64;
        // Lookups fail on every tick; polls keep filling the queue past its capacity.
        store.lookup_fails.set(true);
        let mut seq = 200;
        for _ in 0..cap {
            assert!(p.on_fetch(fetched(0, false, seq), t).is_empty());
            seq += 1;
        }
        assert_eq!(p.on_tick(&store, &endpoints(), t), vec![Effect::Error(ERR_BK_SET_ANCHOR)]);
        for _ in 0..3 {
            assert_eq!(
                p.on_fetch(fetched(0, false, seq), t),
                vec![Effect::Error(ERR_BK_SET_EVICTED)]
            );
            seq += 1;
            assert_eq!(p.on_tick(&store, &endpoints(), t), vec![Effect::Error(ERR_BK_SET_ANCHOR)]);
        }
        // A tick whose lookup succeeds clears the flag: eviction advances again.
        store.lookup_fails.set(false);
        assert!(p.on_tick(&store, &endpoints(), t).is_empty());
        let effects = p.on_fetch(fetched(0, false, seq), t);
        assert_eq!(effects.len(), 2);
        assert_eq!(effects[0], Effect::Error(ERR_BK_SET_EVICTED));
        assert!(matches!(
            effects[1],
            Effect::Advance { reason: AdvanceReason::Evicted { .. }, .. }
        ));
    }

    #[test]
    fn rejected_and_expired_candidates_of_the_current_supplier_advance() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let t = Instant::now();
        p.on_fetch(fetched(0, false, 100), t);
        store.add_block(100, false);
        assert_eq!(
            p.on_tick(&store, &endpoints(), t),
            vec![
                Effect::Error(ERR_BK_SET_COMMITMENT),
                advance(
                    A,
                    AdvanceReason::CandidateRejected {
                        seq_no: 100,
                        reason: "CommitmentMismatch".to_string()
                    }
                )
            ]
        );
        p.on_fetch(fetched(0, false, 101), t);
        assert_eq!(
            p.on_tick(&store, &endpoints(), t + CANDIDATE_MAX_AGE + Duration::from_secs(1)),
            vec![
                Effect::Error(ERR_BK_SET_EVICTED),
                advance(A, AdvanceReason::Expired { seq_no: 101 })
            ]
        );
    }

    #[test]
    fn rejection_by_another_supplier_does_not_advance() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let t = Instant::now();
        p.on_fetch(fetched(0, false, 100), t);
        p.on_fetch(fetched(1, true, 101), t);
        store.add_block(100, true);
        store.add_block(101, false);
        // Candidate 101 came from B and the cursor is at B: advance.
        assert_eq!(
            p.on_tick(&store, &endpoints(), t),
            vec![
                Effect::Error(ERR_BK_SET_COMMITMENT),
                advance(
                    B,
                    AdvanceReason::CandidateRejected {
                        seq_no: 101,
                        reason: "CommitmentMismatch".to_string()
                    }
                )
            ]
        );
        // The same rejection while the cursor is at A (it moved on): no advance.
        let mut p = start(&store);
        p.on_fetch(fetched(1, true, 201), t);
        p.on_fetch(fetched(0, true, 150), t);
        store.add_block(201, false);
        assert_eq!(p.on_tick(&store, &endpoints(), t), vec![Effect::Error(ERR_BK_SET_COMMITMENT)]);
    }

    #[test]
    fn rejection_without_endpoints_does_not_advance() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let t = Instant::now();
        p.on_fetch(fetched(0, false, 100), t);
        store.add_block(100, false);
        assert_eq!(p.on_tick(&store, &[], t), vec![Effect::Error(ERR_BK_SET_COMMITMENT)]);
    }

    #[test]
    fn start_with_snapshot_emits_gauge() {
        let store = FakeStore::default();
        *store.db_snapshot.borrow_mut() = Some(stored_snapshot(42));
        let (_, effects) = Poller::start(&store, Duration::from_secs(60));
        assert_eq!(effects, vec![Effect::Gauge { seq_no: 42, thread_id: tid() }]);
    }

    #[test]
    fn start_with_load_error_emits_restore_error() {
        let store = FakeStore::default();
        store.load_fails.set(true);
        let (mut p, effects) = Poller::start(&store, Duration::from_secs(60));
        assert_eq!(effects, vec![Effect::Error(ERR_BK_SET_RESTORE)]);
        // The poller starts empty and accepts anything.
        assert!(p.on_fetch(fetched(0, false, 1), Instant::now()).is_empty());
    }

    #[test]
    fn put_failure_then_retry_writes() {
        let store = FakeStore::default();
        let mut p = start(&store);
        let t = Instant::now();
        store.add_block(100, true);
        store.add_block(101, true);
        p.on_fetch(fetched(0, false, 100), t);
        store.put_fails.set(true);
        assert_eq!(p.on_tick(&store, &endpoints(), t), vec![Effect::Error(ERR_BK_SET_STORE)]);
        assert!(store.puts.borrow().is_empty());
        // The failed candidate left the queue; the next response is written.
        store.put_fails.set(false);
        p.on_fetch(fetched(0, false, 101), t);
        assert!(p.on_tick(&store, &endpoints(), t).is_empty());
        assert_eq!(store.puts.borrow().len(), 1);
        // The writer thread confirms it: the gauge follows the confirmed state.
        store.written_seq_no.set(101);
        assert_eq!(
            p.on_tick(&store, &endpoints(), t),
            vec![Effect::Gauge { seq_no: 101, thread_id: tid() }]
        );
        assert!(p.on_tick(&store, &endpoints(), t).is_empty());
    }

    #[test]
    fn writer_failures_are_counted_per_failure() {
        let store = FakeStore::default();
        let mut p = start(&store);
        store.write_failures.set(2);
        assert_eq!(
            p.on_tick(&store, &endpoints(), Instant::now()),
            vec![Effect::Error(ERR_BK_SET_STORE); 2]
        );
    }

    #[test]
    fn write_metrics_follow_confirmed_state() {
        let store = FakeStore::default();
        *store.db_snapshot.borrow_mut() = Some(stored_snapshot(50));
        let (mut p, _) = Poller::start(&store, Duration::from_secs(60));
        let t = Instant::now();
        let gauge = |seq_no| Effect::Gauge { seq_no, thread_id: tid() };
        // Nothing confirmed yet.
        assert!(p.on_tick(&store, &endpoints(), t).is_empty());
        // First confirmed write.
        store.written_seq_no.set(100);
        assert_eq!(p.on_tick(&store, &endpoints(), t), vec![gauge(100)]);
        // Queued but not yet written, or the same seq_no rewritten after a rotation.
        assert!(p.on_tick(&store, &endpoints(), t).is_empty());
        // Failed writes: the gauge stays, errors are counted by the delta.
        store.write_failures.set(2);
        assert_eq!(p.on_tick(&store, &endpoints(), t), vec![Effect::Error(ERR_BK_SET_STORE); 2]);
        store.write_failures.set(3);
        store.written_seq_no.set(160);
        assert_eq!(
            p.on_tick(&store, &endpoints(), t),
            vec![Effect::Error(ERR_BK_SET_STORE), gauge(160)]
        );
    }

    #[test]
    fn short_interval_capacity_covers_lag() {
        assert_eq!(queue_capacity(Duration::from_secs(1)), 600);
        let store = FakeStore::default();
        let (mut p, _) = Poller::start(&store, Duration::from_secs(1));
        let t = Instant::now();
        // 30 s of one-second polls, each with a newer seq_no, before the anchor block appears.
        for i in 0..30u64 {
            let now = t + Duration::from_secs(i);
            assert!(p.on_fetch(fetched(0, false, 100 + i), now).is_empty());
        }
        store.add_block(100, true);
        let now = t + Duration::from_secs(30);
        assert!(p.on_tick(&store, &endpoints(), now).is_empty());
        assert_eq!(store.puts.borrow()[0].seq_no, 100);
    }

    #[test]
    fn queue_capacity_bounds() {
        assert_eq!(queue_capacity(Duration::from_secs(60)), 10);
        assert_eq!(queue_capacity(Duration::from_secs(3600)), 10);
        assert_eq!(queue_capacity(Duration::from_secs(30)), 20);
        assert_eq!(queue_capacity(Duration::from_secs(7)), 86);
        assert_eq!(queue_capacity(Duration::from_millis(1500)), 400);
    }

    // ---- fetch_bk_set against local sockets ----

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use message_router::bp_resolver::BPResolver;
    use message_router::message_router::MessageRouter;
    use message_router::message_router::MessageRouterConfig;
    use parking_lot::Mutex;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use transport_layer::HostPort;

    use crate::domain::models::BkApiPool;

    struct NoBp;

    impl BPResolver for NoBp {
        fn resolve(&mut self, _: Option<String>) -> Vec<HostPort> {
            vec![]
        }
    }

    #[derive(Clone)]
    enum Reply {
        InternalError,
        Valid(u64),
        /// Like `Valid`, sent once the `Notify` is signalled.
        Held(u64, Arc<tokio::sync::Notify>),
        /// Chunked body without `Content-Length`, larger than the limit.
        EndlessChunked,
    }

    /// Serves `reply` to every connection on a free local port; returns the address and the
    /// number of requests received.
    async fn serve(reply: Reply) -> (HostPort, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                counter.fetch_add(1, Ordering::SeqCst);
                let reply = reply.clone();
                tokio::spawn(async move {
                    let mut req = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => req.extend_from_slice(&buf[..n]),
                        }
                    }
                    if let Reply::Held(_, gate) = &reply {
                        gate.notified().await;
                    }
                    // Write errors are expected: the client may hang up early.
                    match reply {
                        Reply::InternalError => {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 500 Internal Server Error\r\n\
                                      Content-Length: 0\r\nConnection: close\r\n\r\n",
                                )
                                .await;
                        }
                        Reply::Valid(seq_no) | Reply::Held(seq_no, _) => {
                            let bk_set: Vec<_> = signers()
                                .iter()
                                .map(|(i, k)| json!({"signer_index": i, "pubkey": hex::encode(k)}))
                                .collect();
                            let body =
                                json!({"seq_no": seq_no, "thread_id": tid(), "bk_set": bk_set})
                                    .to_string();
                            let head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            );
                            let _ = sock.write_all(head.as_bytes()).await;
                            let _ = sock.write_all(body.as_bytes()).await;
                        }
                        Reply::EndlessChunked => {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\
                                      Connection: close\r\n\r\n",
                                )
                                .await;
                            let chunk = vec![b'a'; 64 * 1024];
                            // 5 MiB in total, above MAX_RESPONSE_BYTES.
                            for _ in 0..80 {
                                let mut out = format!("{:x}\r\n", chunk.len()).into_bytes();
                                out.extend_from_slice(&chunk);
                                out.extend_from_slice(b"\r\n");
                                if sock.write_all(&out).await.is_err() {
                                    return;
                                }
                            }
                            let _ = sock.write_all(b"0\r\n\r\n").await;
                        }
                    }
                    let _ = sock.shutdown().await;
                });
            }
        });
        (HostPort::from(addr), hits)
    }

    fn app_state(endpoints: Vec<HostPort>) -> AppState {
        let message_router = Arc::new(MessageRouter::new(
            "127.0.0.1:0".to_string(),
            MessageRouterConfig {
                bp_resolver: Arc::new(Mutex::new(NoBp)),
                owner_wallet_pubkey: None,
                signing_keys: None,
            },
        ));
        AppState {
            message_router,
            last_block_gen_utime: Default::default(),
            readiness_max_block_age_secs: 0,
            bk_api_token: "token".to_string(),
            bk_api_pool: BkApiPool::new(endpoints),
        }
    }

    #[tokio::test]
    async fn fetch_fails_over_to_next_endpoint() {
        let (bad, bad_hits) = serve(Reply::InternalError).await;
        let (good, good_hits) = serve(Reply::Valid(42)).await;
        let state = app_state(vec![bad, good.clone()]);
        let fetched = fetch_bk_set(&state, &reqwest::Client::new(), 0).await.unwrap();
        assert_eq!(fetched.idx, 1);
        assert_eq!(fetched.endpoint, good.to_string());
        assert_eq!(fetched.response.seq_no, 42);
        assert!(fetched.failed_over);
        assert_eq!((bad_hits.load(Ordering::SeqCst), good_hits.load(Ordering::SeqCst)), (1, 1));
    }

    #[tokio::test]
    async fn fetch_wraps_around_and_keeps_the_pool_default() {
        let (bad0, hits0) = serve(Reply::InternalError).await;
        let (good1, hits1) = serve(Reply::Valid(42)).await;
        let (bad2, hits2) = serve(Reply::InternalError).await;
        let state = app_state(vec![bad0.clone(), good1, bad2]);
        let fetched = fetch_bk_set(&state, &reqwest::Client::new(), 2).await.unwrap();
        assert_eq!(fetched.idx, 1);
        assert!(fetched.failed_over);
        let hits = |h: &Arc<AtomicUsize>| h.load(Ordering::SeqCst);
        assert_eq!((hits(&hits2), hits(&hits0), hits(&hits1)), (1, 1, 1));
        assert_eq!(state.bk_api_pool.endpoints_to_try()[0], bad0);
    }

    #[tokio::test]
    async fn fetch_from_a_cursor_past_a_shrunk_list_wraps_it() {
        let hits = |h: &Arc<AtomicUsize>| h.load(Ordering::SeqCst);
        // The cursor 3 points at index 1 of 2: that endpoint answers, no failover.
        let (good0, hits0) = serve(Reply::Valid(7)).await;
        let (good1, hits1) = serve(Reply::Valid(8)).await;
        let state = app_state(vec![good0, good1]);
        let fetched = fetch_bk_set(&state, &reqwest::Client::new(), 3).await.unwrap();
        assert_eq!((fetched.idx, fetched.response.seq_no), (1, 8));
        assert!(!fetched.failed_over);
        assert_eq!((hits(&hits0), hits(&hits1)), (0, 1));

        // The wrapped endpoint fails and the next one answers: a failover.
        let (good0, hits0) = serve(Reply::Valid(7)).await;
        let (bad1, hits1) = serve(Reply::InternalError).await;
        let state = app_state(vec![good0, bad1]);
        let fetched = fetch_bk_set(&state, &reqwest::Client::new(), 3).await.unwrap();
        assert_eq!((fetched.idx, fetched.response.seq_no), (0, 7));
        assert!(fetched.failed_over);
        assert_eq!((hits(&hits0), hits(&hits1)), (1, 1));
    }

    #[tokio::test]
    async fn fetch_answer_ignores_a_list_reload_in_flight() {
        let (a, _) = serve(Reply::InternalError).await;
        let (b, _) = serve(Reply::InternalError).await;
        let gate = Arc::new(tokio::sync::Notify::new());
        let (c, c_hits) = serve(Reply::Held(42, gate.clone())).await;
        let state = Arc::new(app_state(vec![a.clone(), b.clone(), c]));
        let task = tokio::spawn({
            let state = state.clone();
            async move { fetch_bk_set(&state, &reqwest::Client::new(), 2).await }
        });
        // The request is in flight once the server has accepted it.
        let reached = tokio::time::timeout(Duration::from_secs(30), async {
            while c_hits.load(Ordering::SeqCst) == 0 && !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(reached.is_ok(), "endpoint c was never reached");
        assert!(
            c_hits.load(Ordering::SeqCst) > 0,
            "the fetch finished without reaching endpoint c"
        );
        state.bk_api_pool.update_endpoints(vec![a, b]);
        gate.notify_one();
        let fetched = task.await.unwrap().unwrap();
        assert_eq!(fetched.idx, 2);
        assert!(!fetched.failed_over);
    }

    #[tokio::test]
    async fn fetch_rejects_streamed_body_over_limit() {
        let (huge, huge_hits) = serve(Reply::EndlessChunked).await;
        let (good, _) = serve(Reply::Valid(7)).await;
        let state = app_state(vec![huge.clone(), good]);
        // The oversize endpoint is skipped and the next one answers.
        let fetched = fetch_bk_set(&state, &reqwest::Client::new(), 0).await.unwrap();
        assert_eq!(fetched.idx, 1);
        assert_eq!(fetched.response.seq_no, 7);
        assert_eq!(huge_hits.load(Ordering::SeqCst), 1);

        // Alone, it is an error naming the limit.
        let state = app_state(vec![huge]);
        let err = fetch_bk_set(&state, &reqwest::Client::new(), 0).await.err().unwrap();
        assert!(error_text(&err).contains("exceeds the limit"), "{}", error_text(&err));
    }

    #[tokio::test]
    async fn fetch_without_endpoints_is_an_error() {
        let state = app_state(vec![]);
        assert!(fetch_bk_set(&state, &reqwest::Client::new(), 0).await.is_err());
    }
}
