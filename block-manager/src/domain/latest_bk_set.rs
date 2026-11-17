// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
//! Pure domain logic for the `latest_bk_set` table: parsing and validating the node's
//! `/v2/bk_set` response, checking it against the anchor block's Poseidon commitment,
//! and queueing candidates until their anchor block reaches the local DB.
//!
//! Nothing here may panic on response content: BM's panic hook exits the process.

use std::collections::HashSet;
use std::ops::Range;
use std::time::Duration;
use std::time::Instant;

use database::sqlite::AnchorBlock;
use database::sqlite::AnchorLookup;
use node::block_keeper_system::bk_set_poseidon_commitment;
use node::block_keeper_system::validate_bls_pubkey;
use node::block_keeper_system::MAX_SIGNERS;
use node::block_keeper_system::PADDING_SIGNER_INDEX;
use node::types::BLOCK_MERKLE_LEAF_COUNT;
use node::types::BLOCK_MERKLE_OLD_BK_SET_LEAF_INDEX;
use serde::Deserialize;

const THREAD_ID_HEX_LEN: usize = 68;
/// Size of one Merkle leaf (a hash) in `blocks.block_merkle_leaves`.
const MERKLE_LEAF_LEN: usize = 32;
/// The old BK set commitment leaf (L2) of `blocks.block_merkle_leaves`.
pub const L2_RANGE: Range<usize> = BLOCK_MERKLE_OLD_BK_SET_LEAF_INDEX * MERKLE_LEAF_LEN
    ..(BLOCK_MERKLE_OLD_BK_SET_LEAF_INDEX + 1) * MERKLE_LEAF_LEN;
pub const MERKLE_LEAVES_LEN: usize = BLOCK_MERKLE_LEAF_COUNT * MERKLE_LEAF_LEN;

#[derive(Deserialize)]
/// `future_bk_set` is deliberately not parsed.
pub struct BkSetResponse {
    pub seq_no: u64,
    #[serde(default)]
    pub thread_id: Option<String>,
    pub bk_set: Vec<ResponseBk>,
}

/// Only the fields BM needs. `protocol_version_support` and the rest are deliberately not parsed.
#[derive(Deserialize)]
pub struct ResponseBk {
    pub signer_index: u64,
    pub pubkey: String,
}

pub fn parse_response(body: &[u8]) -> anyhow::Result<BkSetResponse> {
    Ok(serde_json::from_slice(body)?)
}

/// A response that passed `validate`. Fields are private so that only `validate` builds one.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedBkSet {
    thread_id: String,
    seq_no: u64,
    /// Sorted by signer index.
    signers: Vec<(u16, [u8; 48])>,
}

impl ValidatedBkSet {
    pub fn seq_no(&self) -> u64 {
        self.seq_no
    }

    /// `(thread_id, seq_no, signers)`, signers sorted by index.
    pub fn into_parts(self) -> (String, u64, Vec<(u16, [u8; 48])>) {
        (self.thread_id, self.seq_no, self.signers)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BkSetRejection {
    MissingThreadId,
    InvalidThreadId(String),
    Empty,
    TooManySigners(usize),
    SignerIndexOutOfRange(u64),
    DuplicateSignerIndex(u16),
    InvalidPubkey(u16),
    /// Does not fit into the `INTEGER` column of `blocks.seq_no`.
    SeqNoOutOfRange(u64),
    /// Not above `last`: the `seq_no` of the last snapshot queued for writing (or restored), or
    /// of the newest queued candidate. Normal after a failover to a lagging BK.
    NotNewer {
        got: u64,
        last: u64,
    },
    /// The anchor block can never be used (ambiguous or malformed row).
    BadAnchor(String),
    MissingLeaves,
    /// The commitment function failed on the set.
    CommitmentError(String),
    CommitmentMismatch,
}

fn is_valid_thread_id(s: &str) -> bool {
    s.len() == THREAD_ID_HEX_LEN && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

pub fn validate(
    resp: BkSetResponse,
    last_written_seq_no: Option<u64>,
) -> Result<ValidatedBkSet, BkSetRejection> {
    let thread_id = resp.thread_id.ok_or(BkSetRejection::MissingThreadId)?;
    if !is_valid_thread_id(&thread_id) {
        return Err(BkSetRejection::InvalidThreadId(thread_id));
    }
    if i64::try_from(resp.seq_no).is_err() {
        return Err(BkSetRejection::SeqNoOutOfRange(resp.seq_no));
    }
    // Before the per-key work: after a failover to a lagging BK every response is not newer.
    if let Some(last) = last_written_seq_no {
        if resp.seq_no <= last {
            return Err(BkSetRejection::NotNewer { got: resp.seq_no, last });
        }
    }
    // `bk_set` is the exact signer set of block `seq_no`: every entry is a signer.
    let entries = resp.bk_set;
    if entries.is_empty() {
        return Err(BkSetRejection::Empty);
    }
    // Strictly fewer than `MAX_SIGNERS`, not "at most": the commitment has `MAX_SIGNERS` slots
    // and drops signers past them, so the first 300 of a 301-signer set match it as well. With
    // at most 299 entries the last slot of a matching commitment is padding, which proves the
    // set is complete.
    if entries.len() >= MAX_SIGNERS {
        return Err(BkSetRejection::TooManySigners(entries.len()));
    }
    let mut seen = HashSet::new();
    let mut signers = Vec::with_capacity(entries.len());
    for bk in entries {
        let index = u16::try_from(bk.signer_index)
            .ok()
            .filter(|i| *i != PADDING_SIGNER_INDEX)
            .ok_or(BkSetRejection::SignerIndexOutOfRange(bk.signer_index))?;
        if !seen.insert(index) {
            return Err(BkSetRejection::DuplicateSignerIndex(index));
        }
        let pubkey: [u8; 48] = hex::decode(&bk.pubkey)
            .ok()
            .and_then(|b| <[u8; 48]>::try_from(b).ok())
            .filter(|k| validate_bls_pubkey(k).is_ok())
            .ok_or(BkSetRejection::InvalidPubkey(index))?;
        signers.push((index, pubkey));
    }
    signers.sort_by_key(|(i, _)| *i);
    Ok(ValidatedBkSet { thread_id, seq_no: resp.seq_no, signers })
}

pub fn check_commitment(
    set: &ValidatedBkSet,
    block_merkle_leaves: Option<&[u8]>,
) -> Result<(), BkSetRejection> {
    let l2 = block_merkle_leaves
        .filter(|l| l.len() == MERKLE_LEAVES_LEN)
        .and_then(|l| l.get(L2_RANGE))
        .filter(|l2| l2.iter().any(|b| *b != 0))
        .ok_or(BkSetRejection::MissingLeaves)?;
    // A match proves the set complete only below `MAX_SIGNERS` (see `validate`): keep that
    // conclusion here even for a set that did not come through `validate`.
    if set.signers.len() >= MAX_SIGNERS {
        return Err(BkSetRejection::TooManySigners(set.signers.len()));
    }
    match bk_set_poseidon_commitment(&set.signers) {
        Ok(c) if c.as_slice() == l2 => Ok(()),
        Ok(_) => Err(BkSetRejection::CommitmentMismatch),
        Err(err) => Err(BkSetRejection::CommitmentError(format!("{err:#}"))),
    }
}

/// Candidates waiting for their anchor block, ascending by `seq_no`.
pub struct CandidateQueue {
    capacity: usize,
    max_age: Duration,
    items: Vec<Candidate>,
}

struct Candidate {
    set: ValidatedBkSet,
    /// The endpoint that supplied the candidate.
    supplier: String,
    queued_at: Instant,
}

/// A candidate dropped from a full queue, with the endpoint that supplied it.
#[derive(Debug, PartialEq)]
pub struct Evicted {
    pub seq_no: u64,
    pub supplier: String,
}

#[derive(Debug, PartialEq)]
pub enum PushOutcome {
    /// `evicted` is the oldest entry dropped because the queue was over capacity.
    Queued { evicted: Option<Evicted> },
    /// `seq_no` is not above the newest queued one; the candidate was ignored.
    NotNewerThanQueued,
}

#[derive(Debug, PartialEq)]
pub struct QueueResolution {
    pub hit: Option<(ValidatedBkSet, AnchorBlock)>,
    /// `(seq_no, reason, supplier)`
    pub rejected: Vec<(u64, BkSetRejection, String)>,
    /// `(seq_no, supplier)` of pending candidates removed for waiting longer than `max_age`.
    pub expired: Vec<(u64, String)>,
}

impl CandidateQueue {
    pub fn new(capacity: usize, max_age: Duration) -> Self {
        Self { capacity, max_age, items: Vec::new() }
    }

    /// Ignores `c` unless its `seq_no` is above the newest queued one.
    pub fn push(&mut self, c: ValidatedBkSet, supplier: &str, now: Instant) -> PushOutcome {
        if self.items.last().is_some_and(|newest| c.seq_no <= newest.set.seq_no) {
            return PushOutcome::NotNewerThanQueued;
        }
        self.items.push(Candidate { set: c, supplier: supplier.to_string(), queued_at: now });
        let evicted = (self.items.len() > self.capacity).then(|| {
            let oldest = self.items.remove(0);
            Evicted { seq_no: oldest.set.seq_no, supplier: oldest.supplier }
        });
        PushOutcome::Queued { evicted }
    }

    /// `seq_no` of the newest queued candidate.
    pub fn newest_seq_no(&self) -> Option<u64> {
        self.items.last().map(|c| c.set.seq_no)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Sets the queued time of every candidate to `now`: while the anchor lookup fails the
    /// candidates cannot be checked, so the outage must not count towards their age.
    pub fn restart_ages(&mut self, now: Instant) {
        for c in &mut self.items {
            c.queued_at = now;
        }
    }

    /// Scans newest to oldest. Unresolved blocks stay queued until they are older than
    /// `max_age`; a failed check or an invalid anchor drops only that candidate; the first
    /// passing candidate is the hit and drops itself and all older ones.
    /// A `lookup` error leaves the queue unchanged.
    pub fn resolve<L, C>(
        &mut self,
        mut lookup: L,
        mut check: C,
        now: Instant,
    ) -> anyhow::Result<QueueResolution>
    where
        L: FnMut(&str, u64) -> anyhow::Result<AnchorLookup>,
        C: FnMut(&ValidatedBkSet, &AnchorBlock) -> Result<(), BkSetRejection>,
    {
        let mut rejected_idx: HashSet<usize> = HashSet::new();
        let mut rejected: Vec<(u64, BkSetRejection, String)> = Vec::new();
        let mut hit_at: Option<(usize, AnchorBlock)> = None;
        for (idx, Candidate { set, supplier, .. }) in self.items.iter().enumerate().rev() {
            let block = match lookup(&set.thread_id, set.seq_no)? {
                AnchorLookup::Missing => continue,
                AnchorLookup::Invalid(reason) => {
                    rejected_idx.insert(idx);
                    rejected.push((
                        set.seq_no,
                        BkSetRejection::BadAnchor(reason),
                        supplier.clone(),
                    ));
                    continue;
                }
                AnchorLookup::Found(block) => block,
            };
            match check(set, &block) {
                Ok(()) => {
                    hit_at = Some((idx, block));
                    break;
                }
                Err(reason) => {
                    rejected_idx.insert(idx);
                    rejected.push((set.seq_no, reason, supplier.clone()));
                }
            }
        }
        let hit_pos = hit_at.as_ref().map(|(i, _)| *i);
        let mut hit_set = None;
        let mut kept = Vec::new();
        let mut expired = Vec::new();
        for (idx, c) in std::mem::take(&mut self.items).into_iter().enumerate() {
            if hit_pos == Some(idx) {
                hit_set = Some(c.set);
            } else if hit_pos.is_some_and(|h| idx < h) || rejected_idx.contains(&idx) {
                // older than the hit, or rejected: dropped
            } else if now.saturating_duration_since(c.queued_at) > self.max_age {
                expired.push((c.set.seq_no, c.supplier));
            } else {
                kept.push(c);
            }
        }
        self.items = kept;
        let hit = hit_set.zip(hit_at.map(|(_, b)| b));
        Ok(QueueResolution { hit, rejected, expired })
    }
}

#[cfg(test)]
mod tests {
    use database::sqlite::AnchorBlock;
    use node::block_keeper_system::bk_set_poseidon_commitment;
    use serde_json::json;

    use super::*;

    fn key(i: u32) -> [u8; 48] {
        let mut ikm = [7u8; 32];
        ikm[..4].copy_from_slice(&i.to_le_bytes());
        let sk = gosh_blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap();
        sk.sk_to_pk().to_bytes()
    }

    fn tid() -> String {
        format!("00{}", "ab".repeat(33))
    }

    fn entry(idx: u64, pk: &str) -> serde_json::Value {
        json!({"signer_index": idx, "pubkey": pk, "status": "Active"})
    }

    fn resp(
        seq_no: u64,
        thread_id: Option<String>,
        bk_set: Vec<serde_json::Value>,
    ) -> BkSetResponse {
        let mut v = json!({"seq_no": seq_no, "bk_set": bk_set, "future_bk_set": []});
        if let Some(t) = thread_id {
            v["thread_id"] = json!(t);
        }
        parse_response(v.to_string().as_bytes()).unwrap()
    }

    fn ok_resp(entries: Vec<serde_json::Value>) -> BkSetResponse {
        resp(100, Some(tid()), entries)
    }

    fn set(seq_no: u64) -> ValidatedBkSet {
        ValidatedBkSet { thread_id: tid(), seq_no, signers: vec![(0, key(0))] }
    }

    fn anchor() -> AnchorBlock {
        AnchorBlock {
            block_id: "b".into(),
            height: [0; 8],
            chain_order: "o".into(),
            block_merkle_leaves: None,
        }
    }

    #[test]
    fn validate_accepts_exact_set() {
        let (a, b, c) = (key(1), key(2), key(3));
        let r = ok_resp(vec![
            entry(9, &hex::encode(a)),
            entry(5, &hex::encode(c)),
            entry(2, &hex::encode(b)),
        ]);
        let v = validate(r, None).unwrap();
        assert_eq!(v.signers, vec![(2, b), (5, c), (9, a)]);
        assert_eq!(v.seq_no, 100);
        assert_eq!(v.thread_id, tid());
    }

    #[test]
    fn rejects_old_bk_set_format() {
        let body = json!({
            "bk_set": [{"node_id": "ab", "node_owner_pk": "cd", "epoch_start_seq_no": 1}],
            "future_bk_set": [],
            "seq_no": 5
        });
        assert!(parse_response(body.to_string().as_bytes()).is_err());
        // Old format with an empty set parses, and is rejected for the missing thread id.
        let empty = json!({"bk_set": [], "future_bk_set": [], "seq_no": 5});
        let r = parse_response(empty.to_string().as_bytes()).unwrap();
        assert_eq!(validate(r, None), Err(BkSetRejection::MissingThreadId));
    }

    #[test]
    fn validate_rejects_bad_thread_id() {
        for t in ["XYZ".to_string(), "AB".repeat(34)] {
            let r = resp(1, Some(t.clone()), vec![entry(0, &hex::encode(key(1)))]);
            assert_eq!(validate(r, None), Err(BkSetRejection::InvalidThreadId(t)));
        }
    }

    #[test]
    fn validate_rejects_empty_set() {
        assert_eq!(validate(ok_resp(vec![]), None), Err(BkSetRejection::Empty));
    }

    #[test]
    fn validate_rejects_300_or_more_signers() {
        let k = hex::encode(key(1));
        let many: Vec<_> = (0..300).map(|i| entry(i, &k)).collect();
        assert_eq!(validate(ok_resp(many), None), Err(BkSetRejection::TooManySigners(300)));
        let keys: Vec<_> = (0..299u32).map(key).collect();
        let ok: Vec<_> =
            keys.iter().enumerate().map(|(i, k)| entry(i as u64, &hex::encode(k))).collect();
        assert_eq!(validate(ok_resp(ok), None).unwrap().signers.len(), 299);
    }

    #[test]
    fn validate_rejects_index_out_of_range() {
        let k = hex::encode(key(1));
        for idx in [65536u64, 65535] {
            assert_eq!(
                validate(ok_resp(vec![entry(idx, &k)]), None),
                Err(BkSetRejection::SignerIndexOutOfRange(idx))
            );
        }
    }

    #[test]
    fn validate_rejects_duplicate_index() {
        let r = ok_resp(vec![entry(4, &hex::encode(key(1))), entry(4, &hex::encode(key(2)))]);
        assert_eq!(validate(r, None), Err(BkSetRejection::DuplicateSignerIndex(4)));
    }

    #[test]
    fn validate_rejects_invalid_pubkey_without_panic() {
        let short = hex::encode([1u8; 47]);
        let zero = hex::encode([0u8; 48]);
        for bad in [short.as_str(), "zz", "", zero.as_str()] {
            let r = ok_resp(vec![entry(3, bad)]);
            assert_eq!(validate(r, None), Err(BkSetRejection::InvalidPubkey(3)), "{bad}");
        }
    }

    #[test]
    fn validate_rejects_not_newer() {
        let mk = |n| resp(n, Some(tid()), vec![entry(0, &hex::encode(key(1)))]);
        assert_eq!(
            validate(mk(99), Some(100)),
            Err(BkSetRejection::NotNewer { got: 99, last: 100 })
        );
        assert_eq!(
            validate(mk(100), Some(100)),
            Err(BkSetRejection::NotNewer { got: 100, last: 100 })
        );
        assert!(validate(mk(101), Some(100)).is_ok());
    }

    #[test]
    fn validate_rejects_seq_no_above_i64_max() {
        let mk = |n| resp(n, Some(tid()), vec![entry(0, &hex::encode(key(1)))]);
        let big = i64::MAX as u64 + 1;
        assert_eq!(validate(mk(big), None), Err(BkSetRejection::SeqNoOutOfRange(big)));
        assert_eq!(validate(mk(u64::MAX), Some(5)), Err(BkSetRejection::SeqNoOutOfRange(u64::MAX)));
        assert!(validate(mk(i64::MAX as u64), None).is_ok());
    }

    #[test]
    fn validate_checks_not_newer_before_keys() {
        // A lagging BK's response is rejected as not newer before its keys are decoded.
        let r = resp(99, Some(tid()), vec![entry(0, "zz")]);
        assert_eq!(validate(r, Some(100)), Err(BkSetRejection::NotNewer { got: 99, last: 100 }));
    }

    #[test]
    fn validate_ignores_protocol_version_support() {
        let mut e = entry(0, &hex::encode(key(1)));
        e["protocol_version_support"] = json!("garbage");
        assert!(validate(ok_resp(vec![e]), None).is_ok());
        assert!(validate(ok_resp(vec![entry(0, &hex::encode(key(1)))]), None).is_ok());
    }

    #[test]
    fn parse_response_rejects_garbage() {
        assert!(parse_response(b"<html>502</html>").is_err());
    }

    fn leaves_with(l2: &[u8]) -> Vec<u8> {
        let mut l = vec![0u8; MERKLE_LEAVES_LEN];
        l[L2_RANGE].copy_from_slice(l2);
        l
    }

    #[test]
    fn check_commitment_accepts_matching_l2() {
        let s = set(1);
        let c = bk_set_poseidon_commitment(&s.signers).unwrap();
        assert_eq!(check_commitment(&s, Some(&leaves_with(&c))), Ok(()));
    }

    #[test]
    fn check_commitment_rejects_missing_or_zero_leaves() {
        let s = set(1);
        let missing = Err(BkSetRejection::MissingLeaves);
        assert_eq!(check_commitment(&s, None), missing);
        assert_eq!(check_commitment(&s, Some(&[1u8; 511])), missing);
        assert_eq!(check_commitment(&s, Some(&[1u8; 513])), missing);
        assert_eq!(check_commitment(&s, Some(&leaves_with(&[0; 32]))), missing);
    }

    #[test]
    fn check_commitment_rejects_mismatch() {
        let s = set(1);
        assert_eq!(
            check_commitment(&s, Some(&leaves_with(&[9; 32]))),
            Err(BkSetRejection::CommitmentMismatch)
        );
    }

    #[test]
    fn check_commitment_rejects_300_signers_even_when_l2_matches() {
        // Built directly, not through `validate`: the first 300 of a larger set match L2 as well.
        let signers: Vec<_> = (0..300u16).map(|i| (i, key(0))).collect();
        let c = bk_set_poseidon_commitment(&signers).unwrap();
        let s = ValidatedBkSet { thread_id: tid(), seq_no: 1, signers };
        assert_eq!(
            check_commitment(&s, Some(&leaves_with(&c))),
            Err(BkSetRejection::TooManySigners(300))
        );
    }

    #[test]
    fn check_commitment_reports_computation_error() {
        let s =
            ValidatedBkSet { thread_id: tid(), seq_no: 1, signers: vec![(4, key(1)), (4, key(2))] };
        assert!(matches!(
            check_commitment(&s, Some(&leaves_with(&[9; 32]))),
            Err(BkSetRejection::CommitmentError(_))
        ));
    }

    fn seqs(q: &CandidateQueue) -> Vec<u64> {
        q.items.iter().map(|c| c.set.seq_no).collect()
    }

    const AGE: Duration = Duration::from_secs(600);
    const SUPPLIER: &str = "bk-a:8600";

    fn queue(capacity: usize) -> CandidateQueue {
        CandidateQueue::new(capacity, AGE)
    }

    fn found(_: &str, n: u64, at: &[u64]) -> anyhow::Result<AnchorLookup> {
        Ok(if at.contains(&n) { AnchorLookup::Found(anchor()) } else { AnchorLookup::Missing })
    }

    #[test]
    fn restart_ages_postpones_expiry() {
        let t = Instant::now();
        let min = |m: u64| Duration::from_secs(m * 60);
        let mut q = queue(5);
        q.push(set(100), SUPPLIER, t);
        q.restart_ages(t + min(9));
        let missing = |t: &str, n| found(t, n, &[]);
        let r = q.resolve(missing, |_, _| Ok(()), t + min(11)).unwrap();
        assert!(r.expired.is_empty());
        let r = q.resolve(missing, |_, _| Ok(()), t + min(19) + Duration::from_secs(1)).unwrap();
        assert_eq!(r.expired, vec![(100, SUPPLIER.to_string())]);
    }

    #[test]
    fn queue_evicts_oldest_over_capacity() {
        let t = Instant::now();
        let mut q = queue(2);
        assert_eq!(q.push(set(1), "bk-a:8600", t), PushOutcome::Queued { evicted: None });
        assert_eq!(q.push(set(2), "bk-b:8600", t), PushOutcome::Queued { evicted: None });
        assert_eq!(
            q.push(set(3), "bk-c:8600", t),
            PushOutcome::Queued {
                evicted: Some(Evicted { seq_no: 1, supplier: "bk-a:8600".to_string() })
            }
        );
        assert_eq!(seqs(&q), vec![2, 3]);
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn push_reports_not_newer_than_queued() {
        let t = Instant::now();
        let mut q = queue(5);
        q.push(set(10), SUPPLIER, t);
        assert_eq!(q.push(set(10), SUPPLIER, t), PushOutcome::NotNewerThanQueued);
        assert_eq!(q.push(set(9), SUPPLIER, t), PushOutcome::NotNewerThanQueued);
        assert_eq!(seqs(&q), vec![10]);
    }

    fn fill(t: Instant) -> CandidateQueue {
        let mut q = queue(5);
        for n in [100, 160, 220] {
            q.push(set(n), SUPPLIER, t);
        }
        q
    }

    #[test]
    fn queue_takes_newest_resolved_and_drops_older() {
        let t = Instant::now();
        let mut q = fill(t);
        let r = q.resolve(|t, n| found(t, n, &[160]), |_, _| Ok(()), t).unwrap();
        assert_eq!(r.hit.map(|(s, _)| s.seq_no), Some(160));
        assert!(r.rejected.is_empty());
        assert!(r.expired.is_empty());
        assert_eq!(seqs(&q), vec![220]);
    }

    #[test]
    fn queue_skips_rejected_and_keeps_scanning_older() {
        let t = Instant::now();
        let mut q = fill(t);
        let r = q
            .resolve(
                |t, n| found(t, n, &[160, 220]),
                |s, _| {
                    if s.seq_no == 220 {
                        Err(BkSetRejection::CommitmentMismatch)
                    } else {
                        Ok(())
                    }
                },
                t,
            )
            .unwrap();
        assert_eq!(r.hit.map(|(s, _)| s.seq_no), Some(160));
        assert_eq!(
            r.rejected,
            vec![(220, BkSetRejection::CommitmentMismatch, SUPPLIER.to_string())]
        );
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn queue_keeps_entries_on_lookup_error() {
        let t = Instant::now();
        let mut q = fill(t);
        let r = q.resolve(
            |_, n| {
                if n == 160 {
                    Err(anyhow::anyhow!("db"))
                } else {
                    Ok(AnchorLookup::Found(anchor()))
                }
            },
            |s, _| {
                if s.seq_no == 220 {
                    Err(BkSetRejection::CommitmentMismatch)
                } else {
                    Ok(())
                }
            },
            t + AGE * 2,
        );
        assert!(r.is_err());
        assert_eq!(seqs(&q), vec![100, 160, 220]);
    }

    #[test]
    fn stale_newest_candidate_is_evicted_by_age() {
        let t = Instant::now();
        let mut q = queue(5);
        // Unreachable newest candidate: while it is queued, lower seq_no pushes are ignored.
        q.push(set(1000), SUPPLIER, t);
        assert_eq!(
            q.push(set(160), SUPPLIER, t + Duration::from_secs(1)),
            PushOutcome::NotNewerThanQueued
        );
        let r = q.resolve(|t, n| found(t, n, &[]), |_, _| Ok(()), t + AGE).unwrap();
        // Exactly max_age old: not yet expired.
        assert!(r.expired.is_empty());
        let r = q
            .resolve(|t, n| found(t, n, &[]), |_, _| Ok(()), t + AGE + Duration::from_secs(1))
            .unwrap();
        assert_eq!(r.expired, vec![(1000, SUPPLIER.to_string())]);
        assert!(r.hit.is_none());
        assert!(q.is_empty());
        // The queue accepts lower seq_no candidates again.
        assert_eq!(
            q.push(set(160), SUPPLIER, t + AGE + Duration::from_secs(2)),
            PushOutcome::Queued { evicted: None }
        );
    }

    #[test]
    fn old_candidate_with_found_anchor_is_hit_not_expired() {
        let t = Instant::now();
        let mut q = queue(5);
        q.push(set(100), SUPPLIER, t);
        let r = q
            .resolve(|t, n| found(t, n, &[100]), |_, _| Ok(()), t + AGE + Duration::from_secs(1))
            .unwrap();
        assert_eq!(r.hit.map(|(s, _)| s.seq_no), Some(100));
        assert!(r.expired.is_empty());
        assert!(q.is_empty());
    }

    #[test]
    fn invalid_anchor_rejects_immediately() {
        let t = Instant::now();
        let mut q = fill(t);
        let r = q
            .resolve(
                |_, n| {
                    Ok(match n {
                        220 => AnchorLookup::Invalid("dup".into()),
                        160 => AnchorLookup::Found(anchor()),
                        _ => AnchorLookup::Missing,
                    })
                },
                |_, _| Ok(()),
                t,
            )
            .unwrap();
        assert_eq!(
            r.rejected,
            vec![(220, BkSetRejection::BadAnchor("dup".into()), SUPPLIER.to_string())]
        );
        assert_eq!(r.hit.map(|(s, _)| s.seq_no), Some(160));
        assert_eq!(q.len(), 0);
    }
}
