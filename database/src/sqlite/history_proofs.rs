// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! The history proofs store: a non-rotated SQLite file with the proof fields
//! of every thread-0 block and the events of those blocks: bridge
//! `WithdrawalInitiated` and RootPN `VoucherGenerated`. Rows are write-once: a
//! different content at an occupied height is a conflict.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use proof_data::FormatError;
use rusqlite::params;
use rusqlite::OpenFlags;
use rusqlite::OptionalExtension;
use rusqlite::TransactionBehavior;
use tvm_block::CommonMsgInfo;
use tvm_block::Deserializable;
use tvm_block::Message;

use super::history_bk_set;
use super::history_bk_set::AttestationWrite;
use super::history_bk_set::BkSetUpdateStatus;
use super::history_bk_set::BkSetUpdateWrite;
use super::history_bk_set::MismatchKind;
use super::history_ranges;
use super::history_ranges::RangeTable;
use super::ArchAttestation;
use super::ArchBkSetUpdate;
use super::ArchBlock;

/// File name of the history proofs store, next to the block archive in the
/// data directory. It must not match `bm-archive-*.db`: gql-server archive
/// discovery and the Ansible rotation tasks select files by that pattern.
pub const HISTORY_PROOFS_DB: &str = "bm-history-proofs.db";

/// Schema version in `PRAGMA user_version`. The versions of builds before the
/// first release are collapsed into 1, and any other value is refused. A
/// version-1 file of the original branch (`feature/persistent-history-proofs`,
/// layer roots only) is taken for this schema and fails on its first write:
/// delete such a file before running this build.
pub const SCHEMA_VERSION: i64 = 1;

/// How long a write waits for another writer (the verifier, `import`) before
/// it fails. A failed write is fatal for block-manager.
pub const HISTORY_PROOFS_BUSY_TIMEOUT: Duration = Duration::from_secs(30);

/// The only thread whose blocks are stored.
pub const THREAD_ZERO_HEX: &str =
    "00000000000000000000000000000000000000000000000000000000000000000000";

const SCHEMA: &str = "
CREATE TABLE blocks (
  height INTEGER PRIMARY KEY,
  block_id BLOB NOT NULL UNIQUE CHECK (length(block_id) = 32),
  envelope_hash BLOB NOT NULL CHECK (length(envelope_hash) = 32),
  tracked_ext_out_messages_root BLOB NOT NULL CHECK (length(tracked_ext_out_messages_root) = 32),
  tracked_ext_out_message_hashes BLOB,
  history_proofs BLOB,
  block_merkle_leaves BLOB CHECK (block_merkle_leaves IS NULL OR length(block_merkle_leaves) = 512),
  gen_utime INTEGER);
CREATE TABLE events (
  msg_hash BLOB PRIMARY KEY CHECK (length(msg_hash) = 32),
  kind TEXT NOT NULL CHECK (kind IN ('withdrawal', 'voucher')),
  height INTEGER NOT NULL, block_id BLOB NOT NULL, transaction_id TEXT NOT NULL,
  boc BLOB NOT NULL, src TEXT NOT NULL, dst TEXT NOT NULL,
  created_at INTEGER, src_dapp_id TEXT, msg_type INTEGER,
  routing_dapp_id BLOB NOT NULL CHECK (length(routing_dapp_id) = 32),
  sk_u_commit BLOB CHECK ((kind = 'voucher') = (sk_u_commit IS NOT NULL)
                          AND (sk_u_commit IS NULL OR length(sk_u_commit) = 32))
) WITHOUT ROWID;
CREATE INDEX events_height ON events(height);
CREATE INDEX events_sk_u_commit ON events(sk_u_commit) WHERE sk_u_commit IS NOT NULL;
CREATE TABLE coverage (start INTEGER PRIMARY KEY, end INTEGER NOT NULL);
CREATE TABLE verified (start INTEGER PRIMARY KEY, end INTEGER NOT NULL);
CREATE TABLE check_state (height INTEGER PRIMARY KEY, done INTEGER NOT NULL, failed INTEGER NOT NULL);
CREATE TABLE bk_set_updates (
  block_height INTEGER PRIMARY KEY,
  block_id TEXT NOT NULL UNIQUE,
  thread_id TEXT NOT NULL,
  height BLOB NOT NULL CHECK (length(height) = 8),
  chain_order TEXT NOT NULL,
  bk_set_update BLOB NOT NULL);
CREATE TABLE attestations (
  block_id TEXT NOT NULL,
  parent_block_id TEXT NOT NULL,
  envelope_hash BLOB NOT NULL CHECK (length(envelope_hash) = 32),
  target_type INTEGER NOT NULL CHECK (target_type IN (0, 1)),
  aggregated_signature BLOB NOT NULL,
  signature_occurrences BLOB NOT NULL,
  source_block_id TEXT NOT NULL,
  source_chain_order TEXT NOT NULL,
  target_height INTEGER NOT NULL,
  PRIMARY KEY (block_id, target_type)) WITHOUT ROWID;
CREATE INDEX attestations_source ON attestations(source_block_id);
CREATE INDEX attestations_target_height ON attestations(target_height);
";

/// Bridge routing (`dapp_id::account_id`) of Exchange / `eccUSDCBridge`.
pub const DEFAULT_BRIDGE_ROUTING: &str = "0000000000000000000000000000000000000000000000000000000000000000::1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";
/// `dst` of `WithdrawalInitiated` events (`makeAddrExtern(618)`).
pub const DEFAULT_BRIDGE_EVENT_DST: &str =
    ":000000000000000000000000000000000000000000000000000000000000026a";

/// How many blocks after a stored block its attestation may still arrive.
/// Later, the block is counted in `attestation_missing`.
pub const ATTESTATION_WAIT_BLOCKS: u64 = 256;

/// RootPN routing (`dapp_id::account_id`), the node's `DEFAULT_TRACKED_ROOT_PN_ROUTING`.
pub const DEFAULT_VOUCHER_ROUTING: &str = "0000000000000000000000000000000000000000000000000000000000000004::1010101010101010101010101010101010101010101010101010101010101010";
/// `dst` of `VoucherGenerated` events (`makeAddrExtern(135)`).
pub const DEFAULT_VOUCHER_EVENT_DST: &str =
    ":0000000000000000000000000000000000000000000000000000000000000087";
/// Event id of `VoucherGenerated(uint256,uint256,uint32)`, ABI 2: the first
/// 32 bits of the event body.
pub const VOUCHER_GENERATED_EVENT_ID: u32 = 0x79e3_031c;

/// The kinds of events the store keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EventKind {
    /// Bridge `WithdrawalInitiated`.
    Withdrawal,
    /// RootPN `VoucherGenerated`, stored with its `skUCommit`.
    Voucher,
}

impl EventKind {
    pub const ALL: [EventKind; 2] = [EventKind::Withdrawal, EventKind::Voucher];

    /// The value of the `kind` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKind::Withdrawal => "withdrawal",
            EventKind::Voucher => "voucher",
        }
    }

    pub fn parse(kind: &str) -> Option<EventKind> {
        EventKind::ALL.into_iter().find(|k| k.as_str() == kind)
    }
}

/// Which ext-out messages are events of one kind: sent by the routing's
/// account to the event `dst`. Not selected by the node's tracked list: an
/// event the node did not track is still stored and reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventFilter {
    pub routing_dapp_id: [u8; 32],
    pub routing_account_id: [u8; 32],
    /// The 256-bit external address the events are sent to.
    pub dst_address: [u8; 32],
}

impl EventFilter {
    fn from_defaults(routing: &str, dst: &str) -> Self {
        let (routing_dapp_id, routing_account_id) =
            parse_routing(routing).expect("default event routing is valid");
        let dst_address = parse_event_dst(dst).expect("default event dst is valid");
        Self { routing_dapp_id, routing_account_id, dst_address }
    }

    /// Bridge `WithdrawalInitiated`: Exchange / `eccUSDCBridge` to `makeAddrExtern(618)`.
    pub fn withdrawal_default() -> Self {
        Self::from_defaults(DEFAULT_BRIDGE_ROUTING, DEFAULT_BRIDGE_EVENT_DST)
    }

    /// RootPN `VoucherGenerated` to `makeAddrExtern(135)`.
    pub fn voucher_default() -> Self {
        Self::from_defaults(DEFAULT_VOUCHER_ROUTING, DEFAULT_VOUCHER_EVENT_DST)
    }

    /// The event `src` as `ArchMessage` writes it: `0:<account_id hex>`.
    pub fn src(&self) -> String {
        format!("0:{}", hex::encode(self.routing_account_id))
    }

    /// The event `dst` as `ArchMessage` writes it (`MsgAddressExt::to_string`):
    /// `:<lowercase hex>`.
    pub fn dst(&self) -> String {
        format!(":{}", hex::encode(self.dst_address))
    }
}

/// The filter of each event kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventFilters {
    pub withdrawal: EventFilter,
    pub voucher: EventFilter,
}

impl Default for EventFilters {
    fn default() -> Self {
        Self {
            withdrawal: EventFilter::withdrawal_default(),
            voucher: EventFilter::voucher_default(),
        }
    }
}

impl EventFilters {
    /// Refuses two kinds with the same `src` and `dst`: one message cannot be both.
    pub fn new(withdrawal: EventFilter, voucher: EventFilter) -> Result<Self, String> {
        if withdrawal.src() == voucher.src() && withdrawal.dst() == voucher.dst() {
            return Err(format!(
                "bridge and voucher events have the same source {} and dst {}",
                withdrawal.src(),
                withdrawal.dst()
            ));
        }
        Ok(Self { withdrawal, voucher })
    }

    pub fn get(&self, kind: EventKind) -> &EventFilter {
        match kind {
            EventKind::Withdrawal => &self.withdrawal,
            EventKind::Voucher => &self.voucher,
        }
    }

    /// The kind whose filter selects a message with `src` and `dst`.
    pub fn kind_of(&self, src: &str, dst: &str) -> Option<(EventKind, &EventFilter)> {
        EventKind::ALL
            .into_iter()
            .map(|kind| (kind, self.get(kind)))
            .find(|(_, f)| f.src() == src && f.dst() == dst)
    }
}

/// A number per event kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventCounts {
    pub withdrawal: u64,
    pub voucher: u64,
}

impl EventCounts {
    pub fn add(&mut self, kind: EventKind, n: u64) {
        match kind {
            EventKind::Withdrawal => self.withdrawal += n,
            EventKind::Voucher => self.voucher += n,
        }
    }

    pub fn get(&self, kind: EventKind) -> u64 {
        match kind {
            EventKind::Withdrawal => self.withdrawal,
            EventKind::Voucher => self.voucher,
        }
    }

    pub fn total(&self) -> u64 {
        self.withdrawal + self.voucher
    }
}

/// 64 hex digits in any letter case as 32 bytes; `None` for anything else.
pub(crate) fn decode_hex32(digits: &str) -> Option<[u8; 32]> {
    hex::decode(digits).ok()?.try_into().ok()
}

/// Parses the `dst` of events: an external address of 256 bits, `:`
/// and 64 hex digits in any letter case. A shorter address is another
/// address (`:26a` is 12 bits) and would select nothing.
pub fn parse_event_dst(dst: &str) -> Result<[u8; 32], String> {
    dst.strip_prefix(':')
        .and_then(decode_hex32)
        .ok_or_else(|| {
            format!(
                "expected a 256-bit external address, `:` and 64 hex digits like {DEFAULT_BRIDGE_EVENT_DST}, got {dst:?}"
            )
        })
}

/// Parses `<dapp_id hex>::<account_id hex>`, 64 hex digits each.
pub fn parse_routing(routing: &str) -> Result<([u8; 32], [u8; 32]), String> {
    routing
        .split_once("::")
        .and_then(|(dapp, account)| Some((decode_hex32(dapp)?, decode_hex32(account)?)))
        .ok_or_else(|| {
            format!("expected <dapp_id>::<account_id>, 64 hex digits each, got {routing:?}")
        })
}

/// Events of a block, and per kind the number of matching messages that could
/// not become a row: no transaction id or no BOC, or, for a voucher, a body
/// that is not `VoucherGenerated`.
pub fn events_of(b: &ArchBlock, f: &EventFilters) -> (Vec<EventRow>, EventCounts) {
    let height = u64::from_be_bytes(b.height);
    let mut skipped = EventCounts::default();
    let Some(block_id) = decode_hex32(&b.id) else {
        // The row is rejected for the same reason; nothing to attach events to.
        return (vec![], skipped);
    };
    let selectors: Vec<(EventKind, String, String, [u8; 32])> = EventKind::ALL
        .into_iter()
        .map(|kind| {
            let filter = f.get(kind);
            (kind, filter.src(), filter.dst(), filter.routing_dapp_id)
        })
        .collect();
    let mut events = Vec::new();
    for message in &b.ext_out_messages {
        let Some((kind, src, dst, routing_dapp_id)) = selectors.iter().find(|(_, src, dst, _)| {
            message.src.as_deref() == Some(src.as_str())
                && message.dst.as_deref() == Some(dst.as_str())
        }) else {
            continue;
        };
        let msg_hash = decode_hex32(&message.id);
        let (Some(msg_hash), Some(transaction_id), Some(boc)) =
            (msg_hash, &message.transaction_id, &message.boc)
        else {
            tracing::error!(
                target: "sqlite",
                "{} event {} of block {} at height {height} has no transaction id or BOC, not stored in {HISTORY_PROOFS_DB}",
                kind.as_str(),
                message.id,
                b.id
            );
            skipped.add(*kind, 1);
            continue;
        };
        let sk_u_commit = match kind {
            EventKind::Withdrawal => None,
            EventKind::Voucher => match voucher_sk_u_commit(boc) {
                Ok(sk) => Some(sk),
                Err(err) => {
                    tracing::error!(
                        target: "sqlite",
                        "voucher event {} of block {} at height {height} is not stored in {HISTORY_PROOFS_DB}: {err}",
                        message.id,
                        b.id
                    );
                    skipped.add(*kind, 1);
                    continue;
                }
            },
        };
        events.push(EventRow {
            msg_hash,
            kind: *kind,
            height,
            block_id,
            transaction_id: transaction_id.clone(),
            boc: boc.clone(),
            src: src.clone(),
            dst: dst.clone(),
            created_at: message.created_at,
            src_dapp_id: message.src_dapp_id.clone(),
            msg_type: message.msg_type,
            routing_dapp_id: *routing_dapp_id,
            sk_u_commit,
        });
    }
    (events, skipped)
}

/// `skUCommit` of a `VoucherGenerated` message: the 256 bits after the event
/// id at the start of the body.
pub fn voucher_sk_u_commit(boc: &[u8]) -> Result<[u8; 32], String> {
    let cell = tvm_types::read_single_root_boc(boc).map_err(|err| format!("bad BOC: {err}"))?;
    let message =
        Message::construct_from_cell(cell).map_err(|err| format!("BOC is not a message: {err}"))?;
    let mut body = message.body().ok_or_else(|| "the message has no body".to_string())?;
    let id = body.get_next_u32().map_err(|_| "the body is shorter than an event id".to_string())?;
    if id != VOUCHER_GENERATED_EVENT_ID {
        return Err(format!(
            "event id {id:#010x} of the body is not VoucherGenerated ({VOUCHER_GENERATED_EVENT_ID:#010x})"
        ));
    }
    body.get_next_hash()
        .map(|sk| sk.inner())
        .map_err(|_| "the body ends before skUCommit".to_string())
}

/// The event of an ext-out message BOC, with the fields the archive takes from
/// the message header (`ArchMessage::from`); `routing_dapp_id` is not in the
/// message and is taken as given. For a voucher also `skUCommit` from the
/// body; for a withdrawal only the header is read, so a crafted body cannot
/// make it fail.
pub fn event_from_boc(
    boc: &[u8],
    kind: EventKind,
    height: u64,
    block_id: [u8; 32],
    transaction_id: String,
    routing_dapp_id: [u8; 32],
) -> Result<EventRow, String> {
    let cell = tvm_types::read_single_root_boc(boc).map_err(|err| format!("bad BOC: {err}"))?;
    let hash = cell.repr_hash();
    let message =
        Message::construct_from_cell(cell).map_err(|err| format!("BOC is not a message: {err}"))?;
    let (msg_type, src, dst, created_at, src_dapp_id) = match message.header() {
        CommonMsgInfo::ExtOutMsgInfo(h) => {
            (2, h.src.to_string(), h.dst.to_string(), h.created_at.as_u32(), None)
        }
        CommonMsgInfo::ExtOutMsgInfoV2(h) => (
            4,
            h.src.to_string(),
            h.dst.to_string(),
            h.created_at.as_u32(),
            h.src_dapp_id.as_ref().map(|id| id.to_hex_string()),
        ),
        _ => return Err("BOC is not an ext-out message".to_string()),
    };
    let sk_u_commit = match kind {
        EventKind::Withdrawal => None,
        EventKind::Voucher => Some(voucher_sk_u_commit(boc)?),
    };
    Ok(EventRow {
        msg_hash: hash.inner(),
        kind,
        height,
        block_id,
        transaction_id,
        boc: boc.to_vec(),
        src,
        dst,
        created_at: Some(created_at),
        src_dapp_id,
        msg_type: Some(msg_type),
        routing_dapp_id,
        sk_u_commit,
    })
}

/// Checks that `msg_hash`, `src`, `dst`, `created_at`, `src_dapp_id` and
/// `msg_type` of a stored event, and `skUCommit` of a voucher, are the ones
/// its BOC holds. The current flags play no part: an event stored before a
/// flag change stays good.
pub fn check_event_boc(event: &EventRow) -> Result<(), String> {
    let expected = event_from_boc(
        &event.boc,
        event.kind,
        event.height,
        event.block_id,
        event.transaction_id.clone(),
        event.routing_dapp_id,
    )?;
    let mut differ = Vec::new();
    if expected.msg_hash != event.msg_hash {
        differ.push(format!("BOC hash {}", hex::encode(expected.msg_hash)));
    }
    if expected.src != event.src {
        differ.push(format!("src {}", expected.src));
    }
    if expected.dst != event.dst {
        differ.push(format!("dst {}", expected.dst));
    }
    if expected.created_at != event.created_at {
        differ.push(format!("created_at {:?}", expected.created_at));
    }
    if expected.src_dapp_id != event.src_dapp_id {
        differ.push(format!("src_dapp_id {:?}", expected.src_dapp_id));
    }
    if expected.msg_type != event.msg_type {
        differ.push(format!("msg_type {:?}", expected.msg_type));
    }
    if expected.sk_u_commit != event.sk_u_commit {
        differ.push(format!(
            "skUCommit {}",
            expected.sk_u_commit.map(hex::encode).unwrap_or_default()
        ));
    }
    if differ.is_empty() {
        Ok(())
    } else {
        Err(format!("the BOC has {}", differ.join(", ")))
    }
}

/// Declares `HistoryProofsCounters` and its reading
/// `HistoryProofsCountersSnapshot` from one field list, so that `snapshot` and
/// `since` cover every counter.
macro_rules! history_proofs_counters {
    ($($(#[$doc:meta])* $field:ident,)*) => {
        /// Outcomes of history store writes in the writer thread, shared by all
        /// clones of the helper.
        #[derive(Clone, Default)]
        pub struct HistoryProofsCounters {
            $($(#[$doc])* pub $field: Arc<AtomicU64>,)*
        }

        /// A reading of `HistoryProofsCounters`.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub struct HistoryProofsCountersSnapshot {
            $($(#[$doc])* pub $field: u64,)*
        }

        impl HistoryProofsCounters {
            pub fn snapshot(&self) -> HistoryProofsCountersSnapshot {
                HistoryProofsCountersSnapshot {
                    $($field: self.$field.load(Ordering::Relaxed),)*
                }
            }
        }

        impl HistoryProofsCountersSnapshot {
            /// Growth since `earlier`.
            pub fn since(&self, earlier: &Self) -> Self {
                Self { $($field: self.$field.saturating_sub(earlier.$field),)* }
            }
        }
    };
}

history_proofs_counters! {
    rows_written,
    duplicates,
    conflicts,
    rejected,
    seq_no_mismatch,
    withdrawal_events,
    voucher_events,
    withdrawal_events_skipped,
    voucher_events_skipped,
    attestation_mismatch,
    attestation_missing,
    bk_set_updates_written,
    attestations_written,
    attestations_replaced,
    /// Attestations not written for a wrong parent or block id, and
    /// attestations deleted when the row below the block they sign arrived
    /// and is not their parent. Envelope hash mismatches are counted in
    /// `attestation_mismatch`.
    attestations_rejected,
    /// Events of a written block that are not stored: the file holds their
    /// message at another height. A block sent again counts them again.
    withdrawal_event_conflicts,
    voucher_event_conflicts,
}

/// One thread-0 block as stored in `blocks`.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryRow {
    pub height: u64,
    pub block_id: [u8; 32],
    pub envelope_hash: [u8; 32],
    pub tracked_root: [u8; 32],
    /// `None` when the block tracks no ext-out messages.
    pub tracked_hashes: Option<Vec<u8>>,
    /// `None` when the block carries no layer roots.
    pub history_proofs: Option<Vec<u8>>,
    /// Kept for layer boundaries and BK set updates only.
    pub block_merkle_leaves: Option<Vec<u8>>,
    pub gen_utime: Option<i64>,
}

/// One event as stored in `events`.
#[derive(Clone, Debug, PartialEq)]
pub struct EventRow {
    pub msg_hash: [u8; 32],
    pub kind: EventKind,
    pub height: u64,
    pub block_id: [u8; 32],
    pub transaction_id: String,
    pub boc: Vec<u8>,
    pub src: String,
    pub dst: String,
    pub created_at: Option<u32>,
    pub src_dapp_id: Option<String>,
    pub msg_type: Option<u8>,
    /// The dApp of the kind's routing flag when the event was stored. With
    /// the account of `src`, it names the group of the tracked hashes the
    /// event is checked in: not the current flag, which can change since.
    pub routing_dapp_id: [u8; 32],
    /// `skUCommit` of a voucher; `None` for a withdrawal.
    pub sk_u_commit: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    /// The same row is already stored.
    Duplicate,
    /// A different row holds this height or this block id. Nothing changed.
    Conflict {
        existing_height: u64,
        existing_block_id: [u8; 32],
    },
}

/// What happened to the attestations written with a block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttestationWrites {
    pub inserted: u64,
    pub replaced: u64,
    pub kept: u64,
    pub not_kept: u64,
    pub mismatches: Vec<(MismatchKind, String)>,
    /// Attestations of the block above deleted because the new row is not
    /// their parent (`history_bk_set::detach_attestations_above`).
    pub detached: Vec<String>,
}

impl AttestationWrites {
    pub fn add(&mut self, w: AttestationWrite) {
        match w {
            AttestationWrite::Inserted => self.inserted += 1,
            AttestationWrite::Replaced => self.replaced += 1,
            AttestationWrite::Kept => self.kept += 1,
            AttestationWrite::NotKept => self.not_kept += 1,
            AttestationWrite::Mismatch(kind, reason) => self.mismatches.push((kind, reason)),
        }
    }
}

/// What was written with a block row that is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub bk_set_update: Option<BkSetUpdateWrite>,
    pub attestations: AttestationWrites,
    /// The block's events stored by this write: all of a new row, the ones a
    /// stored row did not have yet.
    pub events_inserted: EventCounts,
    /// `(kind, msg_hash, height)` of the block's events that are not stored because
    /// the event at `height` holds the message. A message comes in one block
    /// only (its hash covers the account's `created_lt`), so this happens on
    /// damaged or foreign data only. `import` refuses such a row instead, so
    /// for it this is always empty.
    pub skipped_events: Vec<(EventKind, [u8; 32], u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockWrite {
    Inserted(Written),
    /// The same row was already stored.
    Duplicate(Written),
    /// Nothing written: another row holds the height or the block id.
    Conflict {
        existing_height: u64,
        existing_block_id: [u8; 32],
    },
    /// Nothing written: another BK set update holds the height or the block id.
    BkSetUpdateConflict {
        height: u64,
    },
    /// Nothing written: the update does not belong to the row, or a
    /// rotation-shaped row came without one (`check_bk_set_update_of`).
    BkSetUpdateRejected(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreResult {
    NotThreadZero,
    /// The block's proof fields do not parse; nothing was written.
    Rejected(FormatError),
    Stored(BlockWrite),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportMode {
    /// Rows at heights that already have a row are skipped with their events
    /// and BK set updates; attestations are imported in both modes
    /// (`import_attestations`).
    GapsOnly,
    /// Every row is compared with the stored one.
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportRowStatus {
    Inserted,
    Duplicate,
    OutsideGaps,
    /// Another block at the height, the block at another height, or another
    /// BK set update: what differs.
    Conflict(String),
    Rejected(FormatError),
    /// An event of the row does not match its BOC, or names another block or
    /// height; nothing of the row is written.
    RejectedEvent(String),
    /// The BK set update does not belong to the row, or a rotation-shaped row
    /// has none (`history_bk_set::check_bk_set_update_of`); nothing of the
    /// row is written.
    RejectedBkSetUpdate(String),
}

/// A row to import with its bridge events and BK set update.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportItem {
    pub row: HistoryRow,
    pub events: Vec<EventRow>,
    pub bk_set_update: Option<ArchBkSetUpdate>,
}

impl From<(HistoryRow, Vec<EventRow>)> for ImportItem {
    fn from((row, events): (HistoryRow, Vec<EventRow>)) -> Self {
        Self { row, events, bk_set_update: None }
    }
}

/// What `import_batch` did with an item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedRow {
    pub status: ImportRowStatus,
    /// Set for `Inserted` and `Duplicate` rows with a BK set update.
    pub bk_set_update: Option<BkSetUpdateWrite>,
    /// Attestations of the block above deleted because the inserted row is not
    /// their parent.
    pub detached: Vec<String>,
}

/// An attestation as stored, with the height of the block it signs.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredAttestation {
    pub attestation: ArchAttestation,
    pub target_height: u64,
}

/// Builds the stored row of a block. `None` for blocks of other threads.
pub fn history_row_from_arch_block(b: &ArchBlock) -> Result<Option<HistoryRow>, FormatError> {
    if b.thread_id.as_deref() != Some(THREAD_ZERO_HEX) {
        return Ok(None);
    }
    let height = u64::from_be_bytes(b.height);
    let block_id = decode_hex32(&b.id).ok_or(FormatError::BadLength {
        field: "block_id",
        expected: 64,
        got: b.id.len(),
    })?;
    // The node writes `[0x00]` (n = 0) for blocks without layer roots.
    let history_proofs = match b.history_proofs.as_slice() {
        [] | [0] => None,
        blob => Some(blob.to_vec()),
    };
    let tracked_hashes = if b.tracked_ext_out_message_hashes.is_empty()
        || proof_data::tracked::decode_tracked_hashes(&b.tracked_ext_out_message_hashes)?.is_empty()
    {
        None
    } else {
        Some(b.tracked_ext_out_message_hashes.clone())
    };
    let keep_leaves =
        height.is_multiple_of(proof_data::layers::WINDOW) || b.bk_set_update.is_some();
    let block_merkle_leaves =
        (keep_leaves && !b.block_merkle_leaves.is_empty()).then(|| b.block_merkle_leaves.clone());
    let row = HistoryRow {
        height,
        block_id,
        envelope_hash: b.envelope_hash,
        tracked_root: b.tracked_ext_out_messages_root,
        tracked_hashes,
        history_proofs,
        block_merkle_leaves,
        gen_utime: b.gen_utime,
    };
    validate_row(&row)?;
    Ok(Some(row))
}

/// Format checks of a row: the layer set matches the height, the tracked
/// hashes decode, the Merkle leaves have the right length.
pub fn validate_row(row: &HistoryRow) -> Result<(), FormatError> {
    proof_data::layers::check_history_proofs_for_height(row.height, row.history_proofs.as_deref())?;
    if let Some(tracked) = &row.tracked_hashes {
        proof_data::tracked::decode_tracked_hashes(tracked)?;
    }
    if let Some(leaves) = &row.block_merkle_leaves {
        proof_data::leaves::parse_block_merkle_leaves(leaves)?;
    }
    Ok(())
}

pub struct HistoryProofsStore {
    conn: rusqlite::Connection,
    path: PathBuf,
}

impl HistoryProofsStore {
    /// Opens or creates `bm-history-proofs.db` in `data_dir`.
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        Self::open_with_busy_timeout(data_dir, HISTORY_PROOFS_BUSY_TIMEOUT)
    }

    pub fn open_with_busy_timeout(data_dir: &Path, busy_timeout: Duration) -> anyhow::Result<Self> {
        Self::open_file(&data_dir.join(HISTORY_PROOFS_DB), busy_timeout, true)
    }

    /// Opens a store file at `path`. With `create`, a missing file or one with
    /// `user_version` 0 gets the schema; without it, the file must exist and
    /// have the current schema. A missing file is built next to `path` and
    /// appears there only complete: gql-server and the Ansible healthcheck
    /// look for the file and must not find it half created.
    pub fn open_file(path: &Path, busy_timeout: Duration, create: bool) -> anyhow::Result<Self> {
        if create && !path.exists() {
            let temp = creating_path(path);
            if let Err(err) = build_store_file(&temp) {
                remove_store_files(&temp);
                return Err(err);
            }
            publish_store_file(&temp, path)?;
        }
        let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        if create {
            flags |= OpenFlags::SQLITE_OPEN_CREATE;
        }
        let mut conn =
            rusqlite::Connection::open_with_flags(path, flags).map_err(|err| in_file(path, err))?;
        conn.busy_timeout(busy_timeout).map_err(|err| in_file(path, err))?;

        let user_version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(|err| in_file(path, err))?;
        match user_version {
            SCHEMA_VERSION => {}
            0 if create => {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|err| in_file(path, err))?;
                // Another process may have created the schema meanwhile.
                let current: i64 = tx
                    .query_row("PRAGMA user_version", [], |row| row.get(0))
                    .map_err(|err| in_file(path, err))?;
                if current == 0 {
                    tx.execute_batch(SCHEMA).map_err(|err| in_file(path, err))?;
                    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))
                        .map_err(|err| in_file(path, err))?;
                } else {
                    anyhow::ensure!(
                        current == SCHEMA_VERSION,
                        "{}: user_version {current}, expected {SCHEMA_VERSION}",
                        path.display()
                    );
                }
                tx.commit().map_err(|err| in_file(path, err))?;
            }
            other => anyhow::bail!(
                "{}: user_version {other}, expected {SCHEMA_VERSION}; not a history proofs store of this block-manager version",
                path.display()
            ),
        }

        // Stock WAL, not WAL2: the file must open with any `sqlite3`.
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .map_err(|err| in_file(path, err))?;
        anyhow::ensure!(
            journal_mode == "wal",
            "{}: journal_mode is {journal_mode}, expected wal",
            path.display()
        );
        conn.execute_batch("PRAGMA synchronous = FULL").map_err(|err| in_file(path, err))?;

        Ok(Self { conn, path: path.to_path_buf() })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn connection(&self) -> &rusqlite::Connection {
        &self.conn
    }

    /// `insert_block_with` without a BK set update and attestations.
    pub fn insert_block(
        &mut self,
        row: &HistoryRow,
        events: &[EventRow],
    ) -> anyhow::Result<BlockWrite> {
        self.insert_block_with(row, events, None, &[])
    }

    /// Writes a block row with its bridge events, its BK set update, the
    /// attestations it carries and the coverage update in one transaction.
    /// Every check runs before any write: when the row or the BK set update
    /// conflicts, or the update does not belong to the row, nothing is written.
    pub fn insert_block_with(
        &mut self,
        row: &HistoryRow,
        events: &[EventRow],
        bk_set_update: Option<&ArchBkSetUpdate>,
        attestations: &[ArchAttestation],
    ) -> anyhow::Result<BlockWrite> {
        let path = self.path.as_path();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| in_file(path, err))?;
        let result = (|| -> rusqlite::Result<BlockWrite> {
            let stored = compare_with_stored(&tx, row)?;
            if let Some(InsertOutcome::Conflict { existing_height, existing_block_id }) = stored {
                return Ok(BlockWrite::Conflict { existing_height, existing_block_id });
            }
            if let Err(reason) = history_bk_set::check_bk_set_update_of(row, bk_set_update) {
                return Ok(BlockWrite::BkSetUpdateRejected(reason));
            }
            let bk_set_update = match bk_set_update {
                None => None,
                Some(u) => match history_bk_set::bk_set_update_status(&tx, row.height, u)? {
                    BkSetUpdateStatus::Absent => Some((u, BkSetUpdateWrite::Inserted)),
                    BkSetUpdateStatus::Present => Some((u, BkSetUpdateWrite::Present)),
                    BkSetUpdateStatus::Conflict => {
                        return Ok(BlockWrite::BkSetUpdateConflict { height: row.height });
                    }
                },
            };
            let new_row = stored.is_none();
            let written =
                write_checked_block(&tx, row, new_row, events, bk_set_update, attestations)?;
            Ok(if new_row { BlockWrite::Inserted(written) } else { BlockWrite::Duplicate(written) })
        })()
        .map_err(|err| in_file(path, err))?;
        tx.commit().map_err(|err| in_file(path, err))?;
        Ok(result)
    }

    /// `history_row_from_arch_block` + `insert_block_with` with the block's BK
    /// set update and carried attestations. `Err` only for SQLite errors.
    pub fn store_arch_block(
        &mut self,
        b: &ArchBlock,
        events: &[EventRow],
    ) -> anyhow::Result<StoreResult> {
        match history_row_from_arch_block(b) {
            Ok(None) => Ok(StoreResult::NotThreadZero),
            Err(err) => Ok(StoreResult::Rejected(err)),
            Ok(Some(row)) => Ok(StoreResult::Stored(self.insert_block_with(
                &row,
                events,
                b.bk_set_update.as_ref(),
                &b.attestations,
            )?)),
        }
    }

    /// Imports a batch of rows with their events and BK set updates in one
    /// transaction. Attestations go through `import_attestations`.
    pub fn import_batch(
        &mut self,
        items: &[ImportItem],
        mode: ImportMode,
    ) -> anyhow::Result<Vec<ImportedRow>> {
        let path = self.path.as_path();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| in_file(path, err))?;
        let mut imported = Vec::with_capacity(items.len());
        for item in items {
            imported.push(import_item(&tx, item, mode).map_err(|err| in_file(path, err))?);
        }
        tx.commit().map_err(|err| in_file(path, err))?;
        Ok(imported)
    }

    /// Writes attestations of an export file by the store's rule
    /// (`history_bk_set::write_exported_attestation`) in one transaction,
    /// whatever the rows around them.
    pub fn import_attestations(
        &mut self,
        rows: &[StoredAttestation],
    ) -> anyhow::Result<Vec<AttestationWrite>> {
        let path = self.path.as_path();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| in_file(path, err))?;
        let writes = rows
            .iter()
            .map(|a| {
                history_bk_set::write_exported_attestation(&tx, &a.attestation, a.target_height)
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|err| in_file(path, err))?;
        tx.commit().map_err(|err| in_file(path, err))?;
        Ok(writes)
    }

    /// Copies attestations into an export file as they are, without the
    /// store's rule: the rule runs on the target, which has the rows the
    /// export file may lack. An attestation already there is kept.
    pub fn copy_attestations(&mut self, rows: &[StoredAttestation]) -> anyhow::Result<()> {
        let path = self.path.as_path();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| in_file(path, err))?;
        for row in rows {
            history_bk_set::copy_attestation(&tx, &row.attestation, row.target_height)
                .map_err(|err| in_file(path, err))?;
        }
        tx.commit().map_err(|err| in_file(path, err))?;
        Ok(())
    }

    /// One export batch, read in one read transaction so that a rotation row
    /// is never read without its BK set update: the rows of `start ..= end`
    /// with their events and BK set updates, and the attestations of
    /// `history_bk_set::attestations_to_export` (blocks of the range and of the
    /// `ATTESTATION_WAIT_BLOCKS` below it).
    pub fn read_export_batch(
        &self,
        start: u64,
        end: u64,
    ) -> anyhow::Result<(Vec<ImportItem>, Vec<StoredAttestation>)> {
        let tx = self.conn.unchecked_transaction().map_err(|err| in_file(&self.path, err))?;
        let rows = self.rows_in_range(start, end)?;
        let events = self.events_in_range(start, end)?;
        let mut updates: BTreeMap<u64, ArchBkSetUpdate> =
            history_bk_set::bk_set_updates_in_range(&tx, start, end)
                .map_err(|err| in_file(&self.path, err))?
                .into_iter()
                .collect();
        let attestations = history_bk_set::attestations_to_export(&tx, start, end)
            .map_err(|err| in_file(&self.path, err))?
            .into_iter()
            .map(|(attestation, target_height)| StoredAttestation { attestation, target_height })
            .collect();
        tx.commit().map_err(|err| in_file(&self.path, err))?;
        let mut events_by_height: BTreeMap<u64, Vec<EventRow>> = BTreeMap::new();
        for event in events {
            events_by_height.entry(event.height).or_default().push(event);
        }
        let items = rows
            .into_iter()
            .map(|row| ImportItem {
                events: events_by_height.remove(&row.height).unwrap_or_default(),
                bk_set_update: updates.remove(&row.height),
                row,
            })
            .collect();
        Ok((items, attestations))
    }

    pub fn row(&self, height: u64) -> anyhow::Result<Option<HistoryRow>> {
        stored_row(&self.conn, height).map_err(|err| in_file(&self.path, err))
    }

    pub fn row_by_block_id(&self, block_id: &[u8; 32]) -> anyhow::Result<Option<HistoryRow>> {
        stored_row_by_block_id(&self.conn, block_id).map_err(|err| in_file(&self.path, err))
    }

    pub fn rows_in_range(&self, start: u64, end: u64) -> anyhow::Result<Vec<HistoryRow>> {
        (|| {
            let mut stmt = self.conn.prepare_cached(&format!(
                "SELECT {ROW_COLUMNS} FROM blocks WHERE height BETWEEN ?1 AND ?2 ORDER BY height"
            ))?;
            let rows = stmt.query_map(params![start as i64, end as i64], read_row)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })()
        .map_err(|err| in_file(&self.path, err))
    }

    pub fn events_in_range(&self, start: u64, end: u64) -> anyhow::Result<Vec<EventRow>> {
        (|| {
            let mut stmt = self.conn.prepare_cached(
                "SELECT msg_hash, kind, height, block_id, transaction_id, boc, src, dst,
                        created_at, src_dapp_id, msg_type, routing_dapp_id, sk_u_commit
                 FROM events WHERE height BETWEEN ?1 AND ?2 ORDER BY height, msg_hash",
            )?;
            let rows = stmt.query_map(params![start as i64, end as i64], |r| {
                let kind: String = r.get(1)?;
                Ok(EventRow {
                    msg_hash: r.get(0)?,
                    // The CHECK of the column allows only the kinds.
                    kind: EventKind::parse(&kind).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            format!("unknown event kind {kind:?}").into(),
                        )
                    })?,
                    height: r.get::<_, i64>(2)? as u64,
                    block_id: r.get(3)?,
                    transaction_id: r.get(4)?,
                    boc: r.get(5)?,
                    src: r.get(6)?,
                    dst: r.get(7)?,
                    created_at: r.get(8)?,
                    src_dapp_id: r.get(9)?,
                    msg_type: r.get(10)?,
                    routing_dapp_id: r.get(11)?,
                    sk_u_commit: r.get(12)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })()
        .map_err(|err| in_file(&self.path, err))
    }
}

const ROW_COLUMNS: &str = "height, block_id, envelope_hash, tracked_ext_out_messages_root,
    tracked_ext_out_message_hashes, history_proofs, block_merkle_leaves, gen_utime";

fn read_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryRow> {
    Ok(HistoryRow {
        height: r.get::<_, i64>(0)? as u64,
        block_id: r.get(1)?,
        envelope_hash: r.get(2)?,
        tracked_root: r.get(3)?,
        tracked_hashes: r.get(4)?,
        history_proofs: r.get(5)?,
        block_merkle_leaves: r.get(6)?,
        gen_utime: r.get(7)?,
    })
}

fn stored_row(conn: &rusqlite::Connection, height: u64) -> rusqlite::Result<Option<HistoryRow>> {
    conn.prepare_cached(&format!("SELECT {ROW_COLUMNS} FROM blocks WHERE height = ?1"))?
        .query_row(params![height as i64], read_row)
        .optional()
}

fn stored_row_by_block_id(
    conn: &rusqlite::Connection,
    block_id: &[u8; 32],
) -> rusqlite::Result<Option<HistoryRow>> {
    conn.prepare_cached(&format!("SELECT {ROW_COLUMNS} FROM blocks WHERE block_id = ?1"))?
        .query_row(params![block_id], read_row)
        .optional()
}

/// What differs between `row` and the stored row it conflicts with.
fn conflict_reason(
    conn: &rusqlite::Connection,
    row: &HistoryRow,
    existing_height: u64,
) -> rusqlite::Result<String> {
    if existing_height != row.height {
        return Ok(format!(
            "block {} is stored at height {existing_height}, the file has it at height {}",
            hex::encode(row.block_id),
            row.height
        ));
    }
    let Some(stored) = stored_row(conn, row.height)? else {
        return Ok(format!("the row at height {} changed during the import", row.height));
    };
    // Every field by name: a new field of the row does not build until it is
    // compared here.
    let HistoryRow {
        height: _,
        block_id,
        envelope_hash,
        tracked_root,
        tracked_hashes,
        history_proofs,
        block_merkle_leaves,
        gen_utime,
    } = &stored;
    let fields: Vec<&str> = [
        ("block_id", *block_id != row.block_id),
        ("envelope_hash", *envelope_hash != row.envelope_hash),
        ("tracked_ext_out_messages_root", *tracked_root != row.tracked_root),
        ("tracked_ext_out_message_hashes", *tracked_hashes != row.tracked_hashes),
        ("history_proofs", *history_proofs != row.history_proofs),
        ("block_merkle_leaves", *block_merkle_leaves != row.block_merkle_leaves),
        ("gen_utime", *gen_utime != row.gen_utime),
    ]
    .into_iter()
    .filter_map(|(name, differs)| differs.then_some(name))
    .collect();
    Ok(if stored.block_id == row.block_id {
        format!(
            "block {} at height {} differs from the stored row in {}",
            hex::encode(row.block_id),
            row.height,
            fields.join(", ")
        )
    } else {
        format!(
            "the row at height {} is block {}, the file has block {}; they differ in {}",
            row.height,
            hex::encode(stored.block_id),
            hex::encode(row.block_id),
            fields.join(", ")
        )
    })
}

/// `None` when neither the height nor the block id is taken.
fn compare_with_stored(
    conn: &rusqlite::Connection,
    row: &HistoryRow,
) -> rusqlite::Result<Option<InsertOutcome>> {
    if let Some(existing) = stored_row(conn, row.height)? {
        return Ok(Some(if existing == *row {
            InsertOutcome::Duplicate
        } else {
            InsertOutcome::Conflict {
                existing_height: existing.height,
                existing_block_id: existing.block_id,
            }
        }));
    }
    Ok(stored_row_by_block_id(conn, &row.block_id)?.map(|existing| InsertOutcome::Conflict {
        existing_height: existing.height,
        existing_block_id: existing.block_id,
    }))
}

/// The writes of a block whose checks passed: the row (when `new_row`) with
/// its coverage, deleting the attestations above it that name another parent,
/// the events (but those whose message is stored at another height: they go
/// to `Written::skipped_events`), the BK set update (inserted when its write
/// is `Inserted`) and the carried attestations.
fn write_checked_block(
    conn: &rusqlite::Connection,
    row: &HistoryRow,
    new_row: bool,
    events: &[EventRow],
    bk_set_update: Option<(&ArchBkSetUpdate, BkSetUpdateWrite)>,
    attestations: &[ArchAttestation],
) -> rusqlite::Result<Written> {
    let mut writes = AttestationWrites::default();
    let mut kept = Vec::with_capacity(events.len());
    let mut skipped_events = Vec::new();
    for event in events {
        match stored_event_height(conn, &event.msg_hash)? {
            Some(height) if height != row.height => {
                skipped_events.push((event.kind, event.msg_hash, height))
            }
            _ => kept.push(event.clone()),
        }
    }
    let events = kept.as_slice();
    let events_inserted = if new_row {
        insert_row(conn, row)?;
        let inserted = insert_events(conn, events)?;
        history_ranges::add_height(conn, RangeTable::Coverage, row.height)?;
        writes.detached = history_bk_set::detach_attestations_above(conn, row)?;
        inserted
    } else {
        add_events_to_stored_row(conn, row.height, events)?
    };
    if let Some((u, BkSetUpdateWrite::Inserted)) = bk_set_update {
        history_bk_set::insert_bk_set_update(conn, row.height, u)?;
    }
    for attestation in attestations {
        writes.add(history_bk_set::write_attestation(conn, attestation)?);
    }
    Ok(Written {
        bk_set_update: bk_set_update.map(|(_, w)| w),
        attestations: writes,
        events_inserted,
        skipped_events,
    })
}

/// Imports one item: every check before any write, then the writes of
/// `insert_block_with` without attestations.
fn import_item(
    conn: &rusqlite::Connection,
    item: &ImportItem,
    mode: ImportMode,
) -> rusqlite::Result<ImportedRow> {
    let ImportItem { row, events, bk_set_update } = item;
    let refused = |status| ImportedRow { status, bk_set_update: None, detached: vec![] };
    if let Err(err) = validate_row(row) {
        return Ok(refused(ImportRowStatus::Rejected(err)));
    }
    if mode == ImportMode::GapsOnly && stored_row(conn, row.height)?.is_some() {
        return Ok(refused(ImportRowStatus::OutsideGaps));
    }
    let stored = compare_with_stored(conn, row)?;
    if let Some(InsertOutcome::Conflict { existing_height, .. }) = stored {
        return Ok(refused(ImportRowStatus::Conflict(conflict_reason(
            conn,
            row,
            existing_height,
        )?)));
    }
    for event in events {
        let belongs = if event.height != row.height || event.block_id != row.block_id {
            Err(format!(
                "the event is of block {} at height {}, the row is block {} at height {}",
                hex::encode(event.block_id),
                event.height,
                hex::encode(row.block_id),
                row.height
            ))
        } else {
            Ok(())
        };
        if let Err(reason) = belongs.and_then(|()| check_event_boc(event)) {
            return Ok(refused(ImportRowStatus::RejectedEvent(format!(
                "event {}: {reason}",
                hex::encode(event.msg_hash)
            ))));
        }
        // A message comes in one block only; the insert would skip the event.
        if let Some(height) = stored_event_height(conn, &event.msg_hash)? {
            if height != row.height {
                return Ok(refused(ImportRowStatus::Conflict(format!(
                    "event {} is stored at height {height}",
                    hex::encode(event.msg_hash)
                ))));
            }
        }
    }
    if let Err(reason) = history_bk_set::check_bk_set_update_of(row, bk_set_update.as_ref()) {
        return Ok(refused(ImportRowStatus::RejectedBkSetUpdate(reason)));
    }
    let bk_set_update = match bk_set_update {
        None => None,
        Some(u) => match history_bk_set::bk_set_update_status(conn, row.height, u)? {
            BkSetUpdateStatus::Absent => Some((u, BkSetUpdateWrite::Inserted)),
            BkSetUpdateStatus::Present => Some((u, BkSetUpdateWrite::Present)),
            BkSetUpdateStatus::Conflict => {
                return Ok(refused(ImportRowStatus::Conflict(format!(
                    "the BK set update of block {} is not the one stored at height {}",
                    u.block_id, row.height
                ))))
            }
        },
    };
    let new_row = stored.is_none();
    let written = write_checked_block(conn, row, new_row, events, bk_set_update, &[])?;
    Ok(ImportedRow {
        status: if new_row { ImportRowStatus::Inserted } else { ImportRowStatus::Duplicate },
        bk_set_update: written.bk_set_update,
        detached: written.attestations.detached,
    })
}

fn insert_row(conn: &rusqlite::Connection, row: &HistoryRow) -> rusqlite::Result<()> {
    conn.prepare_cached(&format!(
        "INSERT INTO blocks ({ROW_COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
    ))?
    .execute(params![
        row.height as i64,
        row.block_id,
        row.envelope_hash,
        row.tracked_root,
        row.tracked_hashes,
        row.history_proofs,
        row.block_merkle_leaves,
        row.gen_utime,
    ])?;
    Ok(())
}

/// Events of a row that is already stored. An event it did not have yet has
/// not been checked against the row's tracked hashes: the row leaves
/// `verified` and its checks are done again. A check that failed stays
/// failed: new events do not repair the row. Returns the number of events
/// inserted per kind.
fn add_events_to_stored_row(
    conn: &rusqlite::Connection,
    height: u64,
    events: &[EventRow],
) -> rusqlite::Result<EventCounts> {
    let inserted = insert_events(conn, events)?;
    if inserted.total() > 0 {
        history_ranges::remove_height(conn, RangeTable::Verified, height)?;
        conn.prepare_cached("DELETE FROM check_state WHERE height = ?1 AND failed = 0")?
            .execute(params![height as i64])?;
        conn.prepare_cached("UPDATE check_state SET done = 0 WHERE height = ?1")?
            .execute(params![height as i64])?;
    }
    Ok(inserted)
}

/// The height of the stored event with `msg_hash`.
fn stored_event_height(
    conn: &rusqlite::Connection,
    msg_hash: &[u8; 32],
) -> rusqlite::Result<Option<u64>> {
    conn.prepare_cached("SELECT height FROM events WHERE msg_hash = ?1")?
        .query_row(params![msg_hash], |r| r.get::<_, i64>(0))
        .optional()
        .map(|height| height.map(|h| h as u64))
}

/// An event already stored under the same hash is kept as is. Returns the
/// number of events inserted per kind.
fn insert_events(
    conn: &rusqlite::Connection,
    events: &[EventRow],
) -> rusqlite::Result<EventCounts> {
    let mut stmt = conn.prepare_cached(
        "INSERT OR IGNORE INTO events (msg_hash, kind, height, block_id, transaction_id, boc,
             src, dst, created_at, src_dapp_id, msg_type, routing_dapp_id, sk_u_commit)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
    )?;
    let mut inserted = EventCounts::default();
    for e in events {
        let n = stmt.execute(params![
            e.msg_hash,
            e.kind.as_str(),
            e.height as i64,
            e.block_id,
            e.transaction_id,
            e.boc,
            e.src,
            e.dst,
            e.created_at,
            e.src_dapp_id,
            e.msg_type,
            e.routing_dapp_id,
            e.sk_u_commit,
        ])?;
        inserted.add(e.kind, n as u64);
    }
    Ok(inserted)
}

/// Prefixes an SQLite error with the store's path. block-manager logs store
/// errors with `{err}`: a bare "database is locked" sends the operator to
/// bm-archive.db, and `.context()` would print the path without the cause.
fn in_file(path: &Path, err: rusqlite::Error) -> anyhow::Error {
    anyhow::format_err!("{}: {err}", path.display())
}

/// Where a new store at `path` is built before it is published.
fn creating_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".creating-{}", std::process::id()));
    path.with_file_name(name)
}

/// `path` and the files SQLite keeps next to it.
fn remove_store_files(path: &Path) {
    for suffix in ["", "-journal", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

/// Builds a complete store at `temp`: the schema, `user_version` and WAL mode.
fn build_store_file(temp: &Path) -> anyhow::Result<()> {
    remove_store_files(temp);
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn =
        rusqlite::Connection::open_with_flags(temp, flags).map_err(|err| in_file(temp, err))?;
    conn.execute_batch(&format!(
        "BEGIN; {SCHEMA}; PRAGMA user_version = {SCHEMA_VERSION}; COMMIT;"
    ))
    .map_err(|err| in_file(temp, err))?;
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(|err| in_file(temp, err))?;
    anyhow::ensure!(journal_mode == "wal", "{}: journal_mode is {journal_mode}", temp.display());
    // Closing the last connection checkpoints and removes the `-wal`.
    conn.close().map_err(|(_, err)| in_file(temp, err))?;
    Ok(())
}

/// Publishes the store built at `temp` as `path` and removes `temp`. A hard
/// link, unlike a rename, never replaces a file: `false` when another
/// process created `path` meanwhile, which is kept.
fn publish_store_file(temp: &Path, path: &Path) -> anyhow::Result<bool> {
    let published = match std::fs::hard_link(temp, path) {
        Ok(()) => true,
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(err) => {
            remove_store_files(temp);
            anyhow::bail!("{}: {err}", path.display());
        }
    };
    remove_store_files(temp);
    Ok(published)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::Path;
    use std::time::Duration;

    use rusqlite::Connection;
    use testdir::testdir;

    use super::*;
    use crate::sqlite::history_bk_set::tests as bk_set_tests;

    pub(crate) fn proofs(layers: u8) -> Vec<u8> {
        let mut blob = vec![layers];
        for layer in 1..=layers {
            blob.push(layer);
            blob.extend_from_slice(&[0x10 + layer; 32]);
        }
        blob
    }

    pub(crate) fn id(seed: u64) -> [u8; 32] {
        let mut id = [0xb0; 32];
        id[24..].copy_from_slice(&seed.to_be_bytes());
        id
    }

    pub(crate) fn row(height: u64) -> HistoryRow {
        let layers = proof_data::layers::required_layers(height);
        HistoryRow {
            height,
            block_id: id(height),
            envelope_hash: [0xe0; 32],
            tracked_root: [0; 32],
            tracked_hashes: None,
            history_proofs: (layers > 0).then(|| proofs(layers)),
            block_merkle_leaves: None,
            gen_utime: Some(1_791_000_000),
        }
    }

    /// An ext-out message of the default bridge routing; `seed` is its `created_lt`.
    pub(crate) fn event_boc(seed: u8) -> Vec<u8> {
        use std::str::FromStr;

        use tvm_block::ExtOutMessageHeader;
        use tvm_block::MsgAddressExt;
        use tvm_block::MsgAddressInt;
        use tvm_block::Serializable;

        let src = MsgAddressInt::from_str(&EventFilter::withdrawal_default().src()).unwrap();
        let dst = MsgAddressExt::from_str(DEFAULT_BRIDGE_EVENT_DST).unwrap();
        let mut header = ExtOutMessageHeader::with_addresses(src, dst);
        header.created_lt = seed as u64;
        header.created_at = 1_791_000_000.into();
        tvm_types::write_boc(&Message::with_ext_out_header(header).serialize().unwrap()).unwrap()
    }

    pub(crate) fn event(height: u64, seed: u8) -> EventRow {
        event_from_boc(
            &event_boc(seed),
            EventKind::Withdrawal,
            height,
            id(height),
            format!("tx-{seed}"),
            [0; 32],
        )
        .unwrap()
    }

    pub(crate) fn arch_block(height: u64, history_proofs: Vec<u8>) -> ArchBlock {
        ArchBlock {
            id: hex::encode(id(height)),
            thread_id: Some(THREAD_ZERO_HEX.to_string()),
            seq_no: height as u32,
            height: height.to_be_bytes(),
            envelope_hash: [0xe0; 32],
            gen_utime: Some(1_791_000_000),
            history_proofs,
            block_merkle_leaves: vec![0x77; 512],
            ..Default::default()
        }
    }

    /// `import_batch` of `(row, events)` items; the statuses.
    fn import(
        store: &mut HistoryProofsStore,
        items: &[(HistoryRow, Vec<EventRow>)],
        mode: ImportMode,
    ) -> anyhow::Result<Vec<ImportRowStatus>> {
        let items: Vec<ImportItem> = items.iter().cloned().map(Into::into).collect();
        Ok(store.import_batch(&items, mode)?.into_iter().map(|imported| imported.status).collect())
    }

    fn rotation_item(height: u64) -> ImportItem {
        ImportItem {
            row: bk_set_tests::rotation(height),
            events: vec![],
            bk_set_update: Some(bk_set_tests::update(height)),
        }
    }

    /// A failed write leaves nothing of the batch: no row, event, BK set
    /// update or coverage of the items before it.
    #[test]
    fn import_batch_is_atomic_on_sqlite_error() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.connection().execute_batch(
            "CREATE TRIGGER t BEFORE INSERT ON blocks WHEN NEW.height = 2003
             BEGIN SELECT RAISE(ABORT, 'boom'); END",
        )?;
        let items: Vec<ImportItem> = vec![
            (row(2000), vec![event(2000, 1)]).into(),
            rotation_item(2001),
            (row(2002), vec![]).into(),
            (row(2003), vec![]).into(),
        ];
        assert!(store.import_batch(&items, ImportMode::GapsOnly).is_err());
        let conn = store.connection();
        assert_eq!(count(conn, "SELECT COUNT(*) FROM blocks"), 0);
        assert_eq!(count(conn, "SELECT COUNT(*) FROM events"), 0);
        assert_eq!(count(conn, "SELECT COUNT(*) FROM bk_set_updates"), 0);
        assert_eq!(count(conn, "SELECT COUNT(*) FROM coverage"), 0);

        // The transaction is closed: the same batch goes in once the error is gone.
        conn.execute_batch("DROP TRIGGER t")?;
        store.import_batch(&items, ImportMode::GapsOnly)?;
        assert_eq!(
            history_ranges::ranges(store.connection(), RangeTable::Coverage)?,
            vec![(2000, 2003)]
        );
        assert_eq!(count(store.connection(), "SELECT COUNT(*) FROM bk_set_updates"), 1);
        Ok(())
    }

    #[test]
    fn import_rejects_an_event_of_another_row() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        // Each differs from row 10 in one field only.
        let mut other_height = event(11, 1);
        other_height.block_id = id(10);
        let mut other_block = event(10, 2);
        other_block.block_id = id(99);
        for foreign in [other_height, other_block] {
            let statuses =
                import(&mut store, &[(row(10), vec![foreign.clone()])], ImportMode::GapsOnly)?;
            assert!(
                matches!(&statuses[0], ImportRowStatus::RejectedEvent(_)),
                "{foreign:?}: {statuses:?}"
            );
        }
        assert_eq!(store.row(10)?, None);
        assert_eq!(store.events_in_range(0, 100)?, vec![]);
        Ok(())
    }

    #[test]
    fn import_rejects_foreign_bk_set_update() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        let mut foreign = rotation_item(2000);
        foreign.bk_set_update.as_mut().unwrap().block_id = hex::encode(id(2001));
        let imported = store.import_batch(&[foreign], ImportMode::GapsOnly)?;
        assert!(
            matches!(imported[0].status, ImportRowStatus::RejectedBkSetUpdate(_)),
            "{imported:?}"
        );
        assert_eq!(imported[0].bk_set_update, None);
        assert_eq!(store.row(2000)?, None);
        assert_eq!(count(store.connection(), "SELECT COUNT(*) FROM bk_set_updates"), 0);
        Ok(())
    }

    #[test]
    fn import_rejects_rotation_row_without_bk_set_update() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        let mut without_update = rotation_item(2000);
        without_update.bk_set_update = None;
        let imported = store.import_batch(&[without_update], ImportMode::GapsOnly)?;
        assert!(
            matches!(imported[0].status, ImportRowStatus::RejectedBkSetUpdate(_)),
            "{imported:?}"
        );
        assert_eq!(store.row(2000)?, None);

        let imported = store.import_batch(&[rotation_item(2000)], ImportMode::GapsOnly)?;
        assert_eq!(
            imported,
            vec![ImportedRow {
                status: ImportRowStatus::Inserted,
                bk_set_update: Some(BkSetUpdateWrite::Inserted),
                detached: vec![],
            }]
        );
        Ok(())
    }

    #[test]
    fn import_bk_set_update_conflict_writes_nothing() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.import_batch(&[rotation_item(2000)], ImportMode::All)?;
        let mut other = rotation_item(2000);
        other.bk_set_update.as_mut().unwrap().bk_set_update = vec![0xcc; 40];
        other.events = vec![event(2000, 1)];
        let imported = store.import_batch(&[other], ImportMode::All)?;
        assert_eq!(
            imported,
            vec![ImportedRow {
                status: ImportRowStatus::Conflict(format!(
                    "the BK set update of block {} is not the one stored at height 2000",
                    hex::encode(id(2000))
                )),
                bk_set_update: None,
                detached: vec![]
            }]
        );
        assert_eq!(store.events_in_range(2000, 2000)?, vec![]);
        assert_eq!(
            history_bk_set::bk_set_update_status(
                store.connection(),
                2000,
                &bk_set_tests::update(2000)
            )?,
            BkSetUpdateStatus::Present
        );
        Ok(())
    }

    #[test]
    fn export_batch_reads_bk_set_update_with_its_row() -> anyhow::Result<()> {
        let dir = testdir!();
        let store = HistoryProofsStore::open(&dir)?;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let (dir, stop) = (dir.clone(), stop.clone());
            std::thread::Builder::new().name("writer".to_string()).spawn(
                move || -> anyhow::Result<u64> {
                    let mut writer =
                        HistoryProofsStore::open_with_busy_timeout(&dir, Duration::from_secs(5))?;
                    let mut height = 1;
                    while !stop.load(Ordering::SeqCst) && height <= 400 {
                        let item = rotation_item(height);
                        writer.insert_block_with(
                            &item.row,
                            &[],
                            item.bk_set_update.as_ref(),
                            &[],
                        )?;
                        height += 1;
                    }
                    Ok(height - 1)
                },
            )?
        };
        for _ in 0..200 {
            let (items, _) = store.read_export_batch(1, 1_000_000)?;
            for item in items {
                assert_eq!(
                    item.bk_set_update,
                    Some(bk_set_tests::update(item.row.height)),
                    "row {} is read without its BK set update",
                    item.row.height
                );
            }
        }
        stop.store(true, Ordering::SeqCst);
        assert!(writer.join().unwrap()? > 0);
        Ok(())
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    fn user_version_file(dir: &Path, version: i64) -> PathBuf {
        let path = dir.join(HISTORY_PROOFS_DB);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&format!("CREATE TABLE t (x); PRAGMA user_version = {version}"))
            .unwrap();
        path
    }

    #[test]
    fn opens_new_file_with_version_1() -> anyhow::Result<()> {
        let dir = testdir!();
        let store = HistoryProofsStore::open(&dir)?;
        let conn = store.connection();
        assert_eq!(count(conn, "PRAGMA user_version"), SCHEMA_VERSION);
        assert_eq!(SCHEMA_VERSION, 1);
        let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        assert_eq!(journal_mode, "wal");
        assert_eq!(count(conn, "PRAGMA synchronous"), 2, "synchronous must be FULL");
        for table in ["blocks", "events", "coverage", "verified", "check_state"] {
            assert_eq!(
                count(conn, &format!("SELECT COUNT(*) FROM sqlite_master WHERE name = '{table}'")),
                1,
                "{table}"
            );
        }
        Ok(())
    }

    #[test]
    fn opens_version_1() -> anyhow::Result<()> {
        let dir = testdir!();
        {
            let mut store = HistoryProofsStore::open(&dir)?;
            store.insert_block(&row(128), &[])?;
        }
        let store = HistoryProofsStore::open(&dir)?;
        assert_eq!(store.row(128)?, Some(row(128)));
        let reopened = HistoryProofsStore::open_file(
            &dir.join(HISTORY_PROOFS_DB),
            HISTORY_PROOFS_BUSY_TIMEOUT,
            false,
        )?;
        assert_eq!(reopened.row(128)?, Some(row(128)));
        Ok(())
    }

    /// Readers that look for the file (gql-server at startup and on SIGHUP,
    /// the Ansible healthcheck) must never find it without its schema. A
    /// directory in place of the target's rollback journal makes building
    /// the schema at the target fail.
    #[test]
    fn new_store_never_appears_without_its_schema() {
        let dir = testdir!();
        let path = dir.join(HISTORY_PROOFS_DB);
        std::fs::create_dir(dir.join(format!("{HISTORY_PROOFS_DB}-journal"))).unwrap();
        let opened = HistoryProofsStore::open_file(&path, HISTORY_PROOFS_BUSY_TIMEOUT, true);
        if path.exists() {
            // The header, not SQLite: the journal directory also breaks a
            // reader's hot journal check. `user_version` is bytes 60..64.
            let header = std::fs::read(&path).unwrap();
            let user_version =
                header.get(60..64).map(|b| i32::from_be_bytes(b.try_into().unwrap()));
            assert_eq!(
                user_version,
                Some(SCHEMA_VERSION as i32),
                "a file without its schema is visible; open: {:?}",
                opened.err()
            );
        }
        let left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.contains(".creating"))
            .collect();
        assert!(left.is_empty(), "{left:?}");
    }

    /// Two processes may create the store at once: the second must not
    /// replace the file the first one already writes to.
    #[test]
    fn publishing_a_new_store_keeps_one_created_meanwhile() -> anyhow::Result<()> {
        let dir = testdir!();
        let path = dir.join(HISTORY_PROOFS_DB);
        let mut first = HistoryProofsStore::open(&dir)?;
        first.insert_block(&row(5), &[])?;
        let temp = dir.join("other.creating");
        build_store_file(&temp)?;
        assert!(!publish_store_file(&temp, &path)?, "an existing file must not be replaced");
        assert!(!temp.exists());
        let conn = rusqlite::Connection::open(&path)?;
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM blocks"), 1);
        Ok(())
    }

    #[test]
    fn other_schema_versions_are_refused() {
        // 2 and 3 are stores of builds before the versions were collapsed.
        for version in [2, 3, 4, 77] {
            let dir = testdir!().join(format!("v{version}"));
            std::fs::create_dir_all(&dir).unwrap();
            user_version_file(&dir, version);
            for create in [true, false] {
                let err = HistoryProofsStore::open_file(
                    &dir.join(HISTORY_PROOFS_DB),
                    HISTORY_PROOFS_BUSY_TIMEOUT,
                    create,
                )
                .err()
                .expect("a foreign user_version must be refused");
                let message = err.to_string();
                assert!(message.contains(HISTORY_PROOFS_DB), "{message}");
                assert!(message.contains(&format!("user_version {version}")), "{message}");
            }
        }
    }

    /// A file without attestations to delete opens without the write lock,
    /// next to a writer that holds it.
    #[test]
    fn open_next_to_a_writer() -> anyhow::Result<()> {
        let dir = testdir!();
        HistoryProofsStore::open(&dir)?;
        let locker = Connection::open(dir.join(HISTORY_PROOFS_DB))?;
        locker.execute_batch("BEGIN IMMEDIATE")?;
        let opened = HistoryProofsStore::open_with_busy_timeout(&dir, Duration::from_millis(50));
        locker.execute_batch("ROLLBACK")?;
        opened?;
        Ok(())
    }

    #[test]
    fn open_without_create_rejects_missing_and_empty() {
        let dir = testdir!();
        let missing = dir.join("missing.db");
        assert!(
            HistoryProofsStore::open_file(&missing, HISTORY_PROOFS_BUSY_TIMEOUT, false).is_err()
        );
        assert!(!missing.exists(), "opening without create must not create a file");

        let empty = dir.join("empty.db");
        std::fs::write(&empty, b"").unwrap();
        let err = HistoryProofsStore::open_file(&empty, HISTORY_PROOFS_BUSY_TIMEOUT, false)
            .err()
            .expect("a file without the schema must be refused");
        assert!(err.to_string().contains("user_version 0"), "{err}");
    }

    #[test]
    fn open_errors_name_the_file() {
        let dir = testdir!();
        std::fs::write(dir.join(HISTORY_PROOFS_DB), vec![0x5a_u8; 8192]).unwrap();
        let err = HistoryProofsStore::open(&dir).err().expect("not a database");
        let message = err.to_string();
        assert!(message.contains(HISTORY_PROOFS_DB), "the error does not name the file: {message}");
        assert!(message.contains("not a database"), "the SQLite cause is lost: {message}");
    }

    #[test]
    fn insert_then_duplicate_then_conflict() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        assert!(matches!(store.insert_block(&row(10), &[])?, BlockWrite::Inserted(_)));
        assert!(matches!(store.insert_block(&row(10), &[])?, BlockWrite::Duplicate(_)));

        let mut other_id = row(10);
        other_id.block_id = id(999);
        assert_eq!(
            store.insert_block(&other_id, &[])?,
            BlockWrite::Conflict { existing_height: 10, existing_block_id: id(10) }
        );
        let mut other_envelope = row(10);
        other_envelope.envelope_hash = [0xee; 32];
        assert_eq!(
            store.insert_block(&other_envelope, &[])?,
            BlockWrite::Conflict { existing_height: 10, existing_block_id: id(10) }
        );
        let mut other_blob = row(10);
        other_blob.gen_utime = Some(1);
        assert!(matches!(store.insert_block(&other_blob, &[])?, BlockWrite::Conflict { .. }));

        assert_eq!(store.row(10)?, Some(row(10)));
        Ok(())
    }

    #[test]
    fn same_block_id_other_height_is_conflict() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        store.insert_block(&row(10), &[])?;
        let mut moved = row(11);
        moved.block_id = id(10);
        assert_eq!(
            store.insert_block(&moved, &[])?,
            BlockWrite::Conflict { existing_height: 10, existing_block_id: id(10) }
        );
        assert_eq!(store.row(11)?, None);

        let statuses = import(
            &mut store,
            &[(row(12), vec![]), (moved.clone(), vec![]), (row(13), vec![])],
            ImportMode::GapsOnly,
        )?;
        let moved_reason = ImportRowStatus::Conflict(format!(
            "block {} is stored at height 10, the file has it at height 11",
            hex::encode(id(10))
        ));
        assert_eq!(
            statuses,
            vec![ImportRowStatus::Inserted, moved_reason.clone(), ImportRowStatus::Inserted]
        );
        let statuses = import(&mut store, &[(moved, vec![])], ImportMode::All)?;
        assert_eq!(statuses, vec![moved_reason]);
        assert_eq!(
            history_ranges::ranges(store.connection(), RangeTable::Coverage)?,
            vec![(10, 10), (12, 13)]
        );
        Ok(())
    }

    #[test]
    fn events_written_with_their_block() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        store.insert_block(&row(10), &[event(10, 1), event(10, 2)])?;
        store.insert_block(&row(11), &[])?;
        let mut conflicting = row(12);
        conflicting.block_id = id(10);
        store.insert_block(&conflicting, &[event(12, 3)])?;
        assert_eq!(store.events_in_range(0, 100)?, vec![event(10, 1), event(10, 2)]);
        assert_eq!(store.events_in_range(11, 100)?, vec![]);
        Ok(())
    }

    #[test]
    fn import_gaps_only_skips_covered_and_their_events() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        store.insert_block(&row(10), &[])?;
        let mut changed = row(10);
        changed.envelope_hash = [0xee; 32];
        let statuses = import(
            &mut store,
            &[(changed, vec![event(10, 1)]), (row(11), vec![event(11, 2)])],
            ImportMode::GapsOnly,
        )?;
        assert_eq!(statuses, vec![ImportRowStatus::OutsideGaps, ImportRowStatus::Inserted]);
        assert_eq!(store.row(10)?, Some(row(10)));
        assert_eq!(store.events_in_range(0, 100)?, vec![event(11, 2)]);
        assert_eq!(
            history_ranges::ranges(store.connection(), RangeTable::Coverage)?,
            vec![(10, 11)]
        );
        Ok(())
    }

    /// Another block at the same height: both ids and the fields that differ.
    #[test]
    fn import_conflict_names_both_blocks() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(10), &[])?;
        let mut other = row(10);
        other.block_id = id(99);
        other.gen_utime = Some(1);
        let statuses = import(&mut store, &[(other, vec![])], ImportMode::All)?;
        assert_eq!(
            statuses,
            vec![ImportRowStatus::Conflict(format!(
                "the row at height 10 is block {}, the file has block {}; they differ in block_id, gen_utime",
                hex::encode(id(10)),
                hex::encode(id(99))
            ))]
        );
        Ok(())
    }

    /// New events of a stored row reset what was checked, not what failed:
    /// a failed row stays failed until the store is restored.
    #[test]
    fn new_events_keep_the_failed_checks() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(10), &[])?;
        store
            .connection()
            .execute("INSERT INTO check_state (height, done, failed) VALUES (10, 3, 2)", [])?;
        store.insert_block(&row(10), &[event(10, 1)])?;
        let state: (i64, i64) = store.connection().query_row(
            "SELECT done, failed FROM check_state WHERE height = 10",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(state, (0, 2));
        Ok(())
    }

    #[test]
    fn import_all_compares() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        store.insert_block(&row(10), &[])?;
        let mut changed = row(10);
        changed.envelope_hash = [0xee; 32];
        let statuses = import(
            &mut store,
            &[(row(10), vec![event(10, 1)]), (changed, vec![event(10, 9)]), (row(11), vec![])],
            ImportMode::All,
        )?;
        assert_eq!(
            statuses,
            vec![
                ImportRowStatus::Duplicate,
                ImportRowStatus::Conflict(format!(
                    "block {} at height 10 differs from the stored row in envelope_hash",
                    hex::encode(id(10))
                )),
                ImportRowStatus::Inserted
            ]
        );
        // Events of a duplicate are kept (the row is the same block), of a conflict are not.
        assert_eq!(store.events_in_range(0, 100)?, vec![event(10, 1)]);
        Ok(())
    }

    #[test]
    fn bridge_event_fields_come_from_the_boc() {
        use tvm_block::Serializable;

        use crate::serialization::MessageSerializationSet;
        use crate::sqlite::ArchMessage;

        // The fields the archive takes from a message, for v1 and v2 headers.
        for fixture in [
            include_str!("fixtures/ext_out_msg_info.boc.b64"),
            include_str!("fixtures/ext_out_msg_info_v2.boc.b64"),
        ] {
            let boc = tvm_types::base64_decode(fixture.trim()).unwrap();
            let message = Message::construct_from_bytes(&boc).unwrap();
            let hash = message.serialize().unwrap().repr_hash();
            let archived: ArchMessage = MessageSerializationSet {
                message,
                id: hash,
                boc: boc.clone(),
                ..Default::default()
            }
            .into();
            let event =
                event_from_boc(&boc, EventKind::Withdrawal, 7, id(7), "tx".to_string(), [0; 32])
                    .unwrap();
            assert_eq!(hex::encode(event.msg_hash), archived.id);
            assert_eq!(
                (Some(event.src), Some(event.dst), event.created_at, event.src_dapp_id),
                (archived.src, archived.dst, archived.created_at, archived.src_dapp_id)
            );
            assert_eq!(event.msg_type, archived.msg_type);
            assert_eq!((event.height, event.block_id, event.boc), (7, id(7), boc));
        }
        assert!(event_from_boc(
            &[1, 2, 3],
            EventKind::Withdrawal,
            7,
            id(7),
            "tx".to_string(),
            [0; 32]
        )
        .is_err());
        let internal = tvm_types::write_boc(&Message::default().serialize().unwrap()).unwrap();
        assert!(event_from_boc(
            &internal,
            EventKind::Withdrawal,
            7,
            id(7),
            "tx".to_string(),
            [0; 32]
        )
        .is_err());
    }

    #[test]
    fn check_event_boc_finds_changed_fields() {
        let genuine = event(10, 1);
        assert_eq!(check_event_boc(&genuine), Ok(()));
        let forged: Vec<EventRow> = vec![
            EventRow { boc: event_boc(2), ..genuine.clone() },
            EventRow { boc: vec![1, 2, 3], ..genuine.clone() },
            EventRow { msg_hash: [0x55; 32], ..genuine.clone() },
            EventRow { src: format!("0:{}", "1b".repeat(32)), ..genuine.clone() },
            EventRow { dst: format!(":{:064x}", 1), ..genuine.clone() },
            EventRow { created_at: Some(1), ..genuine.clone() },
            EventRow { src_dapp_id: Some("dd".repeat(32)), ..genuine.clone() },
            EventRow { msg_type: Some(4), ..genuine.clone() },
        ];
        for event in forged {
            assert!(check_event_boc(&event).is_err(), "{event:?}");
        }
    }

    /// A live write keeps the block and leaves out an event whose message is
    /// stored at another height, also when the block is sent again.
    #[test]
    fn live_write_reports_an_event_stored_at_another_height() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        store.insert_block(&row(10), &[event(10, 1)])?;
        let skipped = vec![(EventKind::Withdrawal, event(11, 1).msg_hash, 10)];
        let events = [event(11, 1), event(11, 2)];
        match store.insert_block(&row(11), &events)? {
            BlockWrite::Inserted(w) => assert_eq!(w.skipped_events, skipped),
            other => panic!("{other:?}"),
        }
        match store.insert_block(&row(11), &events)? {
            BlockWrite::Duplicate(w) => assert_eq!(w.skipped_events, skipped),
            other => panic!("{other:?}"),
        }
        assert_eq!(store.events_in_range(0, 100)?, vec![event(10, 1), event(11, 2)]);
        Ok(())
    }

    /// A message comes in one block only: an event stored at another height
    /// makes the row a conflict, instead of a silent skip of the event.
    #[test]
    fn import_flags_an_event_stored_at_another_height() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        store.insert_block(&row(10), &[event(10, 1)])?;
        let statuses = import(&mut store, &[(row(11), vec![event(11, 1)])], ImportMode::All)?;
        assert!(
            matches!(&statuses[0], ImportRowStatus::Conflict(reason) if reason.contains("stored at height 10")),
            "{statuses:?}"
        );
        assert_eq!(store.events_in_range(0, 100)?, vec![event(10, 1)]);
        assert_eq!(
            history_ranges::ranges(store.connection(), RangeTable::Coverage)?,
            vec![(10, 10)]
        );
        Ok(())
    }

    /// A source may hold a genuine `msg_hash` with another body: the row is
    /// rejected whole and its height stays a gap for another source.
    #[test]
    fn import_rejects_a_row_whose_event_is_not_its_boc() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        store.insert_block(&row(10), &[])?;
        let changed_body = EventRow { boc: event_boc(2), ..event(11, 1) };
        let changed_time = EventRow { created_at: Some(1), ..event(10, 3) };
        let statuses = import(
            &mut store,
            &[
                (row(10), vec![changed_time]),
                (row(11), vec![changed_body]),
                (row(12), vec![event(12, 4)]),
            ],
            ImportMode::All,
        )?;
        assert!(matches!(statuses[0], ImportRowStatus::RejectedEvent(_)), "{statuses:?}");
        assert!(matches!(statuses[1], ImportRowStatus::RejectedEvent(_)), "{statuses:?}");
        assert_eq!(statuses[2], ImportRowStatus::Inserted);
        assert_eq!(store.events_in_range(0, 100)?, vec![event(12, 4)]);
        assert_eq!(
            history_ranges::ranges(store.connection(), RangeTable::Coverage)?,
            vec![(10, 10), (12, 12)]
        );
        Ok(())
    }

    fn verified_and_state(store: &HistoryProofsStore, height: u64) -> (bool, bool) {
        let verified = history_ranges::ranges(store.connection(), RangeTable::Verified)
            .unwrap()
            .iter()
            .any(|(a, b)| *a <= height && height <= *b);
        let state: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM check_state WHERE height = ?1", [height as i64], |r| {
                r.get(0)
            })
            .unwrap();
        (verified, state > 0)
    }

    fn mark_checked(store: &HistoryProofsStore, height: u64) {
        history_ranges::add_height(store.connection(), RangeTable::Verified, height).unwrap();
        store
            .connection()
            .execute(
                "INSERT INTO check_state (height, done, failed) VALUES (?1, 1, 0)",
                [height as i64],
            )
            .unwrap();
    }

    #[test]
    fn new_events_on_a_duplicate_reopen_verification() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        for h in [10, 11, 12] {
            store.insert_block(&row(h), &[event(h, h as u8)])?;
            mark_checked(&store, h);
        }
        // Same events again: nothing to re-check.
        assert!(matches!(
            store.insert_block(&row(10), &[event(10, 10)])?,
            BlockWrite::Duplicate(_)
        ));
        assert_eq!(verified_and_state(&store, 10), (true, true));
        // A new event through live writes and through `import --all`.
        assert!(matches!(
            store.insert_block(&row(11), &[event(11, 11), event(11, 0x71)])?,
            BlockWrite::Duplicate(_)
        ));
        assert_eq!(verified_and_state(&store, 11), (false, false));
        assert_eq!(
            import(&mut store, &[(row(12), vec![event(12, 0x72)])], ImportMode::All)?,
            vec![ImportRowStatus::Duplicate]
        );
        assert_eq!(verified_and_state(&store, 12), (false, false));
        assert_eq!(verified_and_state(&store, 10), (true, true));
        Ok(())
    }

    #[test]
    fn import_rejects_bad_rows() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        let mut no_layers = row(128);
        no_layers.history_proofs = None;
        let mut short_leaves = row(129);
        short_leaves.block_merkle_leaves = Some(vec![0; 511]);
        let statuses = import(
            &mut store,
            &[(no_layers, vec![event(128, 1)]), (short_leaves, vec![]), (row(130), vec![])],
            ImportMode::GapsOnly,
        )?;
        assert_eq!(
            statuses,
            vec![
                ImportRowStatus::Rejected(FormatError::LayerSetMismatch {
                    height: 128,
                    expected: 1,
                    found: vec![]
                }),
                ImportRowStatus::Rejected(FormatError::BadLength {
                    field: "block_merkle_leaves",
                    expected: 512,
                    got: 511
                }),
                ImportRowStatus::Inserted,
            ]
        );
        assert_eq!(store.events_in_range(0, 1000)?, vec![]);
        assert_eq!(
            history_ranges::ranges(store.connection(), RangeTable::Coverage)?,
            vec![(130, 130)]
        );
        Ok(())
    }

    #[test]
    fn row_from_arch_block_thread_and_leaves() {
        let mut other_thread = arch_block(256, proofs(1));
        other_thread.thread_id = Some(format!("{}01", &THREAD_ZERO_HEX[..66]));
        assert_eq!(history_row_from_arch_block(&other_thread), Ok(None));
        let mut no_thread = arch_block(256, proofs(1));
        no_thread.thread_id = None;
        assert_eq!(history_row_from_arch_block(&no_thread), Ok(None));

        let boundary = history_row_from_arch_block(&arch_block(256, proofs(1))).unwrap().unwrap();
        assert_eq!(boundary.block_merkle_leaves, Some(vec![0x77; 512]));
        assert_eq!(boundary.block_id, id(256));
        assert_eq!(boundary.history_proofs, Some(proofs(1)));

        let plain = history_row_from_arch_block(&arch_block(300, vec![0])).unwrap().unwrap();
        assert_eq!(plain.block_merkle_leaves, None);
    }

    #[test]
    fn row_from_arch_block_tracked_hashes() {
        let mut block = arch_block(300, vec![]);
        block.tracked_ext_out_message_hashes = vec![0, 0, 0, 0];
        assert_eq!(history_row_from_arch_block(&block).unwrap().unwrap().tracked_hashes, None);
        let mut group = vec![0, 0, 0, 1];
        group.extend_from_slice(&[0; 32]);
        group.extend_from_slice(&[0x1a; 32]);
        group.extend_from_slice(&[0, 0, 0, 1]);
        group.extend_from_slice(&[0x55; 32]);
        block.tracked_ext_out_message_hashes = group.clone();
        assert_eq!(
            history_row_from_arch_block(&block).unwrap().unwrap().tracked_hashes,
            Some(group.clone())
        );
        group.push(0);
        block.tracked_ext_out_message_hashes = group;
        assert_eq!(history_row_from_arch_block(&block), Err(FormatError::TrailingBytes));
        block.tracked_ext_out_message_hashes = vec![];
        block.id = "not-hex".to_string();
        assert!(matches!(
            history_row_from_arch_block(&block),
            Err(FormatError::BadLength { field: "block_id", .. })
        ));
    }

    #[test]
    fn non_boundary_block_stores_null_history_proofs() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        assert!(matches!(
            store.store_arch_block(&arch_block(129, vec![0x00]), &[])?,
            StoreResult::Stored(BlockWrite::Inserted(_))
        ));
        let stored: Option<Vec<u8>> = store.connection().query_row(
            "SELECT history_proofs FROM blocks WHERE height = 129",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(stored, None);
        assert_eq!(
            store.store_arch_block(&arch_block(128, vec![0x00]), &[])?,
            StoreResult::Rejected(FormatError::LayerSetMismatch {
                height: 128,
                expected: 1,
                found: vec![]
            })
        );
        assert_eq!(store.row(128)?, None);
        let mut other_thread = arch_block(130, vec![]);
        other_thread.thread_id = Some("01".repeat(34));
        assert_eq!(store.store_arch_block(&other_thread, &[])?, StoreResult::NotThreadZero);
        Ok(())
    }

    /// An archived ext-out message with the BOC of `event_boc(seed)`; `src` and
    /// `dst` are what the filter sees.
    pub(crate) fn ext_out(src: &str, dst: &str, seed: u8) -> crate::sqlite::ArchMessage {
        crate::sqlite::ArchMessage {
            id: hex::encode(event(0, seed).msg_hash),
            transaction_id: Some(format!("tx-{seed}")),
            boc: Some(event_boc(seed)),
            msg_type: Some(2),
            src: Some(src.to_string()),
            dst: Some(dst.to_string()),
            created_at: Some(1_791_000_000),
            ..Default::default()
        }
    }

    #[test]
    fn real_withdrawal_initiated_is_selected() {
        use tvm_block::Serializable;

        use crate::serialization::MessageSerializationSet;
        use crate::sqlite::ArchMessage;

        // A `WithdrawalInitiated` event of the shellnet bridge, as the node
        // archives it (v1 header, no `src_dapp_id`).
        let boc = tvm_types::base64_decode(
            include_str!("fixtures/withdrawal_initiated_shellnet.boc.b64").trim(),
        )
        .unwrap();
        let message = Message::construct_from_bytes(&boc).unwrap();
        let hash = message.serialize().unwrap().repr_hash();
        let mut archived: ArchMessage =
            MessageSerializationSet { message, id: hash, boc: boc.clone(), ..Default::default() }
                .into();
        archived.transaction_id = Some("tx".to_string());
        assert_eq!(archived.src_dapp_id, None);
        let mut block = arch_block(10, vec![]);
        block.ext_out_messages = vec![archived];

        let (events, skipped) = events_of(&block, &EventFilters::default());
        assert_eq!(skipped.total(), 0);
        assert_eq!(events.len(), 1, "the default filter does not select the bridge event");
        assert_eq!(
            hex::encode(events[0].msg_hash),
            "744515b86d64f27013c35c4aa7653b2b13d8850abf163b660ae721eefd3713f8"
        );
        assert_eq!(check_event_boc(&events[0]), Ok(()));
    }

    #[test]
    fn event_dst_is_normalized() {
        use std::str::FromStr;

        let dst = |value: &str| {
            parse_event_dst(value).map(|dst_address| {
                EventFilter { dst_address, ..EventFilter::withdrawal_default() }.dst()
            })
        };
        let upper = format!(":{}26A", "0".repeat(61));
        assert_eq!(dst(&upper).as_deref(), Ok(DEFAULT_BRIDGE_EVENT_DST));
        assert_eq!(dst(DEFAULT_BRIDGE_EVENT_DST).as_deref(), Ok(DEFAULT_BRIDGE_EVENT_DST));
        // Written as `ArchMessage` writes the `dst` of an ext-out.
        let letters = format!(":{}", "Ab".repeat(32));
        assert_eq!(
            dst(&letters),
            Ok(tvm_block::MsgAddressExt::from_str(&letters).unwrap().to_string())
        );
        // An internal address or garbage is never the `dst` of an ext-out.
        // A shorter external address is another address: `:26a` is 12 bits.
        for bad in [
            "",
            "0:000000000000000000000000000000000000000000000000000000000000026a",
            "26a",
            ":26a",
            ":01",
        ] {
            assert!(parse_event_dst(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn events_of_matches_src_and_dst() {
        let filter = EventFilter::withdrawal_default();
        assert_eq!(filter.src(), format!("0:{}", "1a".repeat(32)));
        let mut block = arch_block(10, vec![]);
        block.ext_out_messages = vec![
            ext_out(&filter.src(), DEFAULT_BRIDGE_EVENT_DST, 1),
            ext_out(
                &filter.src(),
                ":0000000000000000000000000000000000000000000000000000000000000001",
                2,
            ),
            ext_out(&format!("0:{}", "1b".repeat(32)), DEFAULT_BRIDGE_EVENT_DST, 3),
        ];
        let (events, skipped) = events_of(&block, &EventFilters::default());
        assert_eq!(skipped.total(), 0);
        assert_eq!(events, vec![event(10, 1)]);

        let custom = EventFilter {
            routing_dapp_id: [0; 32],
            routing_account_id: [0x1b; 32],
            dst_address: [0x01; 32],
        };
        let custom = EventFilters { withdrawal: custom, ..EventFilters::default() };
        assert_eq!(events_of(&block, &custom), (vec![], EventCounts::default()));
        block.ext_out_messages.push(ext_out(&custom.withdrawal.src(), &custom.withdrawal.dst(), 4));
        let (events, _) = events_of(&block, &custom);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].src, custom.withdrawal.src());
        assert_eq!(events[0].dst, custom.withdrawal.dst());
    }

    #[test]
    fn event_without_transaction_or_boc_is_skipped() {
        let filter = EventFilter::withdrawal_default();
        let mut block = arch_block(10, vec![]);
        let mut no_tx = ext_out(&filter.src(), DEFAULT_BRIDGE_EVENT_DST, 1);
        no_tx.transaction_id = None;
        let mut no_boc = ext_out(&filter.src(), DEFAULT_BRIDGE_EVENT_DST, 2);
        no_boc.boc = None;
        block.ext_out_messages =
            vec![no_tx, no_boc, ext_out(&filter.src(), DEFAULT_BRIDGE_EVENT_DST, 3)];
        assert_eq!(
            events_of(&block, &EventFilters::default()),
            (vec![event(10, 3)], EventCounts { withdrawal: 2, voucher: 0 })
        );
    }

    #[test]
    fn event_kept_when_not_tracked() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        let filter = EventFilter::withdrawal_default();
        let mut block = arch_block(10, vec![]);
        // The block tracks a different message of the bridge.
        let mut tracked = vec![0, 0, 0, 1];
        tracked.extend_from_slice(&[0; 32]);
        tracked.extend_from_slice(&[0x1a; 32]);
        tracked.extend_from_slice(&[0, 0, 0, 1]);
        tracked.extend_from_slice(&[0x99; 32]);
        block.tracked_ext_out_message_hashes = tracked;
        block.ext_out_messages = vec![ext_out(&filter.src(), DEFAULT_BRIDGE_EVENT_DST, 1)];
        let (events, _) = events_of(&block, &EventFilters::default());
        assert!(matches!(
            store.store_arch_block(&block, &events)?,
            StoreResult::Stored(BlockWrite::Inserted(_))
        ));
        assert_eq!(store.events_in_range(10, 10)?, vec![event(10, 1)]);
        Ok(())
    }

    #[test]
    fn parse_routing_checks_both_parts() {
        assert_eq!(parse_routing(DEFAULT_BRIDGE_ROUTING), Ok(([0; 32], [0x1a; 32])));
        assert!(parse_routing("abc").is_err());
        assert!(parse_routing(&format!("{}::{}", "00".repeat(32), "1a".repeat(31))).is_err());
        assert!(parse_routing(&format!("{}:{}", "00".repeat(32), "1a".repeat(32))).is_err());
    }

    #[test]
    fn lookups_and_ranges() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        for h in [5, 6, 8] {
            store.insert_block(&row(h), &[event(h, h as u8)])?;
        }
        assert_eq!(store.row_by_block_id(&id(6))?, Some(row(6)));
        assert_eq!(store.row_by_block_id(&id(7))?, None);
        assert_eq!(store.rows_in_range(6, 8)?, vec![row(6), row(8)]);
        assert_eq!(store.events_in_range(6, 7)?, vec![event(6, 6)]);
        Ok(())
    }

    #[test]
    fn insert_fails_when_the_lock_outlives_the_timeout() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store =
            HistoryProofsStore::open_with_busy_timeout(&dir, Duration::from_millis(100))?;
        let locker = Connection::open(dir.join(HISTORY_PROOFS_DB))?;
        locker.execute_batch("BEGIN IMMEDIATE")?;
        let err = store.insert_block(&row(128), &[]).expect_err("the write must time out");
        locker.execute_batch("ROLLBACK")?;
        assert!(err.to_string().contains("locked"), "unexpected error: {err}");
        assert!(
            err.to_string().contains(HISTORY_PROOFS_DB),
            "the error does not name the file: {err}"
        );
        Ok(())
    }

    const VOUCHER_SK: [u8; 32] = [0x5c; 32];

    /// A RootPN ext-out to the voucher `dst` with `body`; `v2` picks the
    /// header with `src_dapp_id`. `seed` is its `created_lt`.
    pub(crate) fn voucher_boc_with_body(
        body: Option<tvm_types::BuilderData>,
        v2: bool,
        seed: u8,
    ) -> Vec<u8> {
        use std::str::FromStr;

        use tvm_block::ExtOutMessageHeader;
        use tvm_block::ExtOutMessageHeaderV2;
        use tvm_block::MsgAddressExt;
        use tvm_block::MsgAddressInt;
        use tvm_block::Serializable;

        let filter = EventFilter::voucher_default();
        let src = MsgAddressInt::from_str(&filter.src()).unwrap();
        let dst = MsgAddressExt::from_str(DEFAULT_VOUCHER_EVENT_DST).unwrap();
        let mut message = if v2 {
            let mut header = ExtOutMessageHeaderV2::with_addresses(src, dst);
            header.created_lt = seed as u64;
            header.created_at = 1_791_000_000.into();
            header.src_dapp_id = Some(tvm_types::UInt256::from(filter.routing_dapp_id));
            Message::with_ext_out_header_v2(header)
        } else {
            let mut header = ExtOutMessageHeader::with_addresses(src, dst);
            header.created_lt = seed as u64;
            header.created_at = 1_791_000_000.into();
            Message::with_ext_out_header(header)
        };
        if let Some(body) = body {
            message.set_body(tvm_types::SliceData::load_builder(body).unwrap());
            // 576 bits of body never fit next to two 256-bit addresses.
            message.set_serialization_params(Some(true), None);
        }
        tvm_types::write_boc(&message.serialize().unwrap()).unwrap()
    }

    /// The body of `VoucherGenerated(sk, nominal 0x11.., tokenType 7)` with event id `id`.
    pub(crate) fn voucher_body(id: u32, sk: [u8; 32]) -> tvm_types::BuilderData {
        use tvm_types::IBitstring;

        let mut body = tvm_types::BuilderData::new();
        body.append_u32(id).unwrap();
        body.append_raw(&sk, 256).unwrap();
        body.append_raw(&[0x11; 32], 256).unwrap();
        body.append_u32(7).unwrap();
        body
    }

    pub(crate) fn voucher_boc(sk: [u8; 32], seed: u8) -> Vec<u8> {
        voucher_boc_with_body(Some(voucher_body(VOUCHER_GENERATED_EVENT_ID, sk)), true, seed)
    }

    pub(crate) fn voucher(height: u64, seed: u8) -> EventRow {
        event_from_boc(
            &voucher_boc(VOUCHER_SK, seed),
            EventKind::Voucher,
            height,
            id(height),
            format!("tx-v{seed}"),
            EventFilter::voucher_default().routing_dapp_id,
        )
        .unwrap()
    }

    /// An archived RootPN ext-out with the BOC `boc`.
    pub(crate) fn archived_voucher(boc: Vec<u8>, seed: u8) -> crate::sqlite::ArchMessage {
        let filter = EventFilter::voucher_default();
        crate::sqlite::ArchMessage {
            id: hex::encode(tvm_types::read_single_root_boc(&boc).unwrap().repr_hash().inner()),
            transaction_id: Some(format!("tx-v{seed}")),
            boc: Some(boc),
            msg_type: Some(4),
            src: Some(filter.src()),
            dst: Some(filter.dst()),
            created_at: Some(1_791_000_000),
            // `ArchMessage::from` takes it from the v2 header.
            src_dapp_id: Some(hex::encode(filter.routing_dapp_id)),
            ..Default::default()
        }
    }

    #[test]
    fn voucher_event_id_matches_the_rootpn_abi() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../contracts/0.80.0_compiled/dex/RootPN.abi.json");
        let contract = tvm_abi::Contract::load(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(contract.events()["VoucherGenerated"].get_id(), VOUCHER_GENERATED_EVENT_ID);
    }

    #[test]
    fn voucher_sk_u_commit_reads_the_body() {
        for v2 in [false, true] {
            let boc = voucher_boc_with_body(
                Some(voucher_body(VOUCHER_GENERATED_EVENT_ID, VOUCHER_SK)),
                v2,
                1,
            );
            assert_eq!(voucher_sk_u_commit(&boc), Ok(VOUCHER_SK), "v2 header: {v2}");
        }
    }

    #[test]
    fn voucher_with_another_body_is_skipped() {
        use tvm_types::IBitstring;

        let mut short = tvm_types::BuilderData::new();
        short.append_u32(VOUCHER_GENERATED_EVENT_ID).unwrap();
        short.append_raw(&[0x5c; 13], 100).unwrap();
        let mut block = arch_block(10, vec![]);
        block.ext_out_messages = vec![
            archived_voucher(
                voucher_boc_with_body(Some(voucher_body(0x1234_5678, VOUCHER_SK)), true, 1),
                1,
            ),
            archived_voucher(voucher_boc_with_body(Some(short), true, 2), 2),
            archived_voucher(voucher_boc_with_body(None, true, 3), 3),
        ];
        let (events, skipped) = events_of(&block, &EventFilters::default());
        assert_eq!(events, vec![]);
        assert_eq!((skipped.withdrawal, skipped.voucher), (0, 3));
    }

    #[test]
    fn events_of_selects_both_kinds() {
        let mut block = arch_block(10, vec![]);
        block.ext_out_messages = vec![
            ext_out(&EventFilter::withdrawal_default().src(), DEFAULT_BRIDGE_EVENT_DST, 1),
            archived_voucher(voucher_boc(VOUCHER_SK, 2), 2),
        ];
        let (events, skipped) = events_of(&block, &EventFilters::default());
        assert_eq!(skipped.total(), 0);
        assert_eq!(events, vec![event(10, 1), voucher(10, 2)]);
        assert_eq!(events[0].kind, EventKind::Withdrawal);
        assert_eq!(events[0].sk_u_commit, None);
        assert_eq!(events[1].kind, EventKind::Voucher);
        assert_eq!(events[1].sk_u_commit, Some(VOUCHER_SK));
    }

    #[test]
    fn filters_with_the_same_pair_are_refused() {
        let f = EventFilter::voucher_default();
        assert!(EventFilters::new(f.clone(), f).is_err());
        assert!(EventFilters::new(
            EventFilter::withdrawal_default(),
            EventFilter::voucher_default()
        )
        .is_ok());
    }

    #[test]
    fn check_event_boc_finds_a_changed_sk_u_commit() {
        let mut changed = voucher(10, 1);
        assert_eq!(check_event_boc(&changed), Ok(()));
        changed.sk_u_commit = Some([0x5d; 32]);
        let err = check_event_boc(&changed).unwrap_err();
        assert!(err.contains("skUCommit"), "{err}");
    }

    #[test]
    fn events_table_refuses_a_voucher_without_sk_u_commit() -> anyhow::Result<()> {
        let dir = testdir!();
        let store = HistoryProofsStore::open(&dir)?;
        let insert = |kind: &str, sk: Option<[u8; 32]>| {
            store.connection().execute(
                "INSERT INTO events (msg_hash, kind, height, block_id, transaction_id, boc, src,
                     dst, routing_dapp_id, sk_u_commit)
                 VALUES (?1, ?2, 10, ?3, 'tx', x'00', 's', 'd', ?4, ?5)",
                params![[0x01u8; 32], kind, id(10), [0u8; 32], sk],
            )
        };
        assert!(insert("voucher", None).is_err());
        assert!(insert("withdrawal", Some(VOUCHER_SK)).is_err());
        assert!(insert("other", None).is_err());
        assert!(insert("withdrawal", None).is_ok());
        Ok(())
    }

    #[test]
    fn a_voucher_added_to_a_stored_row_resets_its_checks() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        store.insert_block(&row(10), &[])?;
        mark_checked(&store, 10);
        let BlockWrite::Duplicate(written) = store.insert_block(&row(10), &[voucher(10, 1)])?
        else {
            panic!("the row is stored");
        };
        assert_eq!((written.events_inserted.withdrawal, written.events_inserted.voucher), (0, 1));
        assert_eq!(verified_and_state(&store, 10), (false, false));
        assert_eq!(store.events_in_range(10, 10)?, vec![voucher(10, 1)]);
        Ok(())
    }

    #[test]
    fn real_voucher_generated_is_selected() {
        use tvm_block::Serializable;

        use crate::serialization::MessageSerializationSet;
        use crate::sqlite::ArchMessage;

        // A RootPN `VoucherGenerated` of a local network, as the node archives
        // it (v1 header, body in a ref); `tvm-cli decode msg` gives skUCommit 1.
        let boc = tvm_types::base64_decode(
            include_str!("fixtures/voucher_generated_local.boc.b64").trim(),
        )
        .unwrap();
        let message = Message::construct_from_bytes(&boc).unwrap();
        let hash = message.serialize().unwrap().repr_hash();
        let mut archived: ArchMessage =
            MessageSerializationSet { message, id: hash, boc: boc.clone(), ..Default::default() }
                .into();
        archived.transaction_id = Some("tx".to_string());
        let mut block = arch_block(10, vec![]);
        block.ext_out_messages = vec![archived];

        let (events, skipped) = events_of(&block, &EventFilters::default());
        assert_eq!(skipped.total(), 0);
        assert_eq!(events.len(), 1, "the default filters do not select the voucher");
        assert_eq!(events[0].kind, EventKind::Voucher);
        assert_eq!(
            hex::encode(events[0].msg_hash),
            "559a688613f00974527b3ecd7f1eabd66787d5cf49de342f6e1679b728a44d46"
        );
        let mut one = [0u8; 32];
        one[31] = 1;
        assert_eq!(events[0].sk_u_commit, Some(one));
        assert_eq!(check_event_boc(&events[0]), Ok(()));
    }
}
