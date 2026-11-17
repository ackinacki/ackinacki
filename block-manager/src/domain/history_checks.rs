// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! Checks of history proofs store rows against each other: the events root,
//! the block Merkle leaves, events in the tracked tree, and layer roots
//! rebuilt from the rows below them (the node's own tree code).

use database::sqlite::history_proofs::check_event_boc;
use database::sqlite::history_proofs::EventRow;
use database::sqlite::history_proofs::HistoryRow;
use history_proof::assemble_layer_tree_leaves;
use history_proof::ext_out_messages_root_from_groups;
use history_proof::history_proofs_l0;
use history_proof::layer_tree_root;
use history_proof::PoseidonHasher;
use history_proof::RowView;
use proof_data::layer_tree::LayerTreeSpec;
use proof_data::layers::check_history_proofs_for_height;
use proof_data::layers::required_layers;
use proof_data::layers::WINDOW;
use proof_data::leaves::parse_block_merkle_leaves;
use proof_data::merkle::merkle_root;
use proof_data::merkle::BLOCK_MERKLE_HISTORY_PROOFS_LEAF_INDEX;
use proof_data::merkle::BLOCK_MERKLE_TRACKED_EXT_OUT_MESSAGES_ROOT_LEAF_INDEX;
use proof_data::tracked::decode_tracked_hashes;

/// Check bits, stored in `check_state.done` / `check_state.failed`.
pub const EVENTS_ROOT: i64 = 1;
pub const MERKLE_LEAVES: i64 = 2;
pub const EVENT_IN_TREE: i64 = 4;
/// The block's leaf is in the L1 tree of its window (checked at the next boundary).
pub const LEAF_IN_L1: i64 = 8;

/// The row's own layer-`n` root (n = 1..=10) matches the tree rebuilt from the rows below.
pub fn layer_bit(n: u8) -> i64 {
    16 << (n - 1)
}

/// The stored fields of every event of the row are the ones its BOC holds,
/// and a voucher's `skUCommit` the one of its body. After the layer bits
/// (`layer_bit(10)` is 8 192).
pub const EVENT_BOC: i64 = 1 << 14;

/// The checks whose failure makes a row bad data (spec 5.7): all but
/// `event_in_tree`, which also fails on correct data (the event's routing no
/// longer tracked) and is an alert only.
pub const BAD_ROW_CHECKS: i64 = !EVENT_IN_TREE;

/// Checks that apply to a row.
pub fn applicable_checks(row: &HistoryRow, has_events: bool) -> i64 {
    let mut bits = EVENTS_ROOT | LEAF_IN_L1;
    if row.block_merkle_leaves.is_some() {
        bits |= MERKLE_LEAVES;
    }
    if has_events {
        bits |= EVENT_IN_TREE | EVENT_BOC;
    }
    for n in 1..=required_layers(row.height) {
        bits |= layer_bit(n);
    }
    bits
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckFailure {
    /// The checks that failed.
    pub bits: i64,
    /// Heights the failure is about.
    pub heights: Vec<u64>,
    pub detail: String,
}

impl CheckFailure {
    /// Only `event_in_tree` failed: an unprovable event, not bad data.
    pub fn is_alert_only(&self) -> bool {
        self.bits != 0 && self.bits & BAD_ROW_CHECKS == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckResult {
    Passed,
    /// Inputs are not in the store yet: these heights are missing or lack a layer root.
    Missing(Vec<u64>),
}

/// The row as the tree code reads it; `None` when its layer roots do not parse.
pub fn row_view(row: &HistoryRow) -> Option<RowView> {
    let layers = check_history_proofs_for_height(row.height, row.history_proofs.as_deref()).ok()?;
    Some(RowView {
        block_id: row.block_id,
        envelope_hash: row.envelope_hash,
        tracked_root: row.tracked_root,
        layers,
    })
}

fn failure(bits: i64, heights: Vec<u64>, detail: String) -> CheckFailure {
    CheckFailure { bits, heights, detail }
}

/// The tracked ext-out messages root is rebuilt from the tracked hashes
/// (32 zero bytes when there are none).
pub fn check_events_root(h: &PoseidonHasher, row: &HistoryRow) -> Result<(), CheckFailure> {
    let groups = match row.tracked_hashes.as_deref().map(decode_tracked_hashes).transpose() {
        Ok(groups) => groups.unwrap_or_default(),
        Err(err) => {
            return Err(failure(
                EVENTS_ROOT,
                vec![row.height],
                format!("tracked hashes do not parse: {err}"),
            ))
        }
    };
    let root = ext_out_messages_root_from_groups(h, &groups);
    if root == row.tracked_root {
        Ok(())
    } else {
        Err(failure(
            EVENTS_ROOT,
            vec![row.height],
            format!(
                "tracked root {} is not the root {} of the tracked hashes",
                hex::encode(row.tracked_root),
                hex::encode(root)
            ),
        ))
    }
}

/// Leaf 8 is the tracked root, leaf 0 the history proofs digest, and the
/// SHA-256 root of the 16 leaves is the block id. `Ok` for rows without leaves.
pub fn check_merkle_leaves(row: &HistoryRow) -> Result<(), CheckFailure> {
    let Some(blob) = &row.block_merkle_leaves else {
        return Ok(());
    };
    let fail = |detail: String| Err(failure(MERKLE_LEAVES, vec![row.height], detail));
    let leaves = match parse_block_merkle_leaves(blob) {
        Ok(leaves) => leaves,
        Err(err) => return fail(format!("leaves do not parse: {err}")),
    };
    if leaves[BLOCK_MERKLE_TRACKED_EXT_OUT_MESSAGES_ROOT_LEAF_INDEX] != row.tracked_root {
        return fail("leaf 8 is not the tracked ext-out messages root".to_string());
    }
    let layers = match check_history_proofs_for_height(row.height, row.history_proofs.as_deref()) {
        Ok(layers) => layers,
        Err(err) => return fail(format!("history proofs do not parse: {err}")),
    };
    if leaves[BLOCK_MERKLE_HISTORY_PROOFS_LEAF_INDEX]
        != history_proofs_l0(layers.iter().map(|(n, root)| (*n, root)))
    {
        return fail("leaf 0 is not the digest of the history proofs".to_string());
    }
    if merkle_root(&leaves) != row.block_id {
        return fail("the root of the leaves is not the block id".to_string());
    }
    Ok(())
}

/// Every stored event is in the block's tracked hashes, in the group of its
/// own routing (the `routing_dapp_id` stored with it and the account of its
/// `src`). It also fails on correct data (the routing no longer tracked), so
/// it is an alert only (spec 5.7).
pub fn check_events_in_tree(row: &HistoryRow, events: &[EventRow]) -> Result<(), CheckFailure> {
    if events.is_empty() {
        return Ok(());
    }
    let groups = row
        .tracked_hashes
        .as_deref()
        .map(decode_tracked_hashes)
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default();
    let tracked = |event: &EventRow| {
        let account = event
            .src
            .strip_prefix("0:")
            .and_then(|digits| hex::decode(digits).ok()?.try_into().ok());
        groups.iter().any(|group| {
            Some(group.account_id) == account
                && group.dapp_id == event.routing_dapp_id
                && group.hashes.contains(&event.msg_hash)
        })
    };
    let untracked: Vec<String> = events
        .iter()
        .filter(|event| !tracked(event))
        .map(|event| hex::encode(event.msg_hash))
        .collect();
    if untracked.is_empty() {
        Ok(())
    } else {
        Err(failure(
            EVENT_IN_TREE,
            vec![row.height],
            format!("events not in the tracked tree of their routing: {}", untracked.join(", ")),
        ))
    }
}

/// The stored body and fields of every event of the row are the ones of the
/// message its BOC holds: the tree has the message hash only. A mismatch is a
/// damaged row. The current flags play no part.
pub fn check_event_bocs(row: &HistoryRow, events: &[EventRow]) -> Result<(), CheckFailure> {
    let problems: Vec<String> = events
        .iter()
        .filter_map(|event| {
            check_event_boc(event).err().map(|reason| {
                format!("{} event {}: {reason}", event.kind.as_str(), hex::encode(event.msg_hash))
            })
        })
        .collect();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(failure(EVENT_BOC, vec![row.height], problems.join("; ")))
    }
}

/// Rebuilds the layer-`n` tree at boundary `t` from the rows and compares its
/// root with `history_proofs[n]` of the row at `t`.
pub fn check_layer(
    h: &PoseidonHasher,
    t: u64,
    n: u8,
    get: impl Fn(u64) -> Option<RowView>,
) -> Result<CheckResult, CheckFailure> {
    let spec = LayerTreeSpec::new(t, n).map_err(|err| {
        failure(layer_bit(n.clamp(1, 10)), vec![t], format!("no layer tree: {err}"))
    })?;
    let Some(expected) =
        get(t).and_then(|row| row.layers.iter().find(|(l, _)| *l == n).map(|(_, r)| *r))
    else {
        return Ok(CheckResult::Missing(vec![t]));
    };
    let leaves = match assemble_layer_tree_leaves(h, &spec, &get) {
        Ok(leaves) => leaves,
        Err(missing) => return Ok(CheckResult::Missing(missing)),
    };
    let root = layer_tree_root(h, &leaves);
    if root == expected {
        return Ok(CheckResult::Passed);
    }
    let marks = layer_check_marks(t, n);
    let bits = marks.iter().fold(0, |bits, (_, bit)| bits | bit);
    let mut heights: Vec<u64> = marks.into_iter().map(|(height, _)| height).collect();
    heights.sort_unstable();
    Err(failure(
        bits,
        heights,
        format!(
            "L{n} root at {t} is {}, the tree of the stored rows gives {}",
            hex::encode(expected),
            hex::encode(root)
        ),
    ))
}

/// Which rows a layer check marks, done on pass or failed on mismatch:
/// `layer_bit(n)` on `t`, and for n = 1 also `LEAF_IN_L1` on the window rows
/// `t - 128 ..= t - 1` (height 0 has no row).
pub fn layer_check_marks(t: u64, n: u8) -> Vec<(u64, i64)> {
    let mut marks = vec![(t, layer_bit(n))];
    if n == 1 {
        marks.extend((t.saturating_sub(WINDOW).max(1)..t).map(|height| (height, LEAF_IN_L1)));
    }
    marks
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use database::sqlite::history_proofs::EventFilter;
    use database::sqlite::history_proofs::EventKind;
    use serde::Deserialize;

    use super::*;

    /// A bridge event of the default routing with the fields of its BOC;
    /// `lt` is the message's `created_lt`.
    pub(crate) fn bridge_event(height: u64, block_id: [u8; 32], lt: u64) -> EventRow {
        use std::str::FromStr;

        use database::sqlite::history_proofs::event_from_boc;
        use tvm_block::ExtOutMessageHeader;
        use tvm_block::Message;
        use tvm_block::MsgAddressExt;
        use tvm_block::MsgAddressInt;
        use tvm_block::Serializable;

        let filter = EventFilter::withdrawal_default();
        let src = MsgAddressInt::from_str(&filter.src()).unwrap();
        let dst = MsgAddressExt::from_str(&filter.dst()).unwrap();
        let mut header = ExtOutMessageHeader::with_addresses(src, dst);
        header.created_lt = lt;
        header.created_at = 1_791_000_000.into();
        let message = Message::with_ext_out_header(header);
        let boc = tvm_types::write_boc(&message.serialize().unwrap()).unwrap();
        event_from_boc(
            &boc,
            EventKind::Withdrawal,
            height,
            block_id,
            format!("tx-{lt}"),
            filter.routing_dapp_id,
        )
        .unwrap()
    }

    /// A RootPN `VoucherGenerated` of `filter`'s routing with `skUCommit` `sk`,
    /// with the fields of its BOC; `lt` is the message's `created_lt`.
    pub(crate) fn voucher_event_of(
        filter: &EventFilter,
        height: u64,
        block_id: [u8; 32],
        lt: u64,
        sk: [u8; 32],
    ) -> EventRow {
        use std::str::FromStr;

        use database::sqlite::history_proofs::event_from_boc;
        use database::sqlite::history_proofs::VOUCHER_GENERATED_EVENT_ID;
        use tvm_block::ExtOutMessageHeaderV2;
        use tvm_block::Message;
        use tvm_block::MsgAddressExt;
        use tvm_block::MsgAddressInt;
        use tvm_block::Serializable;
        use tvm_types::IBitstring;

        let src = MsgAddressInt::from_str(&filter.src()).unwrap();
        let dst = MsgAddressExt::from_str(&filter.dst()).unwrap();
        let mut header = ExtOutMessageHeaderV2::with_addresses(src, dst);
        header.created_lt = lt;
        header.created_at = 1_791_000_000.into();
        header.src_dapp_id = Some(tvm_types::UInt256::from(filter.routing_dapp_id));
        let mut body = tvm_types::BuilderData::new();
        body.append_u32(VOUCHER_GENERATED_EVENT_ID).unwrap();
        body.append_raw(&sk, 256).unwrap();
        body.append_raw(&[0x11; 32], 256).unwrap();
        body.append_u32(7).unwrap();
        let mut message = Message::with_ext_out_header_v2(header);
        message.set_body(tvm_types::SliceData::load_builder(body).unwrap());
        message.set_serialization_params(Some(true), None);
        let boc = tvm_types::write_boc(&message.serialize().unwrap()).unwrap();
        event_from_boc(
            &boc,
            EventKind::Voucher,
            height,
            block_id,
            format!("tx-v{lt}"),
            filter.routing_dapp_id,
        )
        .unwrap()
    }

    /// A voucher of the default RootPN routing.
    pub(crate) fn voucher_event(
        height: u64,
        block_id: [u8; 32],
        lt: u64,
        sk: [u8; 32],
    ) -> EventRow {
        voucher_event_of(&EventFilter::voucher_default(), height, block_id, lt, sk)
    }

    #[derive(Deserialize)]
    struct Fixture {
        rows: Vec<FixtureRow>,
    }

    #[derive(Deserialize)]
    struct FixtureRow {
        height: u64,
        block_id: String,
        envelope_hash: String,
        tracked_ext_out_messages_root: String,
        history_proofs: Option<String>,
    }

    fn hash(hex_str: &str) -> [u8; 32] {
        hex::decode(hex_str).unwrap().try_into().unwrap()
    }

    /// Rows of `node/libs/history-proof/tests/fixtures/history_chain.json`.
    pub(crate) fn fixture_rows() -> Vec<HistoryRow> {
        let fixture: Fixture = serde_json::from_str(include_str!(
            "../../../node/libs/history-proof/tests/fixtures/history_chain.json"
        ))
        .unwrap();
        fixture
            .rows
            .into_iter()
            .map(|row| HistoryRow {
                height: row.height,
                block_id: hash(&row.block_id),
                envelope_hash: hash(&row.envelope_hash),
                tracked_root: hash(&row.tracked_ext_out_messages_root),
                tracked_hashes: None,
                history_proofs: row.history_proofs.map(|blob| hex::decode(blob).unwrap()),
                block_merkle_leaves: None,
                gen_utime: None,
            })
            .collect()
    }

    fn views(rows: &[HistoryRow]) -> HashMap<u64, RowView> {
        rows.iter().map(|row| (row.height, row_view(row).unwrap())).collect()
    }

    fn replace_layer(row: &mut HistoryRow, layer: u8, root: [u8; 32]) {
        let mut roots =
            check_history_proofs_for_height(row.height, row.history_proofs.as_deref()).unwrap();
        roots.iter_mut().find(|(n, _)| *n == layer).unwrap().1 = root;
        let mut blob = vec![roots.len() as u8];
        for (n, r) in roots {
            blob.push(n);
            blob.extend_from_slice(&r);
        }
        row.history_proofs = Some(blob);
    }

    #[test]
    fn layer_bits() {
        assert_eq!(layer_bit(1), 16);
        assert_eq!(layer_bit(3), 64);
        assert_eq!(layer_bit(10), 8192);
    }

    #[test]
    fn applicable_checks_by_row() {
        let rows = fixture_rows();
        let plain = rows.iter().find(|r| r.height == 16_300).unwrap();
        assert_eq!(applicable_checks(plain, false), EVENTS_ROOT | LEAF_IN_L1);
        let mut boundary = rows.iter().find(|r| r.height == 2_097_152).unwrap().clone();
        boundary.block_merkle_leaves = Some(vec![0; 512]);
        assert_eq!(
            applicable_checks(&boundary, true),
            EVENTS_ROOT
                | LEAF_IN_L1
                | MERKLE_LEAVES
                | EVENT_IN_TREE
                | EVENT_BOC
                | layer_bit(1)
                | layer_bit(2)
                | layer_bit(3)
        );
    }

    #[test]
    fn layer_check_marks_by_layer() {
        let marks = layer_check_marks(256, 1);
        assert_eq!(marks.len(), 129);
        assert!(marks.contains(&(256, layer_bit(1))));
        assert!(marks.contains(&(128, LEAF_IN_L1)));
        assert!(marks.contains(&(255, LEAF_IN_L1)));
        assert_eq!(layer_check_marks(128, 1).len(), 128, "height 0 has no row");
        assert_eq!(layer_check_marks(16_384, 2), vec![(16_384, layer_bit(2))]);
    }

    #[test]
    fn fixture_rows_pass_all_checks() {
        let rows = fixture_rows();
        let views = views(&rows);
        let h = PoseidonHasher::new();
        for (t, n) in [(16_384, 1), (16_512, 1), (16_384, 2), (2_097_152, 2), (2_097_152, 3)] {
            assert_eq!(
                check_layer(&h, t, n, |height| views.get(&height).cloned()),
                Ok(CheckResult::Passed),
                "L{n} at {t}"
            );
        }
        for row in &rows {
            assert_eq!(check_events_root(&h, row), Ok(()));
        }
    }

    #[test]
    fn substituted_layer_3_fails() {
        let mut rows = fixture_rows();
        let row = rows.iter_mut().find(|r| r.height == 2_097_152).unwrap();
        replace_layer(row, 3, [0x33; 32]);
        let views = views(&rows);
        let h = PoseidonHasher::new();
        let failure = check_layer(&h, 2_097_152, 3, |height| views.get(&height).cloned())
            .expect_err("a substituted L3 root must fail");
        assert_eq!(failure.bits, layer_bit(3));
        assert_eq!(failure.heights, vec![2_097_152]);
        // The lower layers at the same height are still right.
        assert_eq!(
            check_layer(&h, 2_097_152, 2, |height| views.get(&height).cloned()),
            Ok(CheckResult::Passed)
        );
    }

    #[test]
    fn substituted_layer_2_fails() {
        let mut rows = fixture_rows();
        let row = rows.iter_mut().find(|r| r.height == 16_384).unwrap();
        replace_layer(row, 2, [0x22; 32]);
        let views = views(&rows);
        let h = PoseidonHasher::new();
        let failure = check_layer(&h, 16_384, 2, |height| views.get(&height).cloned()).unwrap_err();
        assert_eq!((failure.bits, failure.heights), (layer_bit(2), vec![16_384]));
    }

    #[test]
    fn substituted_envelope_hash_fails_l1() {
        let mut rows = fixture_rows();
        rows.iter_mut().find(|r| r.height == 16_300).unwrap().envelope_hash = [0xee; 32];
        let views = views(&rows);
        let h = PoseidonHasher::new();
        let failure = check_layer(&h, 16_384, 1, |height| views.get(&height).cloned()).unwrap_err();
        assert_eq!(failure.bits, layer_bit(1) | LEAF_IN_L1);
        assert_eq!(failure.heights, (16_256..=16_384).collect::<Vec<u64>>());
    }

    #[test]
    fn missing_input_reports_heights() {
        let mut rows = fixture_rows();
        rows.retain(|r| r.height != 16_300);
        let views = views(&rows);
        let h = PoseidonHasher::new();
        assert_eq!(
            check_layer(&h, 16_384, 1, |height| views.get(&height).cloned()),
            Ok(CheckResult::Missing(vec![16_300]))
        );
        assert_eq!(
            check_layer(&h, 16_512 + 128, 1, |height| views.get(&height).cloned()),
            Ok(CheckResult::Missing(vec![16_640]))
        );
    }

    fn row_with_leaves(tracked_root: [u8; 32]) -> HistoryRow {
        let history_proofs = {
            let mut blob = vec![1, 1];
            blob.extend_from_slice(&[0x61; 32]);
            blob
        };
        let mut leaves = [[0u8; 32]; 16];
        leaves[0] = history_proof::history_proofs_l0([(1u8, &[0x61; 32])]);
        leaves[1] = [0x01; 32];
        leaves[8] = tracked_root;
        HistoryRow {
            height: 128,
            block_id: merkle_root(&leaves),
            envelope_hash: [0xe0; 32],
            tracked_root,
            tracked_hashes: None,
            history_proofs: Some(history_proofs),
            block_merkle_leaves: Some(leaves.concat()),
            gen_utime: None,
        }
    }

    #[test]
    fn merkle_leaves_checks() {
        let row = row_with_leaves([0; 32]);
        assert_eq!(check_merkle_leaves(&row), Ok(()));

        let mut random = row.clone();
        random.block_merkle_leaves = Some((0..512).map(|i| (i * 7 % 251) as u8).collect());
        assert_eq!(check_merkle_leaves(&random).unwrap_err().bits, MERKLE_LEAVES);

        // Leaf 8 differs from the stored tracked root, the id still matches the leaves.
        let mut wrong_leaf8 = row_with_leaves([0x08; 32]);
        wrong_leaf8.tracked_root = [0; 32];
        assert_eq!(check_merkle_leaves(&wrong_leaf8).unwrap_err().heights, vec![128]);

        let mut no_leaves = row.clone();
        no_leaves.block_merkle_leaves = None;
        assert_eq!(check_merkle_leaves(&no_leaves), Ok(()));
    }

    #[test]
    fn leaf0_must_be_the_history_proofs_digest() {
        // Leaf 0 changed and the block id recomputed: only the leaf 0 check can see it.
        let mut row = row_with_leaves([0; 32]);
        let mut leaves =
            parse_block_merkle_leaves(row.block_merkle_leaves.as_ref().unwrap()).unwrap();
        leaves[BLOCK_MERKLE_HISTORY_PROOFS_LEAF_INDEX] = [0x99; 32];
        row.block_id = merkle_root(&leaves);
        row.block_merkle_leaves = Some(leaves.concat());
        let failure = check_merkle_leaves(&row).unwrap_err();
        assert_eq!(failure.bits, MERKLE_LEAVES);
        assert!(failure.detail.contains("leaf 0"), "{failure:?}");
    }

    #[test]
    fn leaves_must_hash_to_the_block_id() {
        // Leaves consistent with the row, but another block id: the only link
        // between the stored row and the block id the BKs signed.
        let mut row = row_with_leaves([0; 32]);
        row.block_id = [0x42; 32];
        let failure = check_merkle_leaves(&row).unwrap_err();
        assert_eq!(failure.bits, MERKLE_LEAVES);
        assert!(failure.detail.contains("block id"), "{failure:?}");
    }

    fn tracked(groups: &[([u8; 32], [u8; 32], Vec<[u8; 32]>)]) -> Vec<u8> {
        let mut blob = (groups.len() as u32).to_be_bytes().to_vec();
        for (dapp, account, hashes) in groups {
            blob.extend_from_slice(dapp);
            blob.extend_from_slice(account);
            blob.extend_from_slice(&(hashes.len() as u32).to_be_bytes());
            for hash in hashes {
                blob.extend_from_slice(hash);
            }
        }
        blob
    }

    #[test]
    fn events_root_zero_when_no_events() {
        let h = PoseidonHasher::new();
        let mut row = row_with_leaves([0; 32]);
        assert_eq!(check_events_root(&h, &row), Ok(()));
        row.tracked_root = [1; 32];
        assert_eq!(check_events_root(&h, &row).unwrap_err().bits, EVENTS_ROOT);

        let groups = [([0; 32], [0x1a; 32], vec![[0x55; 32]])];
        let decoded = proof_data::tracked::decode_tracked_hashes(&tracked(&groups)).unwrap();
        row.tracked_hashes = Some(tracked(&groups));
        row.tracked_root = history_proof::ext_out_messages_root_from_groups(&h, &decoded);
        assert_eq!(check_events_root(&h, &row), Ok(()));
    }

    #[test]
    fn only_an_unprovable_event_is_an_alert_only() {
        let failure = |bits| CheckFailure { bits, heights: vec![10], detail: String::new() };
        assert!(failure(EVENT_IN_TREE).is_alert_only());
        assert!(!failure(EVENT_IN_TREE | MERKLE_LEAVES).is_alert_only());
        assert!(!failure(EVENT_BOC).is_alert_only());
        assert!(!failure(0).is_alert_only());
    }

    #[test]
    fn bridge_event_not_in_tree() {
        let (tracked_event, other_routing) =
            (bridge_event(128, [0; 32], 1), bridge_event(128, [0; 32], 2));
        let mut row = row_with_leaves([0; 32]);
        row.tracked_hashes = Some(tracked(&[
            ([0; 32], [0x1a; 32], vec![tracked_event.msg_hash]),
            ([4; 32], [0x10; 32], vec![other_routing.msg_hash]),
        ]));
        assert_eq!(check_events_in_tree(&row, std::slice::from_ref(&tracked_event)), Ok(()));
        // Tracked, but under another routing.
        let failure = check_events_in_tree(&row, &[other_routing]).unwrap_err();
        assert_eq!((failure.bits, failure.heights), (EVENT_IN_TREE, vec![128]));
        row.tracked_hashes = None;
        assert!(check_events_in_tree(&row, &[tracked_event]).is_err());
        assert_eq!(check_events_in_tree(&row, &[]), Ok(()));
    }

    /// The group is the one of the event's own routing, stored with it, not
    /// the current flag: rows from before a change of the bridge routing stay
    /// provable.
    #[test]
    fn bridge_event_is_checked_in_its_own_routing() {
        let mut old = bridge_event(128, [0; 32], 1);
        old.routing_dapp_id = [0x05; 32];
        let mut row = row_with_leaves([0; 32]);
        row.tracked_hashes = Some(tracked(&[([0x05; 32], [0x1a; 32], vec![old.msg_hash])]));
        assert_eq!(check_events_in_tree(&row, std::slice::from_ref(&old)), Ok(()));
        // The same hash in the group of another dApp is not this event's.
        old.routing_dapp_id = [0; 32];
        assert!(check_events_in_tree(&row, &[old]).is_err());
    }

    /// The tree holds the message hash only: a stored event with a tracked hash
    /// and another body or other fields is not the tracked message. That is a
    /// bad row of its own (`EVENT_BOC`), apart from the tree check,
    /// which is an alert only.
    #[test]
    fn bridge_event_with_another_body_fails() {
        let genuine = bridge_event(128, [0; 32], 1);
        let mut row = row_with_leaves([0; 32]);
        row.tracked_hashes = Some(tracked(&[([0; 32], [0x1a; 32], vec![genuine.msg_hash])]));
        assert_eq!(check_events_in_tree(&row, std::slice::from_ref(&genuine)), Ok(()));
        assert_eq!(check_event_bocs(&row, std::slice::from_ref(&genuine)), Ok(()));
        let changed_body = EventRow { boc: bridge_event(128, [0; 32], 2).boc, ..genuine.clone() };
        let changed_time = EventRow { created_at: Some(1), ..genuine };
        for event in [changed_body, changed_time] {
            assert_eq!(check_events_in_tree(&row, std::slice::from_ref(&event)), Ok(()));
            let failure = check_event_bocs(&row, &[event]).unwrap_err();
            assert_eq!((failure.bits, failure.heights), (EVENT_BOC, vec![128]));
        }
    }

    /// A voucher is checked in the RootPN group, like a withdrawal in the
    /// bridge group.
    #[test]
    fn voucher_is_checked_in_the_root_pn_group() {
        let voucher = voucher_event(128, [0; 32], 1, [0x5c; 32]);
        let f = EventFilter::voucher_default();
        let mut row = row_with_leaves([0; 32]);
        row.tracked_hashes =
            Some(tracked(&[(f.routing_dapp_id, f.routing_account_id, vec![voucher.msg_hash])]));
        assert_eq!(check_events_in_tree(&row, std::slice::from_ref(&voucher)), Ok(()));
        row.tracked_hashes = Some(tracked(&[([0; 32], [0x1a; 32], vec![voucher.msg_hash])]));
        let failure = check_events_in_tree(&row, &[voucher]).unwrap_err();
        assert_eq!((failure.bits, failure.heights), (EVENT_IN_TREE, vec![128]));
    }

    #[test]
    fn voucher_with_another_sk_u_commit_fails_event_boc() {
        let row = row_with_leaves([0; 32]);
        let genuine = voucher_event(128, [0; 32], 1, [0x5c; 32]);
        assert_eq!(check_event_bocs(&row, std::slice::from_ref(&genuine)), Ok(()));
        let changed = EventRow { sk_u_commit: Some([0x5d; 32]), ..genuine };
        let failure = check_event_bocs(&row, &[changed]).unwrap_err();
        assert_eq!((failure.bits, failure.heights), (EVENT_BOC, vec![128]));
        assert!(failure.detail.contains("skUCommit"), "{}", failure.detail);
    }

    /// `event_boc` takes nothing from the flags: an event of a routing the
    /// current flags no longer select stays good.
    #[test]
    fn event_boc_ignores_the_current_filters() {
        let row = row_with_leaves([0; 32]);
        let old_routing =
            EventFilter { routing_account_id: [0x1d; 32], ..EventFilter::voucher_default() };
        let voucher = voucher_event_of(&old_routing, 128, [0; 32], 1, [0x5c; 32]);
        assert_eq!(check_event_bocs(&row, &[voucher]), Ok(()));
    }
}
