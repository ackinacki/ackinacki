// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! Background verification of the history proofs store. Each pass walks the
//! rows that are covered but not verified, newest first, one unit of 129
//! heights at a time (the L1 window `T - 128 ..= T - 1` and its boundary `T`),
//! runs every check whose inputs are in the store, and records the results in
//! `check_state` and `verified`. Each unit is one short write transaction: the
//! block-manager writer must never wait anywhere near its busy timeout.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use database::sqlite::history_bk_set;
use database::sqlite::history_proofs::EventRow;
use database::sqlite::history_proofs::HistoryProofsStore;
use database::sqlite::history_proofs::HistoryRow;
use database::sqlite::history_proofs::HISTORY_PROOFS_BUSY_TIMEOUT;
use database::sqlite::history_ranges;
use database::sqlite::history_ranges::RangeTable;
use history_proof::PoseidonHasher;
use proof_data::layers::required_layers;
use proof_data::layers::WINDOW;
use rusqlite::params;
use rusqlite::TransactionBehavior;

use crate::application::metrics::Metrics;
use crate::domain::history_checks::applicable_checks;
use crate::domain::history_checks::check_event_bocs;
use crate::domain::history_checks::check_events_in_tree;
use crate::domain::history_checks::check_events_root;
use crate::domain::history_checks::check_layer;
use crate::domain::history_checks::check_merkle_leaves;
use crate::domain::history_checks::layer_bit;
use crate::domain::history_checks::layer_check_marks;
use crate::domain::history_checks::row_view;
use crate::domain::history_checks::CheckFailure;
use crate::domain::history_checks::CheckResult;
use crate::domain::history_checks::BAD_ROW_CHECKS;
use crate::domain::history_checks::EVENTS_ROOT;
use crate::domain::history_checks::EVENT_BOC;
use crate::domain::history_checks::EVENT_IN_TREE;
use crate::domain::history_checks::LEAF_IN_L1;
use crate::domain::history_checks::MERKLE_LEAVES;

pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);
/// Units with checks to run per pass; the rest wait for the next pass.
pub const MAX_WINDOWS_PER_PASS: usize = 64;
/// Units read per pass, with or without checks to run.
const MAX_SCANNED_UNITS_PER_PASS: usize = 4096;
/// The BK set tables are checked against the block rows once per this many
/// passes (5 minutes at the default interval): the check joins every
/// attestation of the permanent file, and only a manual edit breaks them.
const BK_SET_CHECK_EVERY_PASSES: u64 = 60;
/// Pause after each unit's write transaction, so a waiting writer gets the lock.
const WRITE_PAUSE: Duration = Duration::from_millis(2);
/// A pause longer than SQLite's longest busy-handler sleep (100 ms). SQLite
/// locks are not fair: a writer waiting for the lock can miss the short
/// `WRITE_PAUSE` windows, so the verifier yields this long at the end of a
/// pass that wrote, and within a pass after `YIELD_AFTER` of writing.
const YIELD_PAUSE: Duration = Duration::from_millis(110);
const YIELD_AFTER: Duration = Duration::from_millis(150);
/// Blocks the DEX proves against: the window of the missing-blocks gauge.
pub const DEX_WINDOW_BLOCKS: u64 = 2_113_536;

/// Labels of `bm_history_check_failures{check}`.
pub const FAILURE_LABELS: [&str; 14] = [
    "events_root",
    "merkle_leaves",
    "event_in_tree",
    "l1",
    "layer_2",
    "layer_3",
    "layer_4",
    "layer_5",
    "layer_6",
    "layer_7",
    "layer_8",
    "layer_9",
    "layer_10",
    "event_boc",
];

/// What one pass saw; reported as gauges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifierStats {
    pub store: StoreGauges,
    /// Rows with a failed check, per label of `FAILURE_LABELS`.
    pub failures: BTreeMap<&'static str, u64>,
    /// Not verified, every remaining check can run now.
    pub checkable: u64,
    /// Not verified, some check lacks rows in the store.
    pub not_checkable: u64,
    /// Units where checks ran in this pass.
    pub worked_units: usize,
    /// BK set update rows and attestation rows that do not match their block
    /// rows (`history_bk_set::inconsistent_bk_set_rows`); `None` when the pass
    /// did not check them.
    pub inconsistent_bk_set_rows: Option<(u64, u64)>,
}

/// What the store holds, whatever the checks: read on every pass, also when
/// the checks fail, so the gap alerts keep working unless this read fails too.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreGauges {
    pub first_last: Option<(u64, u64)>,
    pub missing_dex_window: u64,
    pub missing_total: u64,
    /// Key and rotation blocks without a PRIMARY attestation
    /// (`history_bk_set::missing_attestations_count`).
    pub missing_attestations: u64,
}

pub fn store_gauges(conn: &rusqlite::Connection) -> anyhow::Result<StoreGauges> {
    let first_last = history_ranges::first_last(conn)?;
    let missing_total = history_ranges::gaps(conn)?.iter().map(|(a, b)| b - a + 1).sum();
    let missing_dex_window = match first_last {
        Some((first, last)) => {
            let from = last.saturating_sub(DEX_WINDOW_BLOCKS).max(first);
            history_ranges::missing_in(conn, from, last)?.iter().map(|(a, b)| b - a + 1).sum()
        }
        None => 0,
    };
    let missing_attestations = history_bk_set::missing_attestations_count(conn)?;
    Ok(StoreGauges { first_last, missing_dex_window, missing_total, missing_attestations })
}

#[derive(Debug)]
pub enum PassOutcome {
    Done(VerifierStats),
    /// The checks failed; `store` is read on its own (`None` when that failed too).
    Failed {
        error: anyhow::Error,
        store: Option<StoreGauges>,
    },
}

/// One verifier pass. A failed pass still reads the store gauges.
pub fn run_pass(
    store: &HistoryProofsStore,
    hasher: &PoseidonHasher,
    max_windows: usize,
    check_bk_set_tables: bool,
) -> PassOutcome {
    match verify_pass(store, hasher, max_windows, check_bk_set_tables) {
        Ok(stats) => PassOutcome::Done(stats),
        Err(error) => {
            let gauges = store_gauges(store.connection())
                .map_err(|err| {
                    tracing::error!(target: "history", "history store gauges: {err:#}");
                })
                .ok();
            PassOutcome::Failed { error, store: gauges }
        }
    }
}

/// Starts the verifier thread on its own connection to the store in `data_dir`.
pub fn run(
    data_dir: PathBuf,
    metrics: Option<Metrics>,
    interval: Duration,
) -> anyhow::Result<std::thread::JoinHandle<()>> {
    let store = HistoryProofsStore::open_with_busy_timeout(&data_dir, HISTORY_PROOFS_BUSY_TIMEOUT)?;
    let handle =
        std::thread::Builder::new().name("history-verifier".to_string()).spawn(move || {
            let hasher = PoseidonHasher::new();
            let mut inconsistent = (0, 0);
            let mut pass: u64 = 0;
            loop {
                let check_bk_set_tables = pass.is_multiple_of(BK_SET_CHECK_EVERY_PASSES);
                pass = pass.wrapping_add(1);
                match run_pass(&store, &hasher, MAX_WINDOWS_PER_PASS, check_bk_set_tables)
                {
                    PassOutcome::Done(stats) => {
                        if let Some(now) = stats.inconsistent_bk_set_rows {
                            if now != inconsistent && now != (0, 0) {
                                tracing::error!(
                                    target: "history",
                                    "history proofs store: {} BK set update rows and {} attestation rows do not match their block rows",
                                    now.0,
                                    now.1
                                );
                            }
                            inconsistent = now;
                        }
                        if let Some(metrics) = &metrics {
                            metrics.bm.report_history_verifier(&stats);
                        }
                    }
                    PassOutcome::Failed { error, store: gauges } => {
                        tracing::error!(target: "history", "{}", pass_failure_message(&error));
                        if let Some(metrics) = &metrics {
                            metrics.bm.report_history_verifier_failure();
                            if let Some(gauges) = &gauges {
                                metrics.bm.report_history_store(gauges);
                            }
                        }
                    }
                }
                std::thread::sleep(interval);
            }
        })?;
    Ok(handle)
}

/// `(height, failed bits)` of the rows of `from ..= to` that failed a check
/// of `BAD_ROW_CHECKS`, with those bits only. The bits stay until the store
/// is restored: the verifier does not run a failed check again, and these
/// rows are never marked verified.
pub fn failed_check_states(
    conn: &rusqlite::Connection,
    from: u64,
    to: u64,
) -> rusqlite::Result<Vec<(u64, i64)>> {
    conn.prepare_cached(
        "SELECT height, failed & ?3 FROM check_state
         WHERE failed & ?3 != 0 AND height BETWEEN ?1 AND ?2 ORDER BY height",
    )?
    .query_map(params![from as i64, to.min(i64::MAX as u64) as i64, BAD_ROW_CHECKS], |r| {
        Ok((r.get::<_, i64>(0)? as u64, r.get(1)?))
    })?
    .collect()
}

/// The heights of `from ..= to` with a failed check of `BAD_ROW_CHECKS`, as
/// ranges.
pub fn failed_check_ranges(
    conn: &rusqlite::Connection,
    from: u64,
    to: u64,
) -> rusqlite::Result<Vec<(u64, u64)>> {
    Ok(height_ranges(failed_check_states(conn, from, to)?.into_iter().map(|(height, _)| height)))
}

/// The heights of `from ..= to` with an unprovable event (an alert
/// only, spec 5.7), as ranges. Their rows can be verified: the bit stays in
/// `check_state` for `bm_history_check_failures` and for this list.
pub fn unprovable_event_ranges(
    conn: &rusqlite::Connection,
    from: u64,
    to: u64,
) -> rusqlite::Result<Vec<(u64, u64)>> {
    let heights = conn
        .prepare_cached(
            "SELECT height FROM check_state
             WHERE failed & ?3 != 0 AND height BETWEEN ?1 AND ?2 ORDER BY height",
        )?
        .query_map(params![from as i64, to.min(i64::MAX as u64) as i64, EVENT_IN_TREE], |r| {
            Ok(r.get::<_, i64>(0)? as u64)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(height_ranges(heights))
}

/// Ascending heights as ranges of consecutive ones.
pub fn height_ranges(heights: impl IntoIterator<Item = u64>) -> Vec<(u64, u64)> {
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    for height in heights {
        match ranges.last_mut() {
            Some((_, end)) if *end + 1 == height => *end = height,
            _ => ranges.push((height, height)),
        }
    }
    ranges
}

/// Runs the row-local checks (events root, Merkle leaves, events in
/// the tracked tree and against their BOC) on freshly imported rows and
/// records them in `check_state`, in one short transaction. Returns the
/// failed checks, `event_in_tree` (an alert only) among them: the
/// verifier does not run a check again once its bit is set, so the caller
/// reports them.
pub fn record_row_checks(
    store: &HistoryProofsStore,
    hasher: &PoseidonHasher,
    items: &[(&HistoryRow, &[EventRow])],
) -> anyhow::Result<Vec<CheckFailure>> {
    if items.is_empty() {
        return Ok(vec![]);
    }
    let mut failures = Vec::new();
    let mut results = Vec::with_capacity(items.len());
    for (row, events) in items {
        let mut checks = vec![(EVENTS_ROOT, check_events_root(hasher, row))];
        if row.block_merkle_leaves.is_some() {
            checks.push((MERKLE_LEAVES, check_merkle_leaves(row)));
        }
        if !events.is_empty() {
            checks.push((EVENT_IN_TREE, check_events_in_tree(row, events)));
            checks.push((EVENT_BOC, check_event_bocs(row, events)));
        }
        let (mut done, mut failed) = (0, 0);
        for (bit, result) in checks {
            done |= bit;
            if let Err(failure) = result {
                failures.push(failure);
                failed |= bit;
            }
        }
        results.push((row.height, done, failed));
    }
    let tx =
        rusqlite::Transaction::new_unchecked(store.connection(), TransactionBehavior::Immediate)?;
    for (height, done, failed) in &results {
        tx.execute(
            "INSERT INTO check_state (height, done, failed) VALUES (?1, ?2, ?3)
             ON CONFLICT(height) DO UPDATE SET done = done | excluded.done, failed = failed | excluded.failed",
            params![*height as i64, done, failed],
        )?;
    }
    tx.commit()?;
    Ok(failures)
}

/// One pass over the unverified rows; checks run in at most `max_windows` units.
pub fn verify_pass(
    store: &HistoryProofsStore,
    hasher: &PoseidonHasher,
    max_windows: usize,
    check_bk_set_tables: bool,
) -> anyhow::Result<VerifierStats> {
    let conn = store.connection();
    let unverified = history_ranges::unverified(conn)?;

    // Unverified rows left with a check that cannot run yet: height -> has a failed check.
    let mut pending = BTreeMap::new();
    let mut worked_units = 0;
    let mut scanned_units = 0;
    let mut last_yield = Instant::now();
    let mut last_unit: Option<u64> = None;
    'ranges: for &(start, end) in unverified.iter().rev() {
        // Unit T holds rows T - 128 ..= T: the units of `start ..= end`, newest first.
        let lowest = start.div_ceil(WINDOW).max(1) * WINDOW;
        let mut unit = (end / WINDOW + 1) * WINDOW;
        while unit >= lowest {
            if last_unit.is_none_or(|last| unit < last) {
                if scanned_units >= MAX_SCANNED_UNITS_PER_PASS || worked_units >= max_windows {
                    break 'ranges;
                }
                scanned_units += 1;
                last_unit = Some(unit);
                if verify_unit(store, hasher, unit, &unverified, &mut pending, &mut last_yield)? {
                    worked_units += 1;
                }
            }
            unit -= WINDOW;
        }
    }

    let mut failures: BTreeMap<&'static str, u64> =
        FAILURE_LABELS.iter().map(|label| (*label, 0)).collect();
    // Bad rows are never verified; a row with an alert only can be.
    let mut bad_rows = 0;
    {
        let mut stmt = conn.prepare_cached("SELECT failed FROM check_state WHERE failed != 0")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let failed: i64 = row.get(0)?;
            if failed & BAD_ROW_CHECKS != 0 {
                bad_rows += 1;
            }
            for label in failure_labels(failed) {
                *failures.entry(label).or_default() += 1;
            }
        }
    }
    let total_unverified: u64 = unverified.iter().map(|(a, b)| b - a + 1).sum();
    let not_checkable = pending.values().filter(|failed| !**failed).count() as u64;

    if worked_units > 0 {
        std::thread::sleep(YIELD_PAUSE);
    }
    let store_gauges = store_gauges(conn)?;
    let inconsistent_bk_set_rows = if check_bk_set_tables {
        Some(history_bk_set::inconsistent_bk_set_rows(conn)?)
    } else {
        None
    };
    Ok(VerifierStats {
        store: store_gauges,
        failures,
        checkable: total_unverified.saturating_sub(not_checkable + bad_rows),
        not_checkable,
        worked_units,
        inconsistent_bk_set_rows,
    })
}

/// Runs the checks of unit `t` that have their inputs: row-local checks of the
/// window rows `t - 128 ..= t - 1`, L1 at `t`, and the higher layers of row
/// `t`. Returns whether any check ran.
fn verify_unit(
    store: &HistoryProofsStore,
    hasher: &PoseidonHasher,
    t: u64,
    unverified: &[(u64, u64)],
    pending: &mut BTreeMap<u64, bool>,
    last_yield: &mut Instant,
) -> anyhow::Result<bool> {
    let window_start = (t - WINDOW).max(1);
    let rows: BTreeMap<u64, HistoryRow> =
        store.rows_in_range(window_start, t)?.into_iter().map(|row| (row.height, row)).collect();
    let mut events: HashMap<u64, Vec<EventRow>> = HashMap::new();
    for event in store.events_in_range(window_start, t)? {
        events.entry(event.height).or_default().push(event);
    }
    let states = check_states(store.connection(), window_start, t)?;
    let state = |height: u64| states.get(&height).copied().unwrap_or((0, 0));
    let open = |height: u64| contains(unverified, height);
    // A failed read fails the pass (bm_history_verifier_failures): taken as a
    // missing row, its check would wait for good.
    let read_error: RefCell<Option<anyhow::Error>> = RefCell::new(None);
    let view = |height: u64| match rows.get(&height) {
        Some(row) => row_view(row),
        None => match store.row(height) {
            Ok(row) => row.and_then(|row| row_view(&row)),
            Err(err) => {
                read_error.borrow_mut().get_or_insert(err.context(format!("reading row {height}")));
                None
            }
        },
    };

    let mut marks: BTreeMap<u64, (i64, i64)> = BTreeMap::new();
    let record = |marks: &mut BTreeMap<u64, (i64, i64)>, height: u64, bit: i64, ok: bool| {
        if open(height) {
            let entry = marks.entry(height).or_default();
            if ok {
                entry.0 |= bit;
            } else {
                entry.1 |= bit;
                entry.0 |= bit;
            }
        }
    };
    let mut worked = false;

    for (&height, row) in rows.range(window_start..t) {
        if !open(height) {
            continue;
        }
        let row_events = events.get(&height).map(Vec::as_slice).unwrap_or_default();
        let applicable = applicable_checks(row, !row_events.is_empty());
        let (done, _) = state(height);
        let todo = |bit: i64| applicable & bit != 0 && done & bit == 0;
        let mut row_checks: Vec<(i64, Result<(), CheckFailure>)> = Vec::new();
        if todo(EVENTS_ROOT) {
            row_checks.push((EVENTS_ROOT, check_events_root(hasher, row)));
        }
        if todo(MERKLE_LEAVES) {
            row_checks.push((MERKLE_LEAVES, check_merkle_leaves(row)));
        }
        if todo(EVENT_IN_TREE) {
            row_checks.push((EVENT_IN_TREE, check_events_in_tree(row, row_events)));
        }
        if todo(EVENT_BOC) {
            row_checks.push((EVENT_BOC, check_event_bocs(row, row_events)));
        }
        for (bit, result) in row_checks {
            worked = true;
            if let Err(failure) = &result {
                log_failure(failure);
            }
            record(&mut marks, height, bit, result.is_ok());
        }
    }

    let mut layer_checks = Vec::new();
    let l1_needed = rows
        .range(window_start..t)
        .any(|(&height, _)| open(height) && state(height).0 & LEAF_IN_L1 == 0)
        || (rows.contains_key(&t) && open(t) && state(t).0 & layer_bit(1) == 0);
    if l1_needed {
        layer_checks.push(1);
    }
    if rows.contains_key(&t) && open(t) {
        layer_checks.extend((2..=required_layers(t)).filter(|n| state(t).0 & layer_bit(*n) == 0));
    }
    for n in layer_checks {
        let result = check_layer(hasher, t, n, view);
        if let Some(err) = read_error.take() {
            return Err(err);
        }
        let ok = match &result {
            Ok(CheckResult::Missing(_)) => continue,
            Ok(CheckResult::Passed) => true,
            Err(failure) => {
                log_failure(failure);
                false
            }
        };
        worked = true;
        for (height, bit) in layer_check_marks(t, n) {
            record(&mut marks, height, bit, ok);
        }
    }

    if !marks.is_empty() {
        write_marks(store, &marks, &rows, &events, pending)?;
        // SQLite locks are not fair: without a pause the next unit's write could
        // take the lock again before the block-manager writer gets it.
        if last_yield.elapsed() >= YIELD_AFTER {
            std::thread::sleep(YIELD_PAUSE);
            *last_yield = Instant::now();
        } else {
            std::thread::sleep(WRITE_PAUSE);
        }
    }

    // What is left on the unit's open rows after its checks cannot run now.
    let states = check_states(store.connection(), window_start, t)?;
    for (&height, row) in &rows {
        if !open(height) {
            continue;
        }
        let (done, failed) = states.get(&height).copied().unwrap_or((0, 0));
        let has_events = events.contains_key(&height);
        let applicable = applicable_checks(row, has_events);
        if is_verified(done, failed, applicable) {
            pending.remove(&height);
        } else {
            pending.insert(height, failed & BAD_ROW_CHECKS != 0);
        }
    }
    Ok(worked)
}

/// Writes the check results of a unit in one transaction, merged with the
/// check state read inside it. A live write may have added an event to a row
/// (and reset its state) after the checks read the row's events: such a row
/// is left for the next pass, which checks the new event.
fn write_marks(
    store: &HistoryProofsStore,
    marks: &BTreeMap<u64, (i64, i64)>,
    rows: &BTreeMap<u64, HistoryRow>,
    checked_events: &HashMap<u64, Vec<EventRow>>,
    pending: &mut BTreeMap<u64, bool>,
) -> anyhow::Result<()> {
    let tx =
        rusqlite::Transaction::new_unchecked(store.connection(), TransactionBehavior::Immediate)?;
    for (&height, &(done, failed)) in marks {
        let (old_done, old_failed) =
            check_states(&tx, height, height)?.get(&height).copied().unwrap_or((0, 0));
        let (done, failed) = (old_done | done, old_failed | failed);
        let row = match rows.get(&height) {
            Some(row) => row.clone(),
            None => match store.row(height)? {
                Some(row) => row,
                None => continue,
            },
        };
        let events = store.events_in_range(height, height)?;
        // The rows of the unit's window were checked with these events.
        if rows.contains_key(&height)
            && events.as_slice()
                != checked_events.get(&height).map(Vec::as_slice).unwrap_or_default()
        {
            continue;
        }
        let applicable = applicable_checks(&row, !events.is_empty());
        let verified = is_verified(done, failed, applicable);
        if verified {
            history_ranges::add_height(&tx, RangeTable::Verified, height)?;
            pending.remove(&height);
        }
        // An alert stays in check_state for bm_history_check_failures.
        if verified && failed == 0 {
            tx.execute("DELETE FROM check_state WHERE height = ?1", params![height as i64])?;
        } else {
            tx.execute(
                "INSERT INTO check_state (height, done, failed) VALUES (?1, ?2, ?3)
                 ON CONFLICT(height) DO UPDATE SET done = excluded.done, failed = excluded.failed",
                params![height as i64, done, failed],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// A row is verified when every applicable check ran and none of a bad row
/// failed: an unprovable event is an alert only (spec 5.7), and a row
/// left unverified for it would be read again on every pass.
fn is_verified(done: i64, failed: i64, applicable: i64) -> bool {
    failed & BAD_ROW_CHECKS == 0 && done & applicable == applicable
}

fn check_states(
    conn: &rusqlite::Connection,
    start: u64,
    end: u64,
) -> rusqlite::Result<HashMap<u64, (i64, i64)>> {
    let mut stmt = conn.prepare_cached(
        "SELECT height, done, failed FROM check_state WHERE height BETWEEN ?1 AND ?2",
    )?;
    let rows = stmt.query_map(params![start as i64, end as i64], |r| {
        Ok((r.get::<_, i64>(0)? as u64, (r.get(1)?, r.get(2)?)))
    })?;
    rows.collect()
}

/// The log line of a failed pass.
fn pass_failure_message(error: &anyhow::Error) -> String {
    format!("history verifier pass failed: {error:#}")
}

fn log_failure(failure: &CheckFailure) {
    match failure_log(failure) {
        (tracing::Level::WARN, line) => tracing::warn!(target: "history", "{line}"),
        (_, line) => tracing::error!(target: "history", "{line}"),
    }
}

/// The level and the line of a failed check in the log: a warning for an
/// unprovable event (an alert only, spec 5.7), an error for a check of
/// a bad row.
fn failure_log(failure: &CheckFailure) -> (tracing::Level, String) {
    let line = format!("history proofs {}", describe_failure(failure));
    if failure.is_alert_only() {
        (tracing::Level::WARN, format!("{line} {ALERT_ONLY}"))
    } else {
        (tracing::Level::ERROR, line)
    }
}

/// Added to the line of an unprovable event in the log and in the
/// `import` output.
pub const ALERT_ONLY: &str = "(an alert only: the event cannot be proven)";

/// `check <labels> failed at heights <first>..=<last>: <detail>`.
pub fn describe_failure(failure: &CheckFailure) -> String {
    let labels: Vec<&str> = failure_labels(failure.bits).collect();
    let first = failure.heights.first().copied().unwrap_or_default();
    let last = failure.heights.last().copied().unwrap_or_default();
    format!("check {} failed at heights {first}..={last}: {}", labels.join(","), failure.detail)
}

/// Labels of `bm_history_check_failures` for a set of failed bits.
fn failure_labels(bits: i64) -> impl Iterator<Item = &'static str> {
    let mut labels = Vec::new();
    if bits & EVENTS_ROOT != 0 {
        labels.push("events_root");
    }
    if bits & MERKLE_LEAVES != 0 {
        labels.push("merkle_leaves");
    }
    if bits & EVENT_IN_TREE != 0 {
        labels.push("event_in_tree");
    }
    if bits & (LEAF_IN_L1 | layer_bit(1)) != 0 {
        labels.push("l1");
    }
    for n in 2..=10u8 {
        if bits & layer_bit(n) != 0 {
            labels.push(FAILURE_LABELS[n as usize + 2]);
        }
    }
    if bits & EVENT_BOC != 0 {
        labels.push("event_boc");
    }
    labels.into_iter()
}

fn contains(ranges: &[(u64, u64)], height: u64) -> bool {
    let index = ranges.partition_point(|(start, _)| *start <= height);
    index > 0 && ranges[index - 1].1 >= height
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use database::sqlite::history_proofs::ImportItem;
    use database::sqlite::history_proofs::ImportMode;
    use testdir::testdir;

    use super::*;
    use crate::domain::history_checks::tests::bridge_event;
    use crate::domain::history_checks::tests::fixture_rows;

    fn store_with(dir: &std::path::Path, heights: impl Fn(u64) -> bool) -> HistoryProofsStore {
        let mut store = HistoryProofsStore::open(dir).unwrap();
        for row in fixture_rows().into_iter().filter(|row| heights(row.height)) {
            store.insert_block(&row, &[]).unwrap();
        }
        store
    }

    fn verified(store: &HistoryProofsStore) -> Vec<(u64, u64)> {
        history_ranges::ranges(store.connection(), RangeTable::Verified).unwrap()
    }

    fn state(store: &HistoryProofsStore, height: u64) -> Option<(i64, i64)> {
        store
            .connection()
            .query_row(
                "SELECT done, failed FROM check_state WHERE height = ?1",
                [height as i64],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok()
    }

    fn pass(store: &HistoryProofsStore) -> VerifierStats {
        verify_pass(store, &PoseidonHasher::new(), usize::MAX, true).unwrap()
    }

    /// Passes until one runs no checks.
    fn settle(store: &HistoryProofsStore) -> VerifierStats {
        loop {
            let stats = pass(store);
            if stats.worked_units == 0 {
                return stats;
            }
        }
    }

    fn window_and_l1_boundaries(height: u64) -> bool {
        (16_256..=16_384).contains(&height) || (height.is_multiple_of(128) && height <= 16_384)
    }

    #[test]
    fn verifier_verifies_window_after_boundary() {
        let dir = testdir!();
        let store = store_with(&dir, window_and_l1_boundaries);
        let stats = verify_pass(&store, &PoseidonHasher::new(), 64, true).unwrap();
        assert!(verified(&store).contains(&(16_257, 16_383)), "{:?}", verified(&store));
        assert_eq!(state(&store, 16_384).map(|(done, _)| done & layer_bit(2)), Some(layer_bit(2)));
        assert!(stats.not_checkable >= 1);
        let stats = settle(&store);
        // 16 256 lacks the window below it; 16 384 lacks the window above it.
        assert_eq!(verified(&store), vec![(16_257, 16_383)]);
        assert_eq!(stats.checkable, 0);
        // Every L1 boundary row: 16 256 and 16 384 included.
        assert_eq!(stats.not_checkable, 128);
        assert_eq!(stats.store.first_last, Some((128, 16_384)));
        assert_eq!(stats.failures.values().sum::<u64>(), 0);
    }

    #[test]
    fn first_window_after_start_is_not_checkable() {
        let dir = testdir!();
        let store = store_with(&dir, |h| (16_300..=16_384).contains(&h));
        let stats = settle(&store);
        assert!(verified(&store).is_empty());
        assert_eq!(stats.not_checkable, 85);
        assert_eq!(stats.checkable, 0);
        for height in [16_300, 16_383] {
            let (done, failed) = state(&store, height).unwrap();
            assert_eq!(done & EVENTS_ROOT, EVENTS_ROOT);
            assert_eq!(failed, 0);
        }
    }

    #[test]
    fn row_local_failure_reported_without_window() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        let mut row = fixture_rows().into_iter().find(|r| r.height == 16_384).unwrap();
        row.block_merkle_leaves = Some((0..512).map(|i| (i % 251) as u8).collect());
        store.insert_block(&row, &[]).unwrap();
        let stats = settle(&store);
        let (_, failed) = state(&store, 16_384).unwrap();
        assert_ne!(failed & MERKLE_LEAVES, 0);
        assert_eq!(stats.failures["merkle_leaves"], 1);
        assert!(verified(&store).is_empty());
    }

    #[test]
    fn substituted_row_fails_l1_and_names_the_window() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        for mut row in fixture_rows().into_iter().filter(|r| window_and_l1_boundaries(r.height)) {
            if row.height == 16_300 {
                row.envelope_hash = [0xee; 32];
            }
            store.insert_block(&row, &[]).unwrap();
        }
        let stats = settle(&store);
        assert_ne!(state(&store, 16_300).unwrap().1 & LEAF_IN_L1, 0);
        assert_ne!(state(&store, 16_384).unwrap().1 & layer_bit(1), 0);
        assert_eq!(stats.failures["l1"], 129);
        assert!(verified(&store).is_empty());
    }

    #[test]
    fn l3_root_not_checkable_until_history_below() {
        let dir = testdir!();
        let mut store =
            store_with(&dir, |h| h.is_multiple_of(16_384) && (h >= 1_015_808 || h == 2_097_152));
        let stats = settle(&store);
        let (done, failed) = state(&store, 2_097_152).unwrap();
        assert_eq!(done & layer_bit(3), 0);
        assert_eq!(failed, 0);
        assert!(stats.not_checkable >= 1);

        for row in fixture_rows()
            .into_iter()
            .filter(|r| r.height.is_multiple_of(16_384) && r.height < 1_015_808)
        {
            store.insert_block(&row, &[]).unwrap();
        }
        settle(&store);
        let (done, failed) = state(&store, 2_097_152).unwrap();
        assert_eq!(done & layer_bit(3), layer_bit(3));
        assert_eq!(failed, 0);
    }

    /// Rows 16 384 ..= 2 097 152 by 16 384, with row 16 384 unreadable. It is
    /// in a verified range, so no unit of its own reads it; L3 at 2 097 152
    /// reads it outside its window.
    fn store_with_unreadable_row(dir: &std::path::Path) -> HistoryProofsStore {
        let mut store = store_with(dir, |h| h.is_multiple_of(16_384) && h < 2_097_152);
        store
            .connection()
            .execute_batch(
                "PRAGMA ignore_check_constraints = ON;
                 UPDATE blocks SET block_id = x'00' WHERE height = 16384;
                 PRAGMA ignore_check_constraints = OFF;
                 INSERT INTO verified (start, end) VALUES (16384, 16384);",
            )
            .unwrap();
        let top = fixture_rows().into_iter().find(|r| r.height == 2_097_152).unwrap();
        store.insert_block(&top, &[]).unwrap();
        store
    }

    /// A row read outside the unit's window fails the pass when the read
    /// fails: taken as a missing row, its check would wait for good.
    #[test]
    fn failed_row_read_fails_the_pass() {
        let store = store_with_unreadable_row(&testdir!());
        let err = verify_pass(&store, &PoseidonHasher::new(), usize::MAX, false).unwrap_err();
        assert!(format!("{err:#}").contains("row 16384"), "{err:#}");
    }

    #[test]
    fn height_ranges_join_consecutive_heights() {
        assert_eq!(height_ranges([1, 2, 3, 5, 7, 8]), vec![(1, 3), (5, 5), (7, 8)]);
        assert_eq!(height_ranges([]), Vec::<(u64, u64)>::new());
    }

    /// The log line names the file and the SQLite error, not only the row.
    #[test]
    fn failed_pass_log_has_the_cause() {
        let store = store_with_unreadable_row(&testdir!());
        let PassOutcome::Failed { error, .. } =
            run_pass(&store, &PoseidonHasher::new(), MAX_WINDOWS_PER_PASS, false)
        else {
            panic!("the pass must fail");
        };
        let message = pass_failure_message(&error);
        assert!(message.contains("reading row 16384"), "{message}");
        assert!(message.contains(database::sqlite::history_proofs::HISTORY_PROOFS_DB), "{message}");
        assert!(message.contains("Conversion error"), "{message}");
    }

    #[test]
    fn gap_edge_windows_verified_after_fill() {
        let dir = testdir!();
        let mut store = store_with(&dir, |h| window_and_l1_boundaries(h) && h != 16_300);
        let stats = settle(&store);
        assert!(!verified(&store).iter().any(|(a, b)| *a <= 16_299 && 16_299 <= *b));
        let missing_before = stats.store.missing_total;
        assert_eq!(stats.checkable, 0);

        let row = fixture_rows().into_iter().find(|r| r.height == 16_300).unwrap();
        store.insert_block(&row, &[]).unwrap();
        let stats = settle(&store);
        assert_eq!(verified(&store), vec![(16_257, 16_383)]);
        assert_eq!(stats.checkable, 0);
        assert_eq!(missing_before - stats.store.missing_total, 1);
    }

    #[test]
    fn verify_pass_reports_missing_attestations() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        // Key block 1024 is older than the last 256 heights and has no attestation.
        let items: Vec<ImportItem> = fixture_rows()
            .into_iter()
            .filter(|row| (1000..=1300).contains(&row.height))
            .map(|row| (row, vec![]).into())
            .collect();
        store.import_batch(&items, ImportMode::All).unwrap();
        let stats = pass(&store);
        assert_eq!(stats.store.missing_attestations, 1);
        assert_eq!(stats.inconsistent_bk_set_rows, Some((0, 0)));
    }

    #[test]
    fn event_added_after_the_checks_keeps_the_row_unverified() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        let row = fixture_rows().into_iter().find(|r| r.height == 16_300).unwrap();
        let checked = bridge_event(16_300, row.block_id, 1);
        store.insert_block(&row, std::slice::from_ref(&checked)).unwrap();
        // The checks ran on the row with one event and passed all of them.
        let marks = BTreeMap::from([(16_300, (applicable_checks(&row, true), 0))]);
        let rows = BTreeMap::from([(16_300, row.clone())]);
        let checked_events = HashMap::from([(16_300, vec![checked.clone()])]);
        // Meanwhile the live writer adds another event to the row.
        store.insert_block(&row, &[checked, bridge_event(16_300, row.block_id, 2)]).unwrap();

        write_marks(&store, &marks, &rows, &checked_events, &mut BTreeMap::new()).unwrap();
        assert!(verified(&store).is_empty(), "verified with an unchecked event");
        let (done, _) = state(&store, 16_300).unwrap_or((0, 0));
        assert_eq!(done & EVENT_IN_TREE, 0);
    }

    #[test]
    fn bk_set_tables_checked_only_when_asked() {
        let dir = testdir!();
        let store = store_with(&dir, |h| (16_256..=16_384).contains(&h));
        let hasher = PoseidonHasher::new();
        let skipped = verify_pass(&store, &hasher, usize::MAX, false).unwrap();
        assert_eq!(skipped.inconsistent_bk_set_rows, None);
        let checked = verify_pass(&store, &hasher, usize::MAX, true).unwrap();
        assert_eq!(checked.inconsistent_bk_set_rows, Some((0, 0)));
    }

    #[test]
    fn failed_pass_still_reports_store_gauges() {
        let dir = testdir!();
        let store =
            store_with(&dir, |h| (16_256..=16_384).contains(&h) && h != 16_300 && h != 16_301);
        // The checks cannot read the events: every pass fails the same way.
        store.connection().execute_batch("ALTER TABLE events RENAME TO broken").unwrap();
        let outcome = run_pass(&store, &PoseidonHasher::new(), MAX_WINDOWS_PER_PASS, true);
        let PassOutcome::Failed { store: Some(gauges), .. } = outcome else {
            panic!("the pass must fail and still read the store gauges: {outcome:?}");
        };
        assert_eq!(gauges.missing_total, 2);
        assert_eq!(gauges.first_last, Some((16_256, 16_384)));
    }

    #[test]
    fn missing_blocks_by_scope() {
        let dir = testdir!();
        let store =
            store_with(&dir, |h| (16_256..=16_384).contains(&h) && h != 16_300 && h != 16_301);
        let stats = pass(&store);
        assert_eq!(stats.store.missing_total, 2);
        assert_eq!(stats.store.missing_dex_window, 2);
        assert_eq!(stats.store.first_last, Some((16_256, 16_384)));
    }

    #[test]
    fn verifier_runs_next_to_live_writer() {
        let dir = testdir!();
        let store = store_with(&dir, window_and_l1_boundaries);
        let done = Arc::new(AtomicBool::new(false));
        let writer_dir = dir.clone();
        let writer_done = done.clone();
        let template = fixture_rows()[0].clone();
        let writer = std::thread::Builder::new()
            .name("writer".to_string())
            .spawn(move || -> anyhow::Result<()> {
                let mut writer = HistoryProofsStore::open_with_busy_timeout(
                    &writer_dir,
                    Duration::from_secs(1),
                )?;
                for height in 100_001..=105_000u64 {
                    let mut row = template.clone();
                    row.height = height;
                    row.block_id = [0; 32];
                    row.block_id[..8].copy_from_slice(&height.to_be_bytes());
                    row.history_proofs = None;
                    writer.insert_block(&row, &[])?;
                    // Paced like a live writer: one that never pauses starves others.
                    std::thread::sleep(Duration::from_millis(1));
                }
                writer_done.store(true, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        let hasher = PoseidonHasher::new();
        while !done.load(Ordering::SeqCst) && !writer.is_finished() {
            verify_pass(&store, &hasher, MAX_WINDOWS_PER_PASS, true).unwrap();
        }
        writer.join().unwrap().expect("the writer must not hit its busy timeout");
    }

    #[test]
    fn bridge_event_outside_tree_fails_its_row() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        let row: HistoryRow = fixture_rows().into_iter().find(|r| r.height == 16_300).unwrap();
        // A genuine event that the row's tracked tree does not hold.
        let event = bridge_event(16_300, row.block_id, 1);
        store.insert_block(&row, &[event]).unwrap();
        let stats = settle(&store);
        assert_ne!(state(&store, 16_300).unwrap().1 & EVENT_IN_TREE, 0);
        assert_eq!(stats.failures["event_in_tree"], 1);
        // Not in the tree is an alert only: not a failed check of a bad row.
        assert_eq!(failed_check_ranges(store.connection(), 0, u64::MAX).unwrap(), vec![]);
    }

    /// An unprovable event is an alert only (spec 5.7): its row is verified,
    /// so no pass reads its window again, and the alert stays in check_state
    /// for bm_history_check_failures.
    #[test]
    fn row_with_an_unprovable_event_is_verified_and_keeps_the_alert() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        for row in fixture_rows().into_iter().filter(|r| window_and_l1_boundaries(r.height)) {
            // A genuine event that the row's tracked tree does not hold.
            let events = if row.height == 16_300 {
                vec![bridge_event(16_300, row.block_id, 1)]
            } else {
                vec![]
            };
            store.insert_block(&row, &events).unwrap();
        }
        let stats = settle(&store);
        assert_eq!(verified(&store), vec![(16_257, 16_383)]);
        assert_eq!(state(&store, 16_300).map(|(_, failed)| failed), Some(EVENT_IN_TREE));
        assert_eq!(stats.failures["event_in_tree"], 1);
        assert_eq!(stats.not_checkable, 128);
        // Without a scan every unverified row counts as checkable: the
        // verified alert row is not among them.
        let unscanned = verify_pass(&store, &PoseidonHasher::new(), 0, false).unwrap();
        assert_eq!(unscanned.checkable, 128);
    }

    /// An unprovable event is logged as a warning, an alert only; a failed
    /// check of a bad row as an error.
    #[test]
    fn failure_log_tells_an_alert_from_a_bad_row() {
        let failure = |bits| CheckFailure { bits, heights: vec![10], detail: "detail".into() };
        let (level, line) = failure_log(&failure(EVENT_IN_TREE));
        assert_eq!(level, tracing::Level::WARN);
        assert!(line.ends_with("(an alert only: the event cannot be proven)"), "{line}");
        let (level, line) = failure_log(&failure(MERKLE_LEAVES));
        assert_eq!(level, tracing::Level::ERROR);
        assert!(!line.contains("alert only"), "{line}");
    }

    /// A new event on a verified row with the mark of an unprovable event:
    /// the row leaves verified until its checks ran again, and keeps the mark.
    #[test]
    fn new_event_on_a_row_with_an_unprovable_event_is_checked_again() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        let rows = fixture_rows();
        for row in rows.iter().filter(|r| window_and_l1_boundaries(r.height)) {
            let events = if row.height == 16_300 {
                vec![bridge_event(16_300, row.block_id, 1)]
            } else {
                vec![]
            };
            store.insert_block(row, &events).unwrap();
        }
        settle(&store);
        assert_eq!(verified(&store), vec![(16_257, 16_383)]);

        let row = rows.iter().find(|r| r.height == 16_300).unwrap();
        let events = [bridge_event(16_300, row.block_id, 1), bridge_event(16_300, row.block_id, 2)];
        store.insert_block(row, &events).unwrap();
        assert_eq!(verified(&store), vec![(16_257, 16_299), (16_301, 16_383)]);
        assert_eq!(state(&store, 16_300), Some((0, EVENT_IN_TREE)));

        let stats = settle(&store);
        assert_eq!(verified(&store), vec![(16_257, 16_383)]);
        assert_eq!(state(&store, 16_300).map(|(_, failed)| failed), Some(EVENT_IN_TREE));
        assert_eq!(stats.failures["event_in_tree"], 1);
    }

    /// A row with an unprovable event and a tampered one: a bad row, never
    /// verified, in both lists; the rest of its window is verified.
    #[test]
    fn row_with_an_unprovable_and_a_tampered_event_is_bad() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        for row in fixture_rows().into_iter().filter(|r| window_and_l1_boundaries(r.height)) {
            let events = if row.height == 16_300 {
                vec![bridge_event(16_300, row.block_id, 1)]
            } else {
                vec![]
            };
            store.insert_block(&row, &events).unwrap();
        }
        store.connection().execute("UPDATE events SET created_at = 1", []).unwrap();
        settle(&store);
        assert_eq!(verified(&store), vec![(16_257, 16_299), (16_301, 16_383)]);
        let (_, failed) = state(&store, 16_300).unwrap();
        assert_eq!(failed, EVENT_IN_TREE | EVENT_BOC);
        let conn = store.connection();
        assert_eq!(failed_check_ranges(conn, 0, u64::MAX).unwrap(), vec![(16_300, 16_300)]);
        assert_eq!(unprovable_event_ranges(conn, 0, u64::MAX).unwrap(), vec![(16_300, 16_300)]);
    }

    /// Every failed check but event_in_tree marks a bad row (spec 5.7).
    #[test]
    fn every_failed_check_but_the_alert_is_a_bad_row() {
        let dir = testdir!();
        let store = HistoryProofsStore::open(&dir).unwrap();
        let bad: Vec<i64> = [EVENTS_ROOT, MERKLE_LEAVES, LEAF_IN_L1, EVENT_BOC]
            .into_iter()
            .chain((1..=10).map(layer_bit))
            .collect();
        let insert = |height: u64, bit: i64| {
            store
                .connection()
                .execute(
                    "INSERT INTO check_state (height, done, failed) VALUES (?1, ?2, ?2)",
                    params![height as i64, bit],
                )
                .unwrap();
        };
        for (height, bit) in (1..).zip(&bad) {
            insert(height, *bit);
        }
        insert(100, EVENT_IN_TREE);
        let expected: Vec<(u64, i64)> = (1..).zip(bad).collect();
        assert_eq!(failed_check_states(store.connection(), 0, u64::MAX).unwrap(), expected);
    }

    /// A stored event whose fields are no longer those of its BOC is a damaged
    /// row: a failed check of a bad row, unlike an event outside the tree.
    #[test]
    fn tampered_bridge_event_is_a_bad_row() {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        let row: HistoryRow = fixture_rows().into_iter().find(|r| r.height == 16_300).unwrap();
        store.insert_block(&row, &[bridge_event(16_300, row.block_id, 1)]).unwrap();
        store.connection().execute("UPDATE events SET created_at = 1", []).unwrap();
        let stats = settle(&store);
        assert_ne!(state(&store, 16_300).unwrap().1 & EVENT_BOC, 0);
        assert_eq!(stats.failures["event_boc"], 1);
        assert_eq!(
            failed_check_ranges(store.connection(), 0, u64::MAX).unwrap(),
            vec![(16_300, 16_300)]
        );
    }
}
