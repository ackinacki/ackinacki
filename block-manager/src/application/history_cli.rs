// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! `block-manager history-proofs gaps|export|import`: closing gaps of the
//! history proofs store by copying height ranges from another block-manager.
//! Every command works next to a running block-manager: reads and writes go
//! in batches of short transactions.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;

use anyhow::Context;
use database::sqlite::history_bk_set;
use database::sqlite::history_bk_set::BkSetUpdateWrite;
use database::sqlite::history_proofs::AttestationWrites;
use database::sqlite::history_proofs::EventCounts;
use database::sqlite::history_proofs::EventFilter;
use database::sqlite::history_proofs::EventFilters;
use database::sqlite::history_proofs::EventKind;
use database::sqlite::history_proofs::HistoryProofsStore;
use database::sqlite::history_proofs::ImportItem;
use database::sqlite::history_proofs::ImportMode;
use database::sqlite::history_proofs::ImportRowStatus;
use database::sqlite::history_proofs::HISTORY_PROOFS_BUSY_TIMEOUT;
use database::sqlite::history_proofs::HISTORY_PROOFS_DB;
use database::sqlite::history_ranges;
use history_proof::PoseidonHasher;
use serde_json::json;

use crate::application::services::history_verifier;
use crate::cli::HistoryProofsCommand;

/// Heights per read batch and per import transaction.
pub const BATCH_HEIGHTS: u64 = 5_000;

/// Pause after the write transactions of each import batch that has rows or
/// attestations. SQLite locks are not fair: a writer waiting for the lock
/// retries after sleeps of up to 100 ms, and the next batch would take the
/// lock again first. A longer pause lets the live block-manager writer in
/// once per batch.
const IMPORT_PAUSE: std::time::Duration = std::time::Duration::from_millis(110);

pub const EXIT_OK: i32 = 0;
/// The data is good but not all of it is there. `export`: the source misses
/// heights or attestations of the range. `import`: heights or missing
/// attestations left in the range. `gaps` never returns it.
pub const EXIT_INCOMPLETE: i32 = 1;
/// Any other error: wrong arguments, a missing file or an existing `--out`
/// file, a foreign or older schema, an export file of other event flags or
/// of an export that did not finish, an SQLite or IO error.
pub const EXIT_USAGE: i32 = 2;
/// Bad data, also when data is missing too; an error that ends the run is
/// still `EXIT_USAGE`. `export`: the export file refused some rows, or rows
/// of the range failed a check other than `event_in_tree` on the
/// source; both are left out of the file.
/// `import`: rejected rows, conflicts, inserted rows that failed a check of a
/// bad row (all but `event_in_tree`, an alert only: spec 5.7), rejected
/// or deleted attestations, and rows of the range that failed such a check
/// earlier, so that a second run does not pass.
pub const EXIT_BAD_DATA: i32 = 3;

/// Bad data wins over missing data: the playbook stops on it, and copies on
/// from another source what is only missing.
fn exit_code(bad_data: bool, incomplete: bool) -> i32 {
    if bad_data {
        EXIT_BAD_DATA
    } else if incomplete {
        EXIT_INCOMPLETE
    } else {
        EXIT_OK
    }
}

/// Runs a command against `<data_dir>/bm-history-proofs.db`, prints its
/// result to stdout and its errors and problems to stderr, and returns the
/// exit code.
pub fn run(cmd: HistoryProofsCommand, data_dir: &Path, filter: &EventFilters) -> i32 {
    run_with_output(cmd, data_dir, filter, &mut std::io::stdout())
}

pub fn run_with_output(
    cmd: HistoryProofsCommand,
    data_dir: &Path,
    filter: &EventFilters,
    out: &mut dyn Write,
) -> i32 {
    let path = data_dir.join(HISTORY_PROOFS_DB);
    let result = match cmd {
        HistoryProofsCommand::Gaps { json } => gaps(&path, json, out),
        HistoryProofsCommand::Export { from_height, to_height, out: file, source_id, json } => {
            let source_id = source_id.unwrap_or_default();
            export(&path, from_height, to_height, &file, source_id, filter, json, out)
        }
        HistoryProofsCommand::Import { from, all, from_height, to_height, json } => {
            import(&path, &from, all, from_height, to_height, json, filter, out)
        }
    };
    match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            EXIT_USAGE
        }
    }
}

fn open(path: &Path) -> anyhow::Result<HistoryProofsStore> {
    // SQLite reports a missing file and one it cannot open the same way; the
    // playbook skips a source without the store and stops on other failures.
    anyhow::ensure!(!matches!(path.try_exists(), Ok(false)), "{}: no such file", path.display());
    HistoryProofsStore::open_file(path, HISTORY_PROOFS_BUSY_TIMEOUT, false)
}

fn total(ranges: &[(u64, u64)]) -> u64 {
    ranges.iter().map(|(a, b)| b - a + 1).sum()
}

fn format_ranges(ranges: &[(u64, u64)]) -> String {
    if ranges.is_empty() {
        return "none".to_string();
    }
    ranges.iter().map(|(a, b)| format!("{a}..={b}")).collect::<Vec<_>>().join(", ")
}

/// `format_ranges` of the first `PROBLEMS_PER_KIND` ranges, then how many more.
fn format_ranges_bounded(ranges: &[(u64, u64)]) -> String {
    let shown = format_ranges(&ranges[..ranges.len().min(PROBLEMS_PER_KIND)]);
    match ranges.len().checked_sub(PROBLEMS_PER_KIND) {
        Some(more) if more > 0 => format!("{shown} ... and {more} more ranges"),
        _ => shown,
    }
}

/// The newest ranges of heights with an unprovable event that `gaps`
/// and `import` show. After the bridge routing leaves the tracked routings
/// every height with an event has one, and the list grows for good.
const UNPROVABLE_RANGES_SHOWN: usize = 100;

fn newest_unprovable(ranges: &[(u64, u64)]) -> &[(u64, u64)] {
    &ranges[ranges.len().saturating_sub(UNPROVABLE_RANGES_SHOWN)..]
}

/// `unprovable_events` of the JSON output: the number of heights, the
/// newest `UNPROVABLE_RANGES_SHOWN` ranges, and the number of all ranges.
fn unprovable_json(ranges: &[(u64, u64)]) -> serde_json::Value {
    json!({
        "heights": total(ranges),
        "ranges": newest_unprovable(ranges),
        "total_ranges": ranges.len(),
    })
}

/// The text output line of the heights with an unprovable event (an
/// alert only), with the newest ranges.
fn unprovable_line(ranges: &[(u64, u64)]) -> String {
    let shown = newest_unprovable(ranges);
    let of = if shown.len() < ranges.len() {
        format!(", the newest {} of {} ranges", shown.len(), ranges.len())
    } else {
        String::new()
    };
    format!(
        "unprovable events, an alert only ({} heights{of}): {}",
        total(ranges),
        format_ranges(shown)
    )
}

fn format_heights(heights: &[u64]) -> String {
    if heights.is_empty() {
        return "none".to_string();
    }
    heights.iter().map(u64::to_string).collect::<Vec<_>>().join(", ")
}

fn gaps(path: &Path, json: bool, out: &mut dyn Write) -> anyhow::Result<i32> {
    let store = open(path)?;
    let first_last = history_ranges::first_last(store.connection())?;
    let gaps = history_ranges::gaps(store.connection())?;
    let missing_attestations = match first_last {
        Some((first, last)) => {
            history_bk_set::missing_attestations(store.connection(), first, last)?
        }
        None => vec![],
    };
    let failed_checks = history_verifier::failed_check_ranges(store.connection(), 0, u64::MAX)?;
    let unprovable = history_verifier::unprovable_event_ranges(store.connection(), 0, u64::MAX)?;
    let unverified = history_ranges::unverified(store.connection())?;
    if json {
        writeln!(
            out,
            "{}",
            json!({
                "first_height": first_last.map(|(first, _)| first),
                "last_height": first_last.map(|(_, last)| last),
                "gaps": gaps,
                "missing_attestations": missing_attestations,
                "failed_checks": failed_checks,
                "unverified": unverified,
                "unprovable_events": unprovable_json(&unprovable),
            })
        )?;
    } else {
        match first_last {
            Some((first, last)) => writeln!(out, "first height {first}, last height {last}")?,
            None => writeln!(out, "the store is empty")?,
        }
        writeln!(out, "gaps ({} heights): {}", total(&gaps), format_ranges(&gaps))?;
        writeln!(
            out,
            "missing attestations ({}): {}",
            missing_attestations.len(),
            format_heights(&missing_attestations)
        )?;
        writeln!(
            out,
            "rows that failed their checks ({} heights): {}",
            total(&failed_checks),
            format_ranges(&failed_checks)
        )?;
        writeln!(
            out,
            "rows not verified yet ({} heights): {}",
            total(&unverified),
            format_ranges(&unverified)
        )?;
        writeln!(out, "{}", unprovable_line(&unprovable))?;
    }
    Ok(EXIT_OK)
}

/// Height batches of at most `BATCH_HEIGHTS` covering `from ..= to`.
fn batches(from: u64, to: u64) -> impl Iterator<Item = (u64, u64)> {
    let mut next = Some(from);
    std::iter::from_fn(move || {
        let start = next?;
        let end = start.saturating_add(BATCH_HEIGHTS - 1).min(to);
        next = end.checked_add(1).filter(|n| *n <= to);
        Some((start, end))
    })
}

#[allow(clippy::too_many_arguments)]
fn export(
    path: &Path,
    from: u64,
    to: u64,
    file: &Path,
    source_id: String,
    filter: &EventFilters,
    json: bool,
    out: &mut dyn Write,
) -> anyhow::Result<i32> {
    anyhow::ensure!(from <= to, "--from-height {from} is above --to-height {to}");
    let source = open(path)?;
    anyhow::ensure!(!file.exists(), "{} already exists", file.display());
    let mut target = HistoryProofsStore::open_file(file, HISTORY_PROOFS_BUSY_TIMEOUT, true)?;
    target
        .connection()
        .execute_batch("CREATE TABLE export_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")?;

    let (mut rows, mut events, mut rejected) = (0u64, 0u64, 0u64);
    let mut events_by_kind = EventCounts::default();
    let mut failed_heights = Vec::new();
    let mut problems = Problems::default();
    for (start, end) in batches(from, to) {
        let (mut items, attestations) = source.read_export_batch(start, end)?;
        // The rows this BM's own checks refuse stay out of the file: the
        // source answers for its rows (spec 5.7), and known-bad rows must not
        // spread. Their heights are gaps of the file.
        let failed: BTreeSet<u64> =
            history_verifier::failed_check_states(source.connection(), start, end)?
                .into_iter()
                .map(|(height, _)| height)
                .collect();
        items.retain(|item| !failed.contains(&item.row.height));
        failed_heights.extend(failed);
        let imported = target.import_batch(&items, ImportMode::All)?;
        let printed = problems.lines.len();
        for (item, imported) in items.iter().zip(imported) {
            match imported.status {
                ImportRowStatus::Inserted | ImportRowStatus::Duplicate => {
                    rows += 1;
                    events += item.events.len() as u64;
                    for event in &item.events {
                        events_by_kind.add(event.kind, 1);
                    }
                }
                other => {
                    rejected += 1;
                    let line =
                        format!("row at height {} is not exported: {other:?}", item.row.height);
                    problems.push("rows not exported", line);
                }
            }
        }
        for problem in &problems.lines[printed..] {
            eprintln!("{problem}");
        }
        target.copy_attestations(&attestations)?;
    }
    let failed_checks = history_verifier::height_ranges(failed_heights);
    let printed = problems.lines.len();
    if !failed_checks.is_empty() {
        let line = format!(
            "rows not exported, they failed their checks on this block-manager ({} heights): {}",
            total(&failed_checks),
            format_ranges_bounded(&failed_checks)
        );
        problems.push("rows that failed their checks", line);
    }
    problems.finish();
    for problem in &problems.lines[printed..] {
        eprintln!("{problem}");
    }
    let count = |table: &str| -> anyhow::Result<u64> {
        let rows: i64 =
            target
                .connection()
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))?;
        Ok(rows as u64)
    };
    let (bk_set_updates, attestations) = (count("bk_set_updates")?, count("attestations")?);

    let source_first_last = history_ranges::first_last(source.connection())?;
    let source_gaps = history_ranges::missing_in(source.connection(), from, to)?;
    let source_missing_attestations =
        history_bk_set::missing_attestations(source.connection(), from, to)?;
    let exported_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
    let optional = |value: Option<u64>| value.map(|v| v.to_string()).unwrap_or_default();
    let meta = [
        ("requested_from", from.to_string()),
        ("requested_to", to.to_string()),
        ("source_first", optional(source_first_last.map(|(first, _)| first))),
        ("source_last", optional(source_first_last.map(|(_, last)| last))),
        ("source_gaps", serde_json::to_string(&source_gaps)?),
        ("source_missing_attestations", serde_json::to_string(&source_missing_attestations)?),
        ("exported_at", exported_at.to_string()),
        ("source_id", source_id),
        ("bridge_event_routing", routing_text(&filter.withdrawal)),
        ("bridge_event_dst", filter.withdrawal.dst()),
        ("voucher_event_routing", routing_text(&filter.voucher)),
        ("voucher_event_dst", filter.voucher.dst()),
    ];
    // One transaction: import refuses a file without some key.
    let tx = target.connection().unchecked_transaction()?;
    for (key, value) in meta {
        tx.execute("INSERT INTO export_meta (key, value) VALUES (?1, ?2)", [key, &value])?;
    }
    tx.commit()?;
    // The playbook copies the file without its -wal: the rows must be in the
    // main file when export returns, whatever happens when it closes.
    let busy: i64 =
        target.connection().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    anyhow::ensure!(busy == 0, "{}: the WAL checkpoint did not complete", file.display());

    if json {
        writeln!(
            out,
            "{}",
            json!({
                "requested": [from, to],
                "source_first": source_first_last.map(|(first, _)| first),
                "source_last": source_first_last.map(|(_, last)| last),
                "source_gaps": source_gaps,
                "source_missing_attestations": source_missing_attestations,
                "rows": rows,
                "events": events,
                "events_by_kind": events_json(&events_by_kind),
                "rejected": rejected,
                "bk_set_updates": bk_set_updates,
                "attestations": attestations,
                "failed_checks": failed_checks,
            })
        )?;
    } else {
        writeln!(
            out,
            "exported {rows} rows, {events} events ({} withdrawals, {} vouchers), {bk_set_updates} BK set updates and {attestations} attestations of {from}..={to} to {}",
            events_by_kind.withdrawal,
            events_by_kind.voucher,
            file.display()
        )?;
        writeln!(
            out,
            "source gaps in the range ({} heights): {}",
            total(&source_gaps),
            format_ranges(&source_gaps)
        )?;
        writeln!(
            out,
            "source missing attestations in the range ({}): {}",
            source_missing_attestations.len(),
            format_heights(&source_missing_attestations)
        )?;
        if rejected > 0 {
            writeln!(out, "rows not exported (the file refused them): {rejected}")?;
        }
        if !failed_checks.is_empty() {
            writeln!(
                out,
                "rows not exported, they failed their checks here ({} heights): {}",
                total(&failed_checks),
                format_ranges_bounded(&failed_checks)
            )?;
        }
    }
    let incomplete = !source_gaps.is_empty() || !source_missing_attestations.is_empty();
    Ok(exit_code(rejected > 0 || !failed_checks.is_empty(), incomplete))
}

/// `{"withdrawal": n, "voucher": n}`.
fn events_json(counts: &EventCounts) -> serde_json::Value {
    EventKind::ALL
        .into_iter()
        .map(|kind| (kind.as_str().to_string(), serde_json::Value::from(counts.get(kind))))
        .collect::<serde_json::Map<_, _>>()
        .into()
}

/// The keys `export` writes into `export_meta`.
const EXPORT_META_KEYS: [&str; 12] = [
    "requested_from",
    "requested_to",
    "source_first",
    "source_last",
    "source_gaps",
    "source_missing_attestations",
    "exported_at",
    "source_id",
    "bridge_event_routing",
    "bridge_event_dst",
    "voucher_event_routing",
    "voucher_event_dst",
];

/// What `import` takes from the metadata of an export file.
struct ExportMeta {
    range: (u64, u64),
    /// The event flags of the source: (flag, value) of `--bridge-event-routing`,
    /// `--bridge-event-dst`, `--voucher-event-routing`, `--voucher-event-dst`.
    flags: Vec<(&'static str, String)>,
}

/// The export metadata key of each event flag.
const EVENT_FLAG_KEYS: [(&str, &str); 4] = [
    ("bridge_event_routing", "--bridge-event-routing"),
    ("bridge_event_dst", "--bridge-event-dst"),
    ("voucher_event_routing", "--voucher-event-routing"),
    ("voucher_event_dst", "--voucher-event-dst"),
];

/// The values of the event flags, in the order of `EVENT_FLAG_KEYS`.
fn event_flag_values(filters: &EventFilters) -> [String; 4] {
    [
        routing_text(&filters.withdrawal),
        filters.withdrawal.dst(),
        routing_text(&filters.voucher),
        filters.voucher.dst(),
    ]
}

impl ExportMeta {
    /// `None` for a file without export metadata (a `VACUUM INTO` backup).
    /// `export` writes the metadata in one transaction at its end, so a file
    /// without one of `EXPORT_META_KEYS` is an export that did not finish: an
    /// error. Every error names the file.
    fn read(store: &HistoryProofsStore, file: &Path) -> anyhow::Result<Option<Self>> {
        Self::read_values(store).with_context(|| file.display().to_string())
    }

    fn read_values(store: &HistoryProofsStore) -> anyhow::Result<Option<Self>> {
        let conn = store.connection();
        let has_meta: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = 'export_meta'",
            [],
            |row| row.get(0),
        )?;
        if !has_meta {
            return Ok(None);
        }
        let mut values: BTreeMap<String, String> = conn
            .prepare("SELECT key, value FROM export_meta")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        if let Some(key) = EXPORT_META_KEYS.iter().find(|key| !values.contains_key(**key)) {
            anyhow::bail!("the export metadata has no {key}: the export did not finish");
        }
        let mut take = |key: &str| values.remove(key).unwrap_or_default();
        let height = |key: &str, value: String| -> anyhow::Result<u64> {
            value.parse().with_context(|| format!("export metadata {key}"))
        };
        let range = (
            height("requested_from", take("requested_from"))?,
            height("requested_to", take("requested_to"))?,
        );
        let flags = EVENT_FLAG_KEYS.iter().map(|(key, flag)| (*flag, take(key))).collect();
        Ok(Some(Self { range, flags }))
    }
}

/// A routing flag as it takes its value.
fn routing_text(filter: &EventFilter) -> String {
    format!("{}::{}", hex::encode(filter.routing_dapp_id), hex::encode(filter.routing_account_id))
}

/// An event of `item` whose `src` and `dst` the filters give another kind. An
/// event no filter selects is of older flags and keeps its kind.
fn wrong_event_kind(item: &ImportItem, filters: &EventFilters) -> Option<String> {
    item.events.iter().find_map(|event| match filters.kind_of(&event.src, &event.dst) {
        Some((kind, _)) if kind != event.kind => Some(format!(
            "event {} is stored as {}, but its src {} and dst {} are of {} events",
            hex::encode(event.msg_hash),
            event.kind.as_str(),
            event.src,
            event.dst,
            kind.as_str()
        )),
        _ => None,
    })
}

#[allow(clippy::too_many_arguments)]
fn import(
    path: &Path,
    file: &Path,
    all: bool,
    from_height: Option<u64>,
    to_height: Option<u64>,
    json: bool,
    filter: &EventFilters,
    out: &mut dyn Write,
) -> anyhow::Result<i32> {
    let source = open(file)?;
    let meta = ExportMeta::read(&source, file)?;
    // The events are kept as the source stored them (rows from before a change
    // of the flags have other ones), so the whole file is refused instead.
    if let Some(ExportMeta { flags, .. }) = &meta {
        let own = event_flag_values(filter);
        let differ: Vec<String> = flags
            .iter()
            .zip(&own)
            .filter(|((_, theirs), ours)| theirs != *ours)
            .map(|((flag, theirs), ours)| format!("{flag} {theirs} (this block-manager: {ours})"))
            .collect();
        anyhow::ensure!(
            differ.is_empty(),
            "{} was exported with other event flags: {}; export from a block-manager with the same flags",
            file.display(),
            differ.join(", ")
        );
    }
    let mut target = open(path)?;
    let (from, to) = match (from_height, to_height) {
        (Some(from), Some(to)) => (from, to),
        (None, None) => match &meta {
            Some(meta) => meta.range,
            None => anyhow::bail!(
                "{} has no export metadata: pass --from-height and --to-height",
                file.display()
            ),
        },
        _ => anyhow::bail!("pass both --from-height and --to-height, or neither"),
    };
    anyhow::ensure!(from <= to, "--from-height {from} is above --to-height {to}");
    let mode = if all { ImportMode::All } else { ImportMode::GapsOnly };
    let hasher = PoseidonHasher::new();

    let mut stats = ImportStats::default();
    // Printed after each batch: an error in a later batch must not hide the
    // bad rows an earlier one wrote.
    let mut printed = 0;
    for (start, end) in batches(from, to) {
        let (items, attestations) = source.read_export_batch(start, end)?;
        // An empty batch takes no lock and needs no pause.
        if items.is_empty() && attestations.is_empty() {
            continue;
        }
        stats.read += items.len() as u64;
        // The flags of the file are this block-manager's: an event they give
        // another kind is damaged, and its row is refused whole.
        let mut kept = Vec::with_capacity(items.len());
        for item in items {
            match wrong_event_kind(&item, filter) {
                Some(reason) => {
                    stats.rejected += 1 + item.events.len() as u64;
                    let line =
                        format!("rejected row at height {} (event): {reason}", item.row.height);
                    stats.problems.push("rows with a rejected event", line);
                }
                None => kept.push(item),
            }
        }
        let items = kept;
        let imported =
            if items.is_empty() { Vec::new() } else { target.import_batch(&items, mode)? };
        let mut inserted = Vec::new();
        for (item, imported) in items.iter().zip(imported) {
            let (row, events) = (&item.row, &item.events);
            match imported.bk_set_update {
                Some(BkSetUpdateWrite::Inserted) => stats.bk_set_updates.inserted += 1,
                Some(BkSetUpdateWrite::Present) => stats.bk_set_updates.present += 1,
                None => {}
            }
            for reason in imported.detached {
                stats
                    .problems
                    .push("deleted attestations", format!("deleted attestation {reason}"));
                stats.attestations.detached.push(reason);
            }
            match imported.status {
                ImportRowStatus::Inserted => {
                    stats.inserted += 1;
                    inserted.push((row, events.as_slice()));
                }
                ImportRowStatus::Duplicate => stats.present += 1,
                ImportRowStatus::OutsideGaps => stats.outside_gaps += 1 + events.len() as u64,
                ImportRowStatus::Conflict(reason) => {
                    stats.conflicts += 1;
                    stats.rejected += events.len() as u64;
                    if item.bk_set_update.is_some() {
                        stats.bk_set_updates.conflicts += 1;
                    }
                    let line = format!("conflict at height {}: {reason}", row.height);
                    stats.problems.push("conflicts", line);
                }
                ImportRowStatus::Rejected(err) => {
                    stats.rejected += 1 + events.len() as u64;
                    let line = format!("rejected row at height {} (format): {err}", row.height);
                    stats.problems.push("rows with a bad format", line);
                }
                ImportRowStatus::RejectedEvent(reason) => {
                    stats.rejected += 1 + events.len() as u64;
                    let line = format!("rejected row at height {} (event): {reason}", row.height);
                    stats.problems.push("rows with a rejected event", line);
                }
                ImportRowStatus::RejectedBkSetUpdate(reason) => {
                    stats.rejected += 1 + events.len() as u64;
                    if item.bk_set_update.is_some() {
                        stats.bk_set_updates.rejected += 1;
                    }
                    let line =
                        format!("rejected row at height {} (BK set update): {reason}", row.height);
                    stats.problems.push("rows with a rejected BK set update", line);
                }
            }
        }
        let failures = history_verifier::record_row_checks(&target, &hasher, &inserted)?;
        let mut failed_rows = BTreeSet::new();
        for failure in &failures {
            let line = history_verifier::describe_failure(failure);
            if failure.is_alert_only() {
                // An event not in its routing's tracked group: not provable,
                // but the row is not bad (spec 5.7).
                let line = format!("{line} {}", history_verifier::ALERT_ONLY);
                stats.problems.push("unprovable events", line);
            } else {
                failed_rows.extend(failure.heights.iter().copied());
                stats.problems.push("failed row checks", line);
            }
        }
        stats.row_check_failures += failed_rows.len() as u64;
        // After the rows of the batch: an attestation is kept only when the
        // row of the block it signs is in the target.
        if !attestations.is_empty() {
            let known = stats.attestations.mismatches.len();
            for write in target.import_attestations(&attestations)? {
                stats.attestations.add(write);
            }
            for (_, reason) in &stats.attestations.mismatches[known..] {
                let line = format!("rejected attestation: {reason}");
                stats.problems.push("rejected attestations", line);
            }
        }
        for problem in &stats.problems.lines[printed..] {
            eprintln!("{problem}");
        }
        printed = stats.problems.lines.len();
        std::thread::sleep(IMPORT_PAUSE);
    }
    let remaining_gaps = history_ranges::missing_in(target.connection(), from, to)?;
    let remaining_missing_attestations =
        history_bk_set::missing_attestations(target.connection(), from, to)?;
    // Also the rows an earlier import wrote: they stay bad until the store is
    // restored, and a second run must not pass.
    let failed_checks = history_verifier::failed_check_ranges(target.connection(), from, to)?;
    let unprovable = history_verifier::unprovable_event_ranges(target.connection(), from, to)?;
    if !failed_checks.is_empty() {
        let line = format!(
            "rows of the range that failed their checks ({} heights): {}",
            total(&failed_checks),
            format_ranges_bounded(&failed_checks)
        );
        stats.problems.push("rows that failed their checks", line);
    }
    stats.problems.finish();
    for problem in &stats.problems.lines[printed..] {
        eprintln!("{problem}");
    }
    if json {
        writeln!(
            out,
            "{}",
            json!({
                "requested": [from, to],
                "read": stats.read,
                "inserted": stats.inserted,
                "present": stats.present,
                "outside_gaps": stats.outside_gaps,
                "rejected": stats.rejected,
                "conflicts": stats.conflicts,
                "row_check_failures": stats.row_check_failures,
                "remaining_gaps": remaining_gaps,
                "bk_set_updates": {
                    "inserted": stats.bk_set_updates.inserted,
                    "present": stats.bk_set_updates.present,
                    "rejected": stats.bk_set_updates.rejected,
                    "conflicts": stats.bk_set_updates.conflicts,
                },
                "attestations": {
                    "inserted": stats.attestations.inserted,
                    "replaced": stats.attestations.replaced,
                    "kept": stats.attestations.kept,
                    "not_kept": stats.attestations.not_kept,
                    "rejected": stats.attestations.mismatches.len(),
                    "detached": stats.attestations.detached.len(),
                },
                "remaining_missing_attestations": remaining_missing_attestations,
                "failed_checks": failed_checks,
                "unprovable_events": unprovable_json(&unprovable),
            })
        )?;
    } else {
        for problem in &stats.problems.lines {
            writeln!(out, "{problem}")?;
        }
        writeln!(
            out,
            "range {from}..={to}: read {}, inserted {}, already present {}, outside gaps {}, rejected {}, conflicts {}, row check failures {}",
            stats.read,
            stats.inserted,
            stats.present,
            stats.outside_gaps,
            stats.rejected,
            stats.conflicts,
            stats.row_check_failures
        )?;
        let bk = &stats.bk_set_updates;
        writeln!(
            out,
            "BK set updates: inserted {}, already present {}, rejected {}, conflicts {}",
            bk.inserted, bk.present, bk.rejected, bk.conflicts
        )?;
        let att = &stats.attestations;
        writeln!(
            out,
            "attestations: inserted {}, replaced {}, kept {}, not kept {}, rejected {}, deleted {}",
            att.inserted,
            att.replaced,
            att.kept,
            att.not_kept,
            att.mismatches.len(),
            att.detached.len()
        )?;
        writeln!(
            out,
            "remaining gaps in the range ({} heights): {}",
            total(&remaining_gaps),
            format_ranges(&remaining_gaps)
        )?;
        writeln!(
            out,
            "remaining missing attestations in the range ({}): {}",
            remaining_missing_attestations.len(),
            format_heights(&remaining_missing_attestations)
        )?;
        writeln!(out, "{}", unprovable_line(&unprovable))?;
    }
    let bad_data = stats.rejected > 0
        || stats.conflicts > 0
        || stats.row_check_failures > 0
        || !failed_checks.is_empty()
        || !stats.attestations.mismatches.is_empty()
        || !stats.attestations.detached.is_empty();
    let incomplete = !remaining_gaps.is_empty() || !remaining_missing_attestations.is_empty();
    Ok(exit_code(bad_data, incomplete))
}

#[derive(Default)]
struct ImportStats {
    read: u64,
    inserted: u64,
    present: u64,
    /// Skipped rows and their events.
    outside_gaps: u64,
    /// Rejected rows, and the events of rejected and conflicting rows.
    rejected: u64,
    conflicts: u64,
    row_check_failures: u64,
    bk_set_updates: BkSetUpdateStats,
    attestations: AttestationWrites,
    problems: Problems,
}

/// Lines printed of each kind of import problem; the rest are counted. A
/// chunk of two million heights can have that many problems.
const PROBLEMS_PER_KIND: usize = 20;

/// The problems of an import: at most `PROBLEMS_PER_KIND` lines of each kind.
#[derive(Default)]
struct Problems {
    lines: Vec<String>,
    counts: BTreeMap<&'static str, usize>,
}

impl Problems {
    /// `kind` names the problem in the closing `... and N more <kind>` line.
    fn push(&mut self, kind: &'static str, line: String) {
        let count = self.counts.entry(kind).or_default();
        *count += 1;
        if *count <= PROBLEMS_PER_KIND {
            self.lines.push(line);
        }
    }

    /// Adds `... and N more <kind>` for each kind past the limit.
    fn finish(&mut self) {
        for (kind, count) in &self.counts {
            if *count > PROBLEMS_PER_KIND {
                self.lines.push(format!("... and {} more {kind}", count - PROBLEMS_PER_KIND));
            }
        }
    }
}

#[derive(Default)]
struct BkSetUpdateStats {
    inserted: u64,
    present: u64,
    /// BK set updates of rows rejected for their update.
    rejected: u64,
    /// BK set updates of conflicting rows, or conflicting with a stored one.
    conflicts: u64,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use database::sqlite::history_bk_set::AttestationWrite;
    use database::sqlite::history_bk_set::KEY_BLOCK_STRIDE;
    use database::sqlite::history_proofs::EventRow;
    use database::sqlite::history_proofs::HistoryRow;
    use database::sqlite::history_proofs::THREAD_ZERO_HEX;
    use database::sqlite::history_ranges::RangeTable;
    use database::sqlite::ArchAttestation;
    use database::sqlite::ArchBkSetUpdate;
    use proof_data::layers::required_layers;
    use proof_data::merkle::BLOCK_MERKLE_HISTORY_PROOFS_LEAF_INDEX;
    use proof_data::merkle::BLOCK_MERKLE_TRACKED_EXT_OUT_MESSAGES_ROOT_LEAF_INDEX;
    use serde_json::Value;
    use testdir::testdir;

    use super::*;
    use crate::domain::history_checks::tests::bridge_event;
    use crate::domain::history_checks::tests::voucher_event;
    use crate::domain::history_checks::EVENT_IN_TREE;
    use crate::domain::history_checks::MERKLE_LEAVES;

    static HASHER: std::sync::LazyLock<PoseidonHasher> =
        std::sync::LazyLock::new(PoseidonHasher::new);

    /// `history_proof::history_proofs_l0`, cached by the number of layers (the
    /// roots of `row` depend on nothing else): it builds a hasher per call,
    /// which takes about 20 ms in a debug build.
    fn history_proofs_l0(roots: &[(u8, [u8; 32])]) -> [u8; 32] {
        static L0: std::sync::LazyLock<std::sync::Mutex<Vec<Option<[u8; 32]>>>> =
            std::sync::LazyLock::new(Default::default);
        let mut cache = L0.lock().unwrap();
        if cache.len() <= roots.len() {
            cache.resize(roots.len() + 1, None);
        }
        *cache[roots.len()].get_or_insert_with(|| {
            history_proof::history_proofs_l0(roots.iter().map(|(n, root)| (*n, root)))
        })
    }

    /// A row that passes its checks: a height divisible by 10 tracks the
    /// bridge event `event(height)`, and the block id is the root of
    /// `leaves(height)`.
    fn row(height: u64) -> HistoryRow {
        row_and_leaves(height).0
    }

    /// The block Merkle leaves of `row(height)`, as a rotation row stores them.
    fn leaves(height: u64) -> Vec<u8> {
        row_and_leaves(height).1
    }

    fn row_and_leaves(height: u64) -> (HistoryRow, Vec<u8>) {
        let layers = required_layers(height);
        let roots: Vec<(u8, [u8; 32])> = (1..=layers).map(|n| (n, [n; 32])).collect();
        let history_proofs = (layers > 0).then(|| {
            let mut blob = vec![layers];
            for (n, root) in &roots {
                blob.push(*n);
                blob.extend_from_slice(root);
            }
            blob
        });
        let tracked_hashes = height.is_multiple_of(10).then(|| {
            let filter = EventFilter::withdrawal_default();
            let mut blob = 1u32.to_be_bytes().to_vec();
            blob.extend_from_slice(&filter.routing_dapp_id);
            blob.extend_from_slice(&filter.routing_account_id);
            blob.extend_from_slice(&1u32.to_be_bytes());
            // The message hash does not depend on the block id.
            blob.extend_from_slice(&bridge_event(height, [0; 32], height).msg_hash);
            blob
        });
        let tracked_root = tracked_hashes.as_deref().map_or([0; 32], |blob| {
            let groups = proof_data::tracked::decode_tracked_hashes(blob).unwrap();
            history_proof::ext_out_messages_root_from_groups(&HASHER, &groups)
        });
        let mut leaves = [[0u8; 32]; 16];
        leaves[BLOCK_MERKLE_HISTORY_PROOFS_LEAF_INDEX] = history_proofs_l0(&roots);
        leaves[1][24..].copy_from_slice(&height.to_be_bytes());
        leaves[BLOCK_MERKLE_TRACKED_EXT_OUT_MESSAGES_ROOT_LEAF_INDEX] = tracked_root;
        let row = HistoryRow {
            height,
            block_id: proof_data::merkle::merkle_root(&leaves),
            envelope_hash: [0xe0; 32],
            tracked_root,
            tracked_hashes,
            history_proofs,
            block_merkle_leaves: None,
            gen_utime: Some(1_791_000_000),
        };
        (row, leaves.concat())
    }

    fn event(height: u64) -> EventRow {
        bridge_event(height, row(height).block_id, height)
    }

    /// The PRIMARY attestation of `row(target)` carried in block `row(carrier)`.
    fn attestation(target: u64, carrier: u64, order: &str) -> ArchAttestation {
        ArchAttestation {
            block_id: hex::encode(row(target).block_id),
            parent_block_id: hex::encode(row(target - 1).block_id),
            envelope_hash: [0xe0; 32],
            target_type: 0,
            aggregated_signature: order.as_bytes().to_vec(),
            signature_occurrences: vec![1; 4],
            source_block_id: hex::encode(row(carrier).block_id),
            source_chain_order: order.to_string(),
        }
    }

    /// The attestations of the key blocks among `heights`, each carried in the next block.
    fn key_attestations(heights: impl IntoIterator<Item = u64>) -> Vec<ArchAttestation> {
        heights
            .into_iter()
            .filter(|h| *h > 0 && h.is_multiple_of(KEY_BLOCK_STRIDE))
            .map(|h| attestation(h, h + 1, &format!("{:08}", h + 1)))
            .collect()
    }

    /// The row of rotation block `height` with its BK set update.
    fn rotation_item(height: u64) -> ImportItem {
        let mut rotation = row(height);
        rotation.block_merkle_leaves = Some(leaves(height));
        ImportItem {
            row: rotation,
            events: vec![],
            bk_set_update: Some(ArchBkSetUpdate {
                block_id: hex::encode(row(height).block_id),
                thread_id: THREAD_ZERO_HEX.to_string(),
                height: height.to_be_bytes(),
                chain_order: format!("{height:08}"),
                bk_set_update: vec![0xbb; 40],
            }),
        }
    }

    /// A block-manager data dir whose store holds `heights` and the attestations of
    /// their key blocks; heights divisible by 10 carry an event.
    fn bm_dir(name: &str, heights: impl IntoIterator<Item = u64>) -> PathBuf {
        let heights: Vec<u64> = heights.into_iter().collect();
        let attestations = key_attestations(heights.iter().copied());
        bm_dir_with(name, heights, &[], &attestations)
    }

    /// Like `bm_dir`, with rotation blocks at `rotations` and only `attestations`.
    fn bm_dir_with(
        name: &str,
        heights: impl IntoIterator<Item = u64>,
        rotations: &[u64],
        attestations: &[ArchAttestation],
    ) -> PathBuf {
        let dir = testdir!().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let mut store = HistoryProofsStore::open(&dir).unwrap();
        let items: Vec<ImportItem> = heights
            .into_iter()
            .map(|height| {
                let mut item: ImportItem = if rotations.contains(&height) {
                    rotation_item(height)
                } else {
                    (row(height), vec![]).into()
                };
                if height % 10 == 0 {
                    item.events = vec![event(height)];
                }
                item
            })
            .collect();
        for chunk in items.chunks(5_000) {
            store.import_batch(chunk, ImportMode::All).unwrap();
        }
        for attestation in attestations {
            history_bk_set::write_attestation(store.connection(), attestation).unwrap();
        }
        dir
    }

    fn table_rows(path: &Path, table: &str) -> i64 {
        let store = HistoryProofsStore::open_file(path, Duration::from_secs(1), false).unwrap();
        store
            .connection()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }

    /// `target_height` of the stored PRIMARY attestation of `row(target)`.
    fn attestation_height(dir: &Path, target: u64) -> Option<Option<i64>> {
        store(dir)
            .connection()
            .query_row(
                "SELECT target_height FROM attestations WHERE block_id = ?1 AND target_type = 0",
                [hex::encode(row(target).block_id)],
                |r| r.get(0),
            )
            .ok()
    }

    #[test]
    fn gaps_lists_missing_attestations() {
        let dir = bm_dir_with("bm", 1..=4000, &[], &[]);
        let (code, json) = gaps(&dir);
        assert_eq!(code, EXIT_OK);
        assert_eq!(json["missing_attestations"], json!([1024, 2048, 3072]));
    }

    #[test]
    fn export_import_carry_bk_set_updates_and_attestations() {
        let attested = [
            attestation(1024, 1025, "00001025"),
            attestation(2048, 2049, "00002049"),
            attestation(2000, 2001, "00002001"),
        ];
        let source = bm_dir_with("source", 1..=3400, &[2000], &attested);
        let file = testdir!().join("export.db");
        let (code, json) = export(&source, 1, 3400, &file);
        // Key block 3072 has no attestation in the source.
        assert_eq!(code, EXIT_INCOMPLETE, "{json}");
        assert_eq!(json["source_missing_attestations"], json!([3072]));
        assert_eq!((&json["bk_set_updates"], &json["attestations"]), (&json!(1), &json!(3)));

        let target = bm_dir("target", []);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_INCOMPLETE, "{json}");
        assert_eq!(
            json["bk_set_updates"],
            json!({"inserted": 1, "present": 0, "rejected": 0, "conflicts": 0})
        );
        assert_eq!(
            json["attestations"],
            json!({"inserted": 3, "replaced": 0, "kept": 0, "not_kept": 0, "rejected": 0, "detached": 0})
        );
        assert_eq!(json["remaining_gaps"], json!([]));
        assert_eq!(json["remaining_missing_attestations"], json!([3072]));
        let target_store = target.join(HISTORY_PROOFS_DB);
        assert_eq!(table_rows(&target_store, "bk_set_updates"), 1);
        assert_eq!(table_rows(&target_store, "attestations"), 3);
        assert_eq!(attestation_height(&target, 2000), Some(Some(2000)));
    }

    #[test]
    fn export_reports_rows_it_cannot_export() {
        let source = bm_dir("source", 100..=200);
        // Leaves off a layer boundary without a BK set update: the export
        // file refuses the row.
        store(&source)
            .connection()
            .execute("UPDATE blocks SET block_merkle_leaves = zeroblob(512) WHERE height = 150", [])
            .unwrap();
        let file = testdir!().join("export.db");
        let (code, json) = export(&source, 100, 200, &file);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert_eq!(json["rejected"], 1, "{json}");
        assert_eq!(json["rows"], 100, "{json}");
    }

    #[test]
    fn import_brings_attestation_of_key_block_in_gap() {
        // Rotation block 3000 carried the attestation of key block 2048, which
        // is in a gap of the target: the target does not keep it.
        let target = bm_dir_with("target", (1..=3400).filter(|h| *h != 2048), &[3000], &[]);
        let carried = attestation(2048, 3000, "00003000");
        let write = history_bk_set::write_attestation(store(&target).connection(), &carried);
        assert_eq!(write.unwrap(), AttestationWrite::NotKept);

        // The source has row 2048 and its attestation; the gap import brings both.
        let source = bm_dir("source", 1..=3400);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 2048, 2048, &file).0, EXIT_OK);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_OK, "{json}");
        assert_eq!(json["inserted"], 1);
        assert_eq!(attestation_height(&target, 2048), Some(Some(2048)));
        let (_, gaps) = gaps(&target);
        assert!(!gaps["missing_attestations"].as_array().unwrap().contains(&json!(2048)), "{gaps}");
    }

    #[test]
    fn gap_import_brings_attestation_of_block_below_gap() {
        // The target stored key block 2048 and restarted before the blocks
        // that carry its attestation: they are in the gap 2049..=2099.
        let source = bm_dir("source", 1..=3400);
        let target = bm_dir_with("target", (1..=2048).chain(2100..=3400), &[], &[]);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 2049, 2099, &file).0, EXIT_OK);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_OK, "{json}");
        assert_eq!(json["attestations"]["inserted"], 1, "{json}");
        assert_eq!(attestation_height(&target, 2048), Some(Some(2048)));
        let (_, gaps) = gaps(&target);
        // The target has no attestations of its own key blocks 1024 and 3072.
        assert_eq!(gaps["missing_attestations"], json!([1024, 3072]), "{gaps}");
    }

    #[test]
    fn import_adds_attestations_outside_gaps() {
        let attested = [
            attestation(1024, 1025, "00001025"),
            attestation(2048, 2049, "00002049"),
            attestation(2000, 2001, "00002001"),
        ];
        let source = bm_dir_with("source", 1..=3000, &[2000], &attested);
        let target = bm_dir_with("target", 1..=3000, &[2000], &[]);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 1, 3000, &file).0, EXIT_OK);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_OK, "{json}");
        assert_eq!(json["inserted"], 0);
        assert_eq!(json["attestations"]["inserted"], 3);
        assert_eq!(json["remaining_missing_attestations"], json!([]));
    }

    #[test]
    fn import_takes_later_attestation_copy() {
        for (target_order, file_order, result) in
            [("00000010", "00000020", "replaced"), ("00000020", "00000010", "kept")]
        {
            let source = bm_dir_with(
                &format!("source-{result}"),
                2040..=2050,
                &[],
                &[attestation(2048, 2049, file_order)],
            );
            let target = bm_dir_with(
                &format!("target-{result}"),
                2040..=2050,
                &[],
                &[attestation(2048, 2049, target_order)],
            );
            let file = testdir!().join(format!("export-{result}.db"));
            assert_eq!(export(&source, 2040, 2050, &file).0, EXIT_OK);
            let (code, json) = import(&target, &file, false);
            assert_eq!(code, EXIT_OK, "{json}");
            assert_eq!(json["attestations"][result], 1, "{json}");
            let order: String = store(&target)
                .connection()
                .query_row(
                    "SELECT source_chain_order FROM attestations WHERE block_id = ?1",
                    [hex::encode(row(2048).block_id)],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(order, "00000020");
        }
    }

    #[test]
    fn import_rejects_mismatching_attestation() {
        let source =
            bm_dir_with("source", 2040..=2050, &[], &[attestation(2048, 2049, "00002049")]);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 2040, 2050, &file).0, EXIT_OK);
        rusqlite::Connection::open(&file)
            .unwrap()
            .execute("UPDATE attestations SET envelope_hash = ?1", [[0xee_u8; 32]])
            .unwrap();
        let target = bm_dir_with("target", 2040..=2050, &[], &[]);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert_eq!(json["attestations"]["rejected"], 1);
        assert_eq!(table_rows(&target.join(HISTORY_PROOFS_DB), "attestations"), 0);
    }

    #[test]
    fn reimport_changes_nothing() {
        let attested = [
            attestation(1024, 1025, "00001025"),
            attestation(2048, 2049, "00002049"),
            attestation(2000, 2001, "00002001"),
        ];
        let source = bm_dir_with("source", 1..=3000, &[2000], &attested);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 1, 3000, &file).0, EXIT_OK);
        let target = bm_dir("target", []);
        assert_eq!(import(&target, &file, false).0, EXIT_OK);

        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_OK, "{json}");
        assert_eq!(json["inserted"], 0);
        assert_eq!(json["bk_set_updates"]["inserted"], 0);
        assert_eq!(
            json["attestations"],
            json!({"inserted": 0, "replaced": 0, "kept": 3, "not_kept": 0, "rejected": 0, "detached": 0})
        );
        let (code, json) = import(&target, &file, true);
        assert_eq!(code, EXIT_OK, "{json}");
        assert_eq!(json["present"], 3000);
        assert_eq!(json["bk_set_updates"]["present"], 1);
        assert_eq!(json["attestations"]["kept"], 3);
    }

    fn exec(cmd: HistoryProofsCommand, dir: &Path) -> (i32, Value) {
        let mut out = Vec::new();
        let code = run_with_output(cmd, dir, &EventFilters::default(), &mut out);
        let text = String::from_utf8(out).unwrap();
        let json = text.lines().last().and_then(|line| serde_json::from_str(line).ok());
        (code, json.unwrap_or(Value::Null))
    }

    fn gaps(dir: &Path) -> (i32, Value) {
        exec(HistoryProofsCommand::Gaps { json: true }, dir)
    }

    fn export(dir: &Path, from: u64, to: u64, out: &Path) -> (i32, Value) {
        exec(
            HistoryProofsCommand::Export {
                from_height: from,
                to_height: to,
                out: out.to_path_buf(),
                source_id: Some("bm-source".to_string()),
                json: true,
            },
            dir,
        )
    }

    fn import(dir: &Path, from: &Path, all: bool) -> (i32, Value) {
        exec(
            HistoryProofsCommand::Import {
                from: from.to_path_buf(),
                all,
                from_height: None,
                to_height: None,
                json: true,
            },
            dir,
        )
    }

    fn store(dir: &Path) -> HistoryProofsStore {
        HistoryProofsStore::open(dir).unwrap()
    }

    fn all_events(dir: &Path) -> Vec<EventRow> {
        store(dir).events_in_range(0, u64::MAX >> 1).unwrap()
    }

    #[test]
    fn gaps_reports_first_last_and_holes() {
        let dir = bm_dir("bm", (100..=110).chain(120..=130).chain([140]));
        assert_eq!(
            gaps(&dir),
            (
                0,
                json!({"first_height": 100, "last_height": 140, "gaps": [[111, 119], [131, 139]],
                       "missing_attestations": [], "failed_checks": [],
                       "unverified": [[100, 110], [120, 130], [140, 140]],
                       "unprovable_events": {"heights": 0, "ranges": [], "total_ranges": 0}})
            )
        );
        let empty = bm_dir("empty", []);
        assert_eq!(
            gaps(&empty),
            (
                0,
                json!({"first_height": null, "last_height": null, "gaps": [],
                       "missing_attestations": [], "failed_checks": [], "unverified": [],
                       "unprovable_events": {"heights": 0, "ranges": [], "total_ranges": 0}})
            )
        );
        let missing = testdir!().join("missing");
        assert_eq!(gaps(&missing).0, EXIT_USAGE);
        assert!(!missing.join(HISTORY_PROOFS_DB).exists());
    }

    /// `unverified` reads the ranges only: which rows the verifier keeps out of
    /// `verified` is pinned by the `history_verifier` tests.
    #[test]
    fn gaps_lists_heights_not_verified_yet() {
        let dir = bm_dir("bm", 100..=140);
        {
            let store = store(&dir);
            history_ranges::add_range(store.connection(), RangeTable::Verified, 100, 109).unwrap();
            history_ranges::add_range(store.connection(), RangeTable::Verified, 111, 120).unwrap();
        }
        assert_eq!(gaps(&dir).1["unverified"], json!([[110, 110], [121, 140]]));

        let mut out = Vec::new();
        let cmd = HistoryProofsCommand::Gaps { json: false };
        assert_eq!(run_with_output(cmd, &dir, &EventFilters::default(), &mut out), EXIT_OK);
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("rows not verified yet (21 heights): 110..=110, 121..=140"),
            "{text}"
        );
    }

    /// The playbook skips a source without the store and stops on any other
    /// failure of its `gaps`.
    #[test]
    fn open_tells_a_missing_file_from_one_that_does_not_open() {
        let dir = testdir!();
        let err = format!("{:#}", open(&dir.join("missing.db")).err().unwrap());
        assert!(err.ends_with(": no such file"), "{err}");
        let not_a_file = dir.join("dir.db");
        std::fs::create_dir(&not_a_file).unwrap();
        let err = format!("{:#}", open(&not_a_file).err().unwrap());
        assert!(!err.contains("no such file"), "{err}");
    }

    #[test]
    fn export_writes_rows_events_and_metadata() {
        let source = bm_dir("source", 100..=200);
        let out = testdir!().join("history-export-120-130.db");
        let (code, json) = export(&source, 120, 130, &out);
        assert_eq!(code, 0);
        assert_eq!(
            json,
            json!({"requested": [120, 130], "source_first": 100, "source_last": 200,
                   "source_gaps": [], "source_missing_attestations": [], "rows": 11,
                   "events": 2, "events_by_kind": {"withdrawal": 2, "voucher": 0}, "rejected": 0, "bk_set_updates": 0, "attestations": 0,
                   "failed_checks": []})
        );
        let exported = HistoryProofsStore::open_file(&out, Duration::from_secs(1), false).unwrap();
        assert_eq!(
            exported.rows_in_range(0, 1000).unwrap(),
            (120..=130).map(row).collect::<Vec<_>>()
        );
        assert_eq!(exported.events_in_range(0, 1000).unwrap(), vec![event(120), event(130)]);
        let meta: Vec<(String, String)> = exported
            .connection()
            .prepare("SELECT key, value FROM export_meta ORDER BY key")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let keys: Vec<&str> = meta.iter().map(|(k, _)| k.as_str()).collect();
        let mut expected = EXPORT_META_KEYS.to_vec();
        expected.sort_unstable();
        assert_eq!(keys, expected, "EXPORT_META_KEYS");
        assert_eq!(
            keys,
            vec![
                "bridge_event_dst",
                "bridge_event_routing",
                "exported_at",
                "requested_from",
                "requested_to",
                "source_first",
                "source_gaps",
                "source_id",
                "source_last",
                "source_missing_attestations",
                "voucher_event_dst",
                "voucher_event_routing"
            ]
        );
        assert!(meta.contains(&("source_id".to_string(), "bm-source".to_string())));
        assert!(meta.contains(&("source_gaps".to_string(), "[]".to_string())));
        // An existing output file is refused.
        assert_eq!(export(&source, 120, 130, &out).0, EXIT_USAGE);
    }

    #[test]
    fn export_reports_missing_prefix_suffix_and_empty() {
        let source = bm_dir("source", 100..=200);
        let out = testdir!().join("wide.db");
        let (code, json) = export(&source, 50, 250, &out);
        assert_eq!(code, EXIT_INCOMPLETE);
        assert_eq!(json["source_gaps"], json!([[50, 99], [201, 250]]));
        assert_eq!(json["rows"], 101);
        assert!(out.exists());
        let (code, json) = export(&source, 300, 400, &testdir!().join("empty.db"));
        assert_eq!(code, EXIT_INCOMPLETE);
        assert_eq!(json["rows"], 0);
        assert_eq!(json["source_gaps"], json!([[300, 400]]));
        assert_eq!(export(&source, 20, 10, &testdir!().join("reversed.db")).0, EXIT_USAGE);
    }

    #[test]
    fn import_fills_gaps_and_reports_remaining() {
        let source = bm_dir("source", 90..=140);
        let target = bm_dir("target", (100..=110).chain(120..=130));
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 90, 140, &file).0, 0);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, 0, "{json}");
        assert_eq!(json["requested"], json!([90, 140]));
        assert_eq!(json["read"], 51);
        assert_eq!(json["inserted"], 51 - 22);
        assert_eq!(json["remaining_gaps"], json!([]));
        assert_eq!(json["conflicts"], 0);
        assert_eq!(json["rejected"], 0);
        assert_eq!(
            history_ranges::ranges(store(&target).connection(), RangeTable::Coverage).unwrap(),
            vec![(90, 140)]
        );
        assert_eq!(
            all_events(&target),
            (90..=140).filter(|h| h % 10 == 0).map(event).collect::<Vec<_>>()
        );
    }

    #[test]
    fn import_from_partial_export_reports_remaining_gaps_exit_1() {
        let source = bm_dir("source", 95..=140);
        let target = bm_dir("target", (100..=110).chain(120..=130));
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 90, 140, &file).0, EXIT_INCOMPLETE);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_INCOMPLETE);
        assert_eq!(json["remaining_gaps"], json!([[90, 94]]));
    }

    #[test]
    fn reimport_after_success_and_after_interruption_exits_0() {
        let source = bm_dir("source", 1..=12_000);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 1, 12_000, &file).0, 0);

        let target = bm_dir("target", []);
        assert_eq!(import(&target, &file, false).0, 0);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, 0);
        assert_eq!(json["inserted"], 0);
        // 12 000 skipped rows and their 1 200 events.
        assert_eq!(json["outside_gaps"], 12_000 + 1_200);

        // Interrupted: only the first batch reached the target.
        let first_batch = testdir!().join("first-batch.db");
        assert_eq!(export(&source, 1, BATCH_HEIGHTS, &first_batch).0, 0);
        let interrupted = bm_dir("interrupted", []);
        assert_eq!(import(&interrupted, &first_batch, false).0, 0);
        assert_eq!(import(&interrupted, &file, false).0, 0);
        let target_store = store(&interrupted);
        assert_eq!(target_store.rows_in_range(1, 12_000).unwrap().len(), 12_000);
        assert_eq!(all_events(&interrupted).len(), 1_200);
    }

    #[test]
    fn import_all_reports_conflicts() {
        let source = bm_dir("source", 100..=130);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 100, 130, &file).0, 0);
        let target = bm_dir("target", []);
        {
            let mut target_store = store(&target);
            let mut other_id = row(110);
            other_id.block_id = [0x11; 32];
            let mut other_envelope = row(120);
            other_envelope.envelope_hash = [0x12; 32];
            let mut other_blob = row(130);
            other_blob.gen_utime = Some(5);
            for r in [other_id, other_envelope, other_blob] {
                target_store.insert_block(&r, &[]).unwrap();
            }
        }
        let (code, json) = import(&target, &file, true);
        assert_eq!(code, EXIT_BAD_DATA);
        assert_eq!(json["conflicts"], 3);
        assert_eq!(json["inserted"], 28);
        assert_eq!(json["rejected"], 3, "the events of conflicting blocks are rejected");
        assert_eq!(all_events(&target), vec![event(100)]);

        // Without `--all` the occupied heights are skipped, not compared.
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, 0);
        assert_eq!(json["conflicts"], 0);
    }

    /// An export whose event is not the message its BOC holds: the row is
    /// rejected and its height stays a gap.
    #[test]
    fn import_rejects_rows_with_forged_events() {
        let source = bm_dir("source", 100..=130);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 100, 130, &file).0, 0);
        rusqlite::Connection::open(&file)
            .unwrap()
            .execute("UPDATE events SET boc = ?1 WHERE height = 110", [event(120).boc])
            .unwrap();
        let target = bm_dir("target", []);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_BAD_DATA);
        assert_eq!(json["rejected"], 2, "the row and its event");
        assert_eq!(json["inserted"], 30);
        assert_eq!(json["remaining_gaps"], json!([[110, 110]]));
        assert_eq!(all_events(&target), vec![event(100), event(120), event(130)]);
    }

    /// Each reason of a rejected row is a kind of its own in the bounded
    /// list: forged events do not hide behind rows with a bad format.
    #[test]
    fn import_counts_rejected_rows_by_reason() {
        let source = bm_dir("source", 10..=300);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 10, 300, &file).0, EXIT_OK);
        rusqlite::Connection::open(&file)
            .unwrap()
            .execute("UPDATE events SET boc = ?1 WHERE height != 300", [event(300).boc])
            .unwrap();
        let target = bm_dir("target", []);
        let mut out = Vec::new();
        let cmd = HistoryProofsCommand::Import {
            from: file,
            all: false,
            from_height: None,
            to_height: None,
            json: false,
        };
        assert_eq!(run_with_output(cmd, &target, &EventFilters::default(), &mut out), 3);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("... and 9 more rows with a rejected event"), "{text}");
    }

    #[test]
    fn failed_ranges_line_is_bounded() {
        let ranges: Vec<(u64, u64)> = (1..=25).map(|n| (n * 10, n * 10)).collect();
        let line = format_ranges_bounded(&ranges);
        assert!(line.starts_with("10..=10, 20..=20"), "{line}");
        assert!(line.ends_with("200..=200 ... and 5 more ranges"), "{line}");
        assert_eq!(format_ranges_bounded(&ranges[..3]), "10..=10, 20..=20, 30..=30");
    }

    #[test]
    fn import_does_not_pause_on_empty_batches() {
        // A backup with 11 rows imported over 1 000 000 heights: 200 batches,
        // all but one empty.
        let source = bm_dir("source", 100..=110);
        let backup = testdir!().join("backup.db");
        store(&source).connection().execute("VACUUM INTO ?1", [backup.to_str().unwrap()]).unwrap();
        let target = bm_dir("target", []);
        let started = std::time::Instant::now();
        let (_, json) = exec(
            HistoryProofsCommand::Import {
                from: backup,
                all: false,
                from_height: Some(1),
                to_height: Some(1_000_000),
                json: true,
            },
            &target,
        );
        let elapsed = started.elapsed();
        assert_eq!(json["inserted"], 11, "{json}");
        // 200 pauses would take 22 s.
        assert!(elapsed < Duration::from_secs(10), "import took {elapsed:?}");
    }

    #[test]
    fn import_without_metadata_needs_range() {
        let source = bm_dir("source", 100..=110);
        let backup = testdir!().join("backup.db");
        store(&source).connection().execute("VACUUM INTO ?1", [backup.to_str().unwrap()]).unwrap();
        let target = bm_dir("target", []);
        assert_eq!(import(&target, &backup, false).0, EXIT_USAGE);
        let (code, json) = exec(
            HistoryProofsCommand::Import {
                from: backup.clone(),
                all: false,
                from_height: Some(100),
                to_height: Some(110),
                json: true,
            },
            &target,
        );
        assert_eq!(code, 0, "{json}");
        assert_eq!(json["inserted"], 11);
    }

    #[test]
    fn import_refuses_foreign_user_version() {
        let source = bm_dir("source", 100..=110);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 100, 110, &file).0, 0);
        let target = bm_dir("target", [200]);

        let foreign = testdir!().join("foreign.db");
        std::fs::copy(&file, &foreign).unwrap();
        rusqlite::Connection::open(&foreign)
            .unwrap()
            .execute_batch("PRAGMA user_version = 2")
            .unwrap();
        assert_eq!(import(&target, &foreign, false).0, EXIT_USAGE);

        let foreign_target = bm_dir("foreign-target", [200]);
        rusqlite::Connection::open(foreign_target.join(HISTORY_PROOFS_DB))
            .unwrap()
            .execute_batch("PRAGMA user_version = 2")
            .unwrap();
        assert_eq!(import(&foreign_target, &file, false).0, EXIT_USAGE);

        let nowhere = testdir!().join("nowhere.db");
        assert_eq!(import(&target, &nowhere, false).0, EXIT_USAGE);
        assert!(!nowhere.exists());
        assert_eq!(store(&target).rows_in_range(0, 1000).unwrap(), vec![row(200)]);
    }

    /// A row that failed its checks is named with the check and the reason:
    /// the verifier does not run a failed check again, so import is the only
    /// place that names it (the store counts it in bm_history_check_failures).
    #[test]
    fn import_names_rows_that_fail_their_checks() {
        let source = bm_dir("source", []);
        {
            let mut bad = row(128);
            bad.block_merkle_leaves = Some(vec![0x42; 512]);
            store(&source).insert_block(&bad, &[]).unwrap();
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 128, 128, &file).0, 0);
        let target = bm_dir("target", []);
        let mut out = Vec::new();
        let cmd = HistoryProofsCommand::Import {
            from: file,
            all: false,
            from_height: None,
            to_height: None,
            json: false,
        };
        run_with_output(cmd, &target, &EventFilters::default(), &mut out);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("check merkle_leaves failed at heights 128..=128: "), "{text}");
    }

    /// An export records the source's bridge filter; a target with another
    /// one refuses the file. The events are kept as the source stored them:
    /// rows from before a change of the flags have other events and are
    /// still valid.
    #[test]
    fn import_refuses_an_export_of_another_bridge_filter() {
        let source = bm_dir("source", 1..=30);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 1, 30, &file).0, EXIT_OK);
        let filter = EventFilter::withdrawal_default();
        assert_eq!(
            export_meta(&file, "bridge_event_routing"),
            format!(
                "{}::{}",
                hex::encode(filter.routing_dapp_id),
                hex::encode(filter.routing_account_id)
            )
        );
        assert_eq!(export_meta(&file, "bridge_event_dst"), filter.dst());

        let target = bm_dir("target", []);
        let other = EventFilters {
            withdrawal: EventFilter {
                dst_address: [0x01; 32],
                ..EventFilter::withdrawal_default()
            },
            ..EventFilters::default()
        };
        let cmd = HistoryProofsCommand::Import {
            from: file,
            all: false,
            from_height: None,
            to_height: None,
            json: true,
        };
        assert_eq!(run_with_output(cmd.clone(), &target, &other, &mut Vec::new()), EXIT_USAGE);
        // Another bridge account with the same dst is refused too.
        let other_routing = EventFilters {
            withdrawal: EventFilter {
                routing_account_id: [0x1b; 32],
                ..EventFilter::withdrawal_default()
            },
            ..EventFilters::default()
        };
        assert_eq!(run_with_output(cmd, &target, &other_routing, &mut Vec::new()), EXIT_USAGE);
        assert_eq!(store(&target).rows_in_range(0, 100).unwrap(), vec![]);
    }

    /// An export that did not finish has export metadata without some key, or
    /// none at all: import refuses it, also with an explicit range.
    #[test]
    fn import_refuses_an_export_without_some_metadata() {
        let source = bm_dir("source", 1..=30);
        let root = testdir!();
        let file = root.join("export.db");
        assert_eq!(export(&source, 1, 30, &file).0, EXIT_OK);
        let target = bm_dir("target", []);
        let deletions = EXPORT_META_KEYS
            .iter()
            .map(|key| format!("DELETE FROM export_meta WHERE key = '{key}'"))
            .chain(["DELETE FROM export_meta".to_string()]);
        for (n, deletion) in deletions.enumerate() {
            let broken = root.join(format!("broken-{n}.db"));
            std::fs::copy(&file, &broken).unwrap();
            rusqlite::Connection::open(&broken).unwrap().execute(&deletion, []).unwrap();
            assert_eq!(import(&target, &broken, false).0, EXIT_USAGE, "{deletion}");
            let ranged = HistoryProofsCommand::Import {
                from: broken,
                all: false,
                from_height: Some(1),
                to_height: Some(30),
                json: true,
            };
            assert_eq!(exec(ranged, &target).0, EXIT_USAGE, "{deletion}");
        }
        assert_eq!(store(&target).rows_in_range(0, 100).unwrap(), vec![]);
    }

    fn export_meta(file: &Path, key: &str) -> String {
        rusqlite::Connection::open(file)
            .unwrap()
            .query_row("SELECT value FROM export_meta WHERE key = ?1", [key], |r| r.get(0))
            .unwrap()
    }

    /// The target kept the attestation of key block 2048 while row 2047 was
    /// missing; the imported 2047 is not its parent, so it is deleted, and
    /// that is bad data on its own.
    #[test]
    fn import_deleting_an_attestation_exits_3() {
        let target = bm_dir("target", (1..=2400).filter(|h| *h != 2047));
        let source = bm_dir("source", (1..=2400).filter(|h| *h != 2047));
        {
            let mut other = row(2047);
            other.block_id = [0x77; 32];
            store(&source).insert_block(&other, &[]).unwrap();
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 2047, 2047, &file).0, EXIT_OK);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert_eq!(json["attestations"]["detached"], 1, "{json}");
        assert_eq!(json["rejected"], 0, "{json}");
        assert_eq!(json["row_check_failures"], 0, "{json}");
        // The attestation is gone: a second run passes, and the block waits in
        // missing_attestations for one that agrees with the row.
        assert_eq!(import(&target, &file, false).0, EXIT_OK);
        let missing = gaps(&target).1["missing_attestations"].clone();
        assert!(missing.as_array().unwrap().contains(&json!(2048)), "{missing}");
    }

    /// Conflicts alone, rows without events: exit 3.
    #[test]
    fn import_conflicts_alone_exit_3() {
        let source = bm_dir("source", 11..=19);
        let target = bm_dir("target", []);
        {
            let mut target_store = store(&target);
            for height in 11..=19 {
                let mut other = row(height);
                other.gen_utime = Some(1);
                target_store.insert_block(&other, &[]).unwrap();
            }
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 11, 19, &file).0, EXIT_OK);
        let (code, json) = import(&target, &file, true);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert_eq!(json["conflicts"], 9, "{json}");
        assert_eq!(json["rejected"], 0, "{json}");
    }

    /// A row that fails two checks is one row with failed checks.
    #[test]
    fn import_counts_a_row_failing_two_checks_once() {
        let source = bm_dir("source", []);
        {
            let mut bad = row(128);
            bad.tracked_root = [0x01; 32];
            bad.block_merkle_leaves = Some(vec![0x42; 512]);
            store(&source).insert_block(&bad, &[]).unwrap();
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 128, 128, &file).0, EXIT_OK);
        let target = bm_dir("target", []);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert_eq!(json["row_check_failures"], 1, "{json}");
    }

    /// At most PROBLEMS_PER_KIND lines of each kind, then how many more.
    #[test]
    fn import_prints_a_bounded_list_of_problems() {
        let source = bm_dir("source", 10..=40);
        let target = bm_dir("target", []);
        {
            let mut target_store = store(&target);
            for height in 10..=40 {
                let mut other = row(height);
                other.gen_utime = Some(1);
                target_store.insert_block(&other, &[]).unwrap();
            }
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 10, 40, &file).0, EXIT_OK);
        let mut out = Vec::new();
        let cmd = HistoryProofsCommand::Import {
            from: file,
            all: true,
            from_height: None,
            to_height: None,
            json: false,
        };
        assert_eq!(run_with_output(cmd, &target, &EventFilters::default(), &mut out), 3);
        let text = String::from_utf8(out).unwrap();
        let conflicts = text.lines().filter(|line| line.starts_with("conflict at height")).count();
        assert_eq!(conflicts, PROBLEMS_PER_KIND, "{text}");
        assert!(
            text.contains(&format!(
                "conflict at height 11: block {} at height 11 differs from the stored row in gen_utime",
                hex::encode(row(11).block_id)
            )),
            "{text}"
        );
        assert!(
            text.contains(&format!("... and {} more conflicts", 31 - PROBLEMS_PER_KIND)),
            "{text}"
        );
    }

    /// The chains differ at a key block: the target has another block at the
    /// height the source's attestation signs. It is bad data, not an
    /// attestation to bring later.
    #[test]
    fn import_flags_an_attestation_of_another_block_at_its_height() {
        let source = bm_dir("source", 1..=2400);
        let target = bm_dir("target", (1..=2400).filter(|h| *h != 2048));
        {
            let mut other = row(2048);
            other.block_id = [0x99; 32];
            store(&target).insert_block(&other, &[]).unwrap();
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 2048, 2048, &file).0, EXIT_OK);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert_eq!(json["attestations"]["rejected"], 1, "{json}");
    }

    /// The source's verifier failed row 128: export leaves it out and exits 3,
    /// so known-bad rows do not spread, and its height stays a gap of the
    /// file. No failure bits travel with an export (spec 5.7).
    #[test]
    fn export_leaves_out_rows_that_failed_their_checks_and_exits_3() {
        let source = source_with_bad_leaves_at_128(MERKLE_LEAVES);
        for height in [127, 129] {
            store(&source).insert_block(&row(height), &[]).unwrap();
        }
        let file = testdir!().join("export.db");
        let (code, json) = export(&source, 127, 129, &file);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert_eq!(json["failed_checks"], json!([[128, 128]]), "{json}");
        assert_eq!(json["rows"], 2, "{json}");
        assert_eq!(table_rows(&file, "blocks"), 2);
        assert_eq!(table_rows(&file, "check_state"), 0, "no failure bits in the file");
        let target = bm_dir("target", []);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_INCOMPLETE, "{json}");
        assert_eq!(json["remaining_gaps"], json!([[128, 128]]), "{json}");
    }

    /// A row with an unprovable bridge event is not bad (spec 5.7): export
    /// takes it.
    #[test]
    fn export_takes_a_row_with_an_unprovable_bridge_event() {
        let source = bm_dir("source", 1..=20);
        store(&source)
            .connection()
            .execute(
                "INSERT INTO check_state (height, done, failed) VALUES (10, ?1, ?1)",
                [EVENT_IN_TREE],
            )
            .unwrap();
        let file = testdir!().join("export.db");
        let (code, json) = export(&source, 1, 20, &file);
        assert_eq!(code, EXIT_OK, "{json}");
        assert_eq!(json["failed_checks"], json!([]), "{json}");
        assert_eq!(json["rows"], 20, "{json}");
    }

    /// Each event keeps the routing it was stored with, also through an export.
    #[test]
    fn export_and_import_carry_the_routing_of_each_event() {
        let source = bm_dir("source", []);
        let mut stored = event(10);
        stored.routing_dapp_id = [0x5a; 32];
        store(&source).insert_block(&row(10), std::slice::from_ref(&stored)).unwrap();
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 10, 10, &file).0, EXIT_OK);
        let target = bm_dir("target", []);
        assert_eq!(import(&target, &file, false).0, EXIT_OK);
        assert_eq!(all_events(&target), vec![stored]);
    }

    /// An event that is not in the bridge's tracked group cannot be proven,
    /// but the row is not bad: it happens on correct data on every
    /// block-manager (spec 5.7). An alert only.
    #[test]
    fn an_unprovable_event_is_an_alert_only() {
        let source = bm_dir("source", []);
        {
            let mut untracked = row(10);
            untracked.tracked_hashes = None;
            untracked.tracked_root = [0; 32];
            store(&source).insert_block(&untracked, &[event(10)]).unwrap();
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 10, 10, &file).0, EXIT_OK);
        let target = bm_dir("target", []);
        let mut out = Vec::new();
        let cmd = HistoryProofsCommand::Import {
            from: file.clone(),
            all: false,
            from_height: None,
            to_height: None,
            json: false,
        };
        assert_eq!(run_with_output(cmd, &target, &EventFilters::default(), &mut out), EXIT_OK);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("check event_in_tree failed at heights 10..=10"), "{text}");
        assert!(text.contains("row check failures 0"), "{text}");
        assert!(text.contains("unprovable events, an alert only (1 heights): 10..=10"), "{text}");
        assert_eq!(gaps(&target).1["failed_checks"], json!([]));
        // The heights are listed, so the alert can be followed without SQL.
        let listed = json!({"heights": 1, "ranges": [[10, 10]], "total_ranges": 1});
        assert_eq!(gaps(&target).1["unprovable_events"], listed);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_OK, "{json}");
        assert_eq!(json["unprovable_events"], listed, "{json}");
    }

    /// After the bridge routing leaves the tracked routings every height with
    /// an event has the mark: the lists keep the newest ranges only.
    #[test]
    fn unprovable_events_list_is_bounded() {
        let dir = bm_dir("bm", []);
        for height in (1..=150u64).map(|n| n * 10) {
            store(&dir)
                .connection()
                .execute(
                    "INSERT INTO check_state (height, done, failed) VALUES (?1, ?2, ?2)",
                    rusqlite::params![height as i64, EVENT_IN_TREE],
                )
                .unwrap();
        }
        let listed = gaps(&dir).1["unprovable_events"].clone();
        assert_eq!(listed["heights"], 150, "{listed}");
        assert_eq!(listed["total_ranges"], 150, "{listed}");
        let ranges = listed["ranges"].as_array().unwrap();
        assert_eq!(ranges.len(), UNPROVABLE_RANGES_SHOWN, "{listed}");
        assert_eq!(ranges.first().unwrap(), &json!([510, 510]), "{listed}");
        assert_eq!(ranges.last().unwrap(), &json!([1500, 1500]), "{listed}");

        let mut out = Vec::new();
        let cmd = HistoryProofsCommand::Gaps { json: false };
        run_with_output(cmd, &dir, &EventFilters::default(), &mut out);
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains(
                "unprovable events, an alert only (150 heights, the newest 100 of 150 ranges): 510..=510, 520..=520, "
            ),
            "{text}"
        );
    }

    /// An SQLite error on the export metadata names the file.
    #[test]
    fn export_metadata_errors_name_the_file() {
        let source = bm_dir("source", 1..=3);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 1, 3, &file).0, EXIT_OK);
        rusqlite::Connection::open(&file)
            .unwrap()
            .execute_batch("DROP TABLE export_meta; CREATE TABLE export_meta (other TEXT)")
            .unwrap();
        let exported = HistoryProofsStore::open_file(&file, Duration::from_secs(1), false).unwrap();
        let err = ExportMeta::read(&exported, &file).err().expect("no key column");
        let message = format!("{err:#}");
        assert!(message.contains(&file.display().to_string()), "{message}");
    }

    /// Row 128 with leaves that are not the ones of its block id, and the
    /// failure bits `failed` in the source's check state.
    fn source_with_bad_leaves_at_128(failed: i64) -> PathBuf {
        let source = bm_dir("source", []);
        let mut source_store = store(&source);
        let mut bad = row(128);
        bad.block_merkle_leaves = Some(vec![0x42; 512]);
        source_store.insert_block(&bad, &[]).unwrap();
        source_store
            .connection()
            .execute(
                "INSERT INTO check_state (height, done, failed) VALUES (128, ?1, ?1)",
                [failed],
            )
            .unwrap();
        source
    }

    /// Bad data stays bad: the rows that failed their checks are in the
    /// store, so a second import of the same file exits 3 again and `gaps`
    /// lists them until the store is restored.
    #[test]
    fn import_after_bad_data_exits_3_again() {
        let source = bm_dir("source", []);
        {
            let mut bad = row(128);
            bad.block_merkle_leaves = Some(vec![0x42; 512]);
            store(&source).insert_block(&bad, &[]).unwrap();
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 128, 128, &file).0, EXIT_OK);
        let target = bm_dir("target", []);
        assert_eq!(import(&target, &file, false).0, EXIT_BAD_DATA);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert_eq!(json["failed_checks"], json!([[128, 128]]));
        assert_eq!(gaps(&target).1["failed_checks"], json!([[128, 128]]));
    }

    #[test]
    fn import_boundary_row_with_bad_leaves_counts_row_check_failure() {
        let source = bm_dir("source", []);
        {
            let mut source_store = store(&source);
            let mut bad = row(128);
            bad.block_merkle_leaves = Some(vec![0x42; 512]);
            source_store.insert_block(&bad, &[]).unwrap();
        }
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 128, 128, &file).0, 0);
        let target = bm_dir("target", []);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_BAD_DATA);
        assert_eq!(json["inserted"], 1);
        assert_eq!(json["row_check_failures"], 1);
        let failed: i64 = store(&target)
            .connection()
            .query_row("SELECT failed FROM check_state WHERE height = 128", [], |r| r.get(0))
            .unwrap();
        assert_ne!(failed & MERKLE_LEAVES, 0);
    }

    #[test]
    fn imported_rows_not_in_verified() {
        let source = bm_dir("source", 1..=300);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 1, 300, &file).0, 0);
        let target = bm_dir("target", []);
        assert_eq!(import(&target, &file, false).0, 0);
        assert!(history_ranges::ranges(store(&target).connection(), RangeTable::Verified)
            .unwrap()
            .is_empty());
    }

    /// Inserts rows from `first` on, one transaction each (about 500 a second),
    /// with a 1 s busy timeout, until `stop` is set; fails if a write waits longer.
    fn live_writer(
        dir: PathBuf,
        first: u64,
        stop: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<anyhow::Result<u64>> {
        std::thread::Builder::new()
            .name("writer".to_string())
            .spawn(move || {
                let mut writer =
                    HistoryProofsStore::open_with_busy_timeout(&dir, Duration::from_secs(1))?;
                let mut height = first;
                while !stop.load(Ordering::SeqCst) {
                    writer.insert_block(&row(height), &[])?;
                    height += 1;
                    // A live block-manager writes a few blocks a second; a writer that
                    // never pauses starves other writers (SQLite locks are not fair).
                    std::thread::sleep(Duration::from_millis(2));
                }
                Ok(height - first)
            })
            .unwrap()
    }

    #[test]
    fn import_runs_next_to_live_writer() {
        let source = bm_dir("source", 1..=20_000);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 1, 20_000, &file).0, 0);
        let target = bm_dir("target", []);
        let stop = Arc::new(AtomicBool::new(false));
        let writer = live_writer(target.clone(), 1_000_000, stop.clone());
        let (code, json) = import(&target, &file, false);
        stop.store(true, Ordering::SeqCst);
        let written =
            writer.join().unwrap().expect("the live writer must not hit its busy timeout");
        assert_eq!(code, 0, "{json}");
        assert_eq!(json["inserted"], 20_000);
        assert!(written > 0);
    }

    #[test]
    fn export_runs_next_to_live_writer() {
        let source = bm_dir("source", 1..=20_000);
        let stop = Arc::new(AtomicBool::new(false));
        let writer = live_writer(source.clone(), 1_000_000, stop.clone());
        let (code, json) = export(&source, 1, 20_000, &testdir!().join("export.db"));
        stop.store(true, Ordering::SeqCst);
        writer.join().unwrap().expect("the live writer must not hit its busy timeout");
        assert_eq!(code, 0, "{json}");
        assert_eq!(json["rows"], 20_000);
    }

    /// Imported rows reach `verified`: import records their row checks and
    /// the verifier adds the layer checks.
    #[test]
    fn imported_rows_get_verified() {
        use crate::application::services::history_verifier;
        use crate::domain::history_checks::tests::fixture_rows;
        use crate::domain::history_checks::EVENTS_ROOT;

        let source = bm_dir("source", []);
        {
            let mut source_store = store(&source);
            for row in fixture_rows().into_iter().filter(|r| {
                (16_256..=16_384).contains(&r.height)
                    || (r.height.is_multiple_of(128) && r.height <= 16_384)
            }) {
                source_store.insert_block(&row, &[]).unwrap();
            }
        }
        let file = testdir!().join("export.db");
        export(&source, 1, 16_384, &file);
        let target = bm_dir("target", []);
        let (_, json) = import(&target, &file, false);
        assert_eq!(json["inserted"], 255);
        assert_eq!(json["row_check_failures"], 0);

        let target_store = store(&target);
        let state = |height: u64| -> (i64, i64) {
            target_store
                .connection()
                .query_row(
                    "SELECT done, failed FROM check_state WHERE height = ?1",
                    [height as i64],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap()
        };
        assert_eq!(state(16_300).0 & EVENTS_ROOT, EVENTS_ROOT);
        let hasher = PoseidonHasher::new();
        while history_verifier::verify_pass(&target_store, &hasher, usize::MAX, false)
            .unwrap()
            .worked_units
            > 0
        {}
        assert_eq!(
            history_ranges::ranges(target_store.connection(), RangeTable::Verified).unwrap(),
            vec![(16_257, 16_383)]
        );
    }

    /// The playbook copies the export file without its `-wal`: once export
    /// returns, the file alone holds every row, event and metadata line.
    #[test]
    fn export_file_is_complete_without_wal() {
        let source = bm_dir("source", 1..=300);
        let dir = testdir!();
        let file = dir.join("export.db");
        let (code, json) = export(&source, 1, 300, &file);
        assert_eq!(code, 0);
        assert!(!dir.join("export.db-wal").exists());
        let copied = dir.join("copy").join("export.db");
        std::fs::create_dir_all(copied.parent().unwrap()).unwrap();
        std::fs::copy(&file, &copied).unwrap();
        let copy = HistoryProofsStore::open_file(&copied, Duration::from_secs(1), false).unwrap();
        assert_eq!(copy.rows_in_range(1, 300).unwrap().len(), 300);
        assert_eq!(copy.events_in_range(1, 300).unwrap().len() as u64, json["events"]);
        assert_eq!(table_rows(&copied, "export_meta"), 12);
    }

    #[test]
    fn export_live_file_with_wal_rows() {
        let source = bm_dir("source", []);
        // The connection stays open during the export: closing the last
        // connection would move the WAL into the main file.
        let live = HistoryProofsStore::open(&source).unwrap();
        live.connection().execute_batch("PRAGMA wal_autocheckpoint = 0").unwrap();
        let mut live = live;
        for height in 1..=500 {
            live.insert_block(&row(height), &[]).unwrap();
        }
        let wal = source.join(format!("{HISTORY_PROOFS_DB}-wal"));
        assert!(std::fs::metadata(&wal).unwrap().len() > 0, "rows must sit in the WAL");
        let (code, json) = export(&source, 1, 500, &testdir!().join("export.db"));
        assert_eq!(code, 0);
        assert_eq!(json["rows"], 500);
        drop(live);
    }

    /// A voucher at `height`, added to the stored row of `dir`.
    fn with_voucher(dir: &Path, height: u64) -> EventRow {
        let voucher = voucher_event(height, row(height).block_id, 1000 + height, [0x5c; 32]);
        store(dir).insert_block(&row(height), std::slice::from_ref(&voucher)).unwrap();
        voucher
    }

    #[test]
    fn export_writes_both_kinds_and_the_four_filter_keys() {
        let source = bm_dir("source", 100..=110);
        let voucher = with_voucher(&source, 105);
        let file = testdir!().join("export.db");
        let (code, json) = export(&source, 100, 110, &file);
        assert_eq!(code, EXIT_OK, "{json}");
        assert_eq!(json["events"], 3, "{json}");
        assert_eq!(json["events_by_kind"], json!({"withdrawal": 2, "voucher": 1}), "{json}");
        assert_eq!(table_rows(&file, "export_meta"), 12);
        let f = EventFilter::voucher_default();
        assert_eq!(
            export_meta(&file, "voucher_event_routing"),
            format!("{}::{}", hex::encode(f.routing_dapp_id), hex::encode(f.routing_account_id))
        );
        assert_eq!(export_meta(&file, "voucher_event_dst"), f.dst());
        let exported = HistoryProofsStore::open_file(&file, Duration::from_secs(1), false).unwrap();
        assert!(exported.events_in_range(105, 105).unwrap().contains(&voucher));
    }

    #[test]
    fn import_refuses_an_export_of_another_voucher_filter() {
        let source = bm_dir("source", 1..=30);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 1, 30, &file).0, EXIT_OK);
        let target = bm_dir("target", []);
        let other = EventFilters {
            voucher: EventFilter { dst_address: [0x02; 32], ..EventFilter::voucher_default() },
            ..EventFilters::default()
        };
        let cmd = HistoryProofsCommand::Import {
            from: file.clone(),
            all: false,
            from_height: None,
            to_height: None,
            json: true,
        };
        assert_eq!(run_with_output(cmd, &target, &other, &mut Vec::new()), EXIT_USAGE);

        let unfinished = testdir!().join("unfinished.db");
        std::fs::copy(&file, &unfinished).unwrap();
        rusqlite::Connection::open(&unfinished)
            .unwrap()
            .execute("DELETE FROM export_meta WHERE key = 'voucher_event_dst'", [])
            .unwrap();
        assert_eq!(import(&target, &unfinished, false).0, EXIT_USAGE);
        assert_eq!(store(&target).rows_in_range(0, 100).unwrap(), vec![]);
    }

    /// The filters of the export give the voucher's `src` and `dst` the kind
    /// `voucher`: a file that says `withdrawal` is damaged.
    #[test]
    fn import_rejects_a_voucher_whose_kind_differs_from_the_filters() {
        let source = bm_dir("source", 100..=110);
        with_voucher(&source, 105);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 100, 110, &file).0, EXIT_OK);
        rusqlite::Connection::open(&file)
            .unwrap()
            .execute(
                "UPDATE events SET kind = 'withdrawal', sk_u_commit = NULL WHERE kind = 'voucher'",
                [],
            )
            .unwrap();
        let target = bm_dir("target", []);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_BAD_DATA, "{json}");
        assert!(store(&target).row(105).unwrap().is_none());
        assert!(store(&target).row(104).unwrap().is_some());
    }

    #[test]
    fn export_and_import_carry_vouchers() {
        let source = bm_dir("source", 100..=110);
        let voucher = with_voucher(&source, 105);
        let file = testdir!().join("export.db");
        assert_eq!(export(&source, 100, 110, &file).0, EXIT_OK);
        let target = bm_dir("target", []);
        let (code, json) = import(&target, &file, false);
        assert_eq!(code, EXIT_OK, "{json}");
        assert!(all_events(&target).contains(&voucher));
    }
}
