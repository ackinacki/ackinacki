// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! Closed height ranges kept in the history proofs store: `coverage` (heights
//! that have a row) and `verified` (heights whose row ran every applicable
//! check and failed none but `event_in_tree`, an alert only).
//! Both are updated incrementally inside the writer's own short transaction;
//! a full recount over `blocks` would hold the write lock for minutes.

use rusqlite::params;
use rusqlite::Connection;
use rusqlite::OptionalExtension;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeTable {
    Coverage,
    Verified,
}

impl RangeTable {
    fn name(self) -> &'static str {
        match self {
            RangeTable::Coverage => "coverage",
            RangeTable::Verified => "verified",
        }
    }
}

/// Adds one height, extending or merging the neighbouring ranges.
pub fn add_height(conn: &Connection, table: RangeTable, height: u64) -> rusqlite::Result<()> {
    add_range(conn, table, height, height)
}

/// Adds `[start, end]`, merging every range it overlaps or touches.
pub fn add_range(
    conn: &Connection,
    table: RangeTable,
    start: u64,
    end: u64,
) -> rusqlite::Result<()> {
    debug_assert!(start <= end);
    let name = table.name();
    // Ranges are disjoint and never touch, so ends grow with starts: walk down
    // from the last range starting at or before `end + 1` until one ends
    // before `start - 1`.
    let mut merged = Vec::new();
    {
        let mut stmt = conn.prepare_cached(&format!(
            "SELECT start, end FROM {name} WHERE start <= ?1 ORDER BY start DESC"
        ))?;
        let mut rows = stmt.query(params![to_sql(end.saturating_add(1))])?;
        while let Some(row) = rows.next()? {
            let (s, e) = (from_sql(row.get(0)?), from_sql(row.get(1)?));
            if e.saturating_add(1) < start {
                break;
            }
            merged.push((s, e));
        }
    }
    let new_start = merged.iter().map(|(s, _)| *s).chain([start]).min().unwrap_or(start);
    let new_end = merged.iter().map(|(_, e)| *e).chain([end]).max().unwrap_or(end);
    let mut delete = conn.prepare_cached(&format!("DELETE FROM {name} WHERE start = ?1"))?;
    for (s, _) in &merged {
        delete.execute(params![to_sql(*s)])?;
    }
    conn.prepare_cached(&format!("INSERT INTO {name} (start, end) VALUES (?1, ?2)"))?
        .execute(params![to_sql(new_start), to_sql(new_end)])?;
    Ok(())
}

// Heights are below 2^63 (a block every millisecond for 290 million years).
fn to_sql(height: u64) -> i64 {
    height as i64
}

fn from_sql(value: i64) -> u64 {
    value as u64
}

/// Removes one height, splitting the range that holds it.
pub fn remove_height(conn: &Connection, table: RangeTable, height: u64) -> rusqlite::Result<()> {
    let name = table.name();
    let holder: Option<(i64, i64)> = conn
        .prepare_cached(&format!(
            "SELECT start, end FROM {name} WHERE start <= ?1 ORDER BY start DESC LIMIT 1"
        ))?
        .query_row(params![to_sql(height)], |row| Ok((row.get(0)?, row.get(1)?)))
        .optional()?;
    let Some((start, end)) = holder.map(|(s, e)| (from_sql(s), from_sql(e))) else {
        return Ok(());
    };
    if end < height {
        return Ok(());
    }
    conn.prepare_cached(&format!("DELETE FROM {name} WHERE start = ?1"))?
        .execute(params![to_sql(start)])?;
    let mut insert =
        conn.prepare_cached(&format!("INSERT INTO {name} (start, end) VALUES (?1, ?2)"))?;
    if start < height {
        insert.execute(params![to_sql(start), to_sql(height - 1)])?;
    }
    if height < end {
        insert.execute(params![to_sql(height + 1), to_sql(end)])?;
    }
    Ok(())
}

pub fn ranges(conn: &Connection, table: RangeTable) -> rusqlite::Result<Vec<(u64, u64)>> {
    let mut stmt =
        conn.prepare_cached(&format!("SELECT start, end FROM {} ORDER BY start", table.name()))?;
    let rows = stmt.query_map([], |row| Ok((from_sql(row.get(0)?), from_sql(row.get(1)?))))?;
    rows.collect()
}

/// First and last covered heights.
pub fn first_last(conn: &Connection) -> rusqlite::Result<Option<(u64, u64)>> {
    let (first, last): (Option<i64>, Option<i64>) =
        conn.query_row("SELECT MIN(start), MAX(end) FROM coverage", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
    Ok(first.zip(last).map(|(first, last)| (from_sql(first), from_sql(last))))
}

/// Heights between the first and the last covered height that have no row.
pub fn gaps(conn: &Connection) -> rusqlite::Result<Vec<(u64, u64)>> {
    let covered = ranges(conn, RangeTable::Coverage)?;
    Ok(covered.windows(2).map(|pair| (pair[0].1 + 1, pair[1].0 - 1)).collect())
}

/// Covered heights that are not verified yet, ascending.
pub fn unverified(conn: &Connection) -> rusqlite::Result<Vec<(u64, u64)>> {
    let covered = ranges(conn, RangeTable::Coverage)?;
    let mut verified = ranges(conn, RangeTable::Verified)?.into_iter().peekable();
    let mut out = Vec::new();
    for (start, end) in covered {
        // The first height of `start ..= end` not checked yet; `None` once the
        // rest of the range is verified.
        let mut next = Some(start);
        while let (Some(from), Some(&(a, b))) = (next, verified.peek()) {
            if b < from {
                verified.next();
                continue;
            }
            if a > end {
                break;
            }
            if a > from {
                out.push((from, a - 1));
            }
            if b >= end {
                // It may cover the next covered range too: keep it.
                next = None;
            } else {
                next = Some(b + 1);
                verified.next();
            }
        }
        if let Some(from) = next {
            out.push((from, end));
        }
    }
    Ok(out)
}

/// Heights of `[start, end]` that have no row, including those below the
/// first and above the last covered height.
pub fn missing_in(conn: &Connection, start: u64, end: u64) -> rusqlite::Result<Vec<(u64, u64)>> {
    let mut missing = Vec::new();
    let mut next = start;
    for (s, e) in ranges(conn, RangeTable::Coverage)? {
        if next > end {
            break;
        }
        if e < next {
            continue;
        }
        if s > next {
            missing.push((next, (s - 1).min(end)));
        }
        next = match e.checked_add(1) {
            Some(n) => n,
            None => return Ok(missing),
        };
    }
    if next <= end {
        missing.push((next, end));
    }
    Ok(missing)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE coverage (start INTEGER PRIMARY KEY, end INTEGER NOT NULL);
             CREATE TABLE verified (start INTEGER PRIMARY KEY, end INTEGER NOT NULL);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn coverage_ranges() {
        let conn = conn();
        assert_eq!(first_last(&conn).unwrap(), None);
        for h in [10, 11, 12] {
            add_height(&conn, RangeTable::Coverage, h).unwrap();
        }
        assert_eq!(ranges(&conn, RangeTable::Coverage).unwrap(), vec![(10, 12)]);
        add_height(&conn, RangeTable::Coverage, 14).unwrap();
        assert_eq!(ranges(&conn, RangeTable::Coverage).unwrap(), vec![(10, 12), (14, 14)]);
        assert_eq!(gaps(&conn).unwrap(), vec![(13, 13)]);
        assert_eq!(missing_in(&conn, 5, 16).unwrap(), vec![(5, 9), (13, 13), (15, 16)]);
        add_height(&conn, RangeTable::Coverage, 13).unwrap();
        assert_eq!(ranges(&conn, RangeTable::Coverage).unwrap(), vec![(10, 14)]);
        assert_eq!(missing_in(&conn, 5, 16).unwrap(), vec![(5, 9), (15, 16)]);
        assert_eq!(missing_in(&conn, 11, 13).unwrap(), vec![]);
        assert_eq!(first_last(&conn).unwrap(), Some((10, 14)));
        assert_eq!(gaps(&conn).unwrap(), vec![]);
        // The other table is independent.
        assert_eq!(ranges(&conn, RangeTable::Verified).unwrap(), vec![]);
    }

    #[test]
    fn remove_height_splits_its_range() {
        let conn = conn();
        add_range(&conn, RangeTable::Verified, 10, 20).unwrap();
        add_range(&conn, RangeTable::Verified, 30, 30).unwrap();
        remove_height(&conn, RangeTable::Verified, 15).unwrap();
        assert_eq!(
            ranges(&conn, RangeTable::Verified).unwrap(),
            vec![(10, 14), (16, 20), (30, 30)]
        );
        remove_height(&conn, RangeTable::Verified, 10).unwrap();
        remove_height(&conn, RangeTable::Verified, 20).unwrap();
        remove_height(&conn, RangeTable::Verified, 30).unwrap();
        remove_height(&conn, RangeTable::Verified, 99).unwrap();
        assert_eq!(ranges(&conn, RangeTable::Verified).unwrap(), vec![(11, 14), (16, 19)]);
    }

    #[test]
    fn add_range_merges_overlapping_and_touching() {
        let conn = conn();
        add_range(&conn, RangeTable::Verified, 0, 0).unwrap();
        add_range(&conn, RangeTable::Verified, 5, 7).unwrap();
        add_range(&conn, RangeTable::Verified, 20, 30).unwrap();
        add_range(&conn, RangeTable::Verified, 40, 50).unwrap();
        add_range(&conn, RangeTable::Verified, 6, 19).unwrap();
        assert_eq!(ranges(&conn, RangeTable::Verified).unwrap(), vec![(0, 0), (5, 30), (40, 50)]);
        add_range(&conn, RangeTable::Verified, 1, 60).unwrap();
        assert_eq!(ranges(&conn, RangeTable::Verified).unwrap(), vec![(0, 60)]);
        add_range(&conn, RangeTable::Verified, 10, 20).unwrap();
        assert_eq!(ranges(&conn, RangeTable::Verified).unwrap(), vec![(0, 60)]);
    }

    #[test]
    fn unverified_is_coverage_minus_verified() {
        let c = conn();
        for (s, e) in [(1, 10), (20, 30), (40, 40)] {
            add_range(&c, RangeTable::Coverage, s, e).unwrap();
        }
        for (s, e) in [(1, 5), (8, 10), (20, 30)] {
            add_range(&c, RangeTable::Verified, s, e).unwrap();
        }
        assert_eq!(unverified(&c).unwrap(), vec![(6, 7), (40, 40)]);
        assert_eq!(unverified(&conn()).unwrap(), vec![]);
        let all = conn();
        add_range(&all, RangeTable::Coverage, 1, 9).unwrap();
        add_range(&all, RangeTable::Verified, 1, 9).unwrap();
        assert_eq!(unverified(&all).unwrap(), vec![]);
    }
}
