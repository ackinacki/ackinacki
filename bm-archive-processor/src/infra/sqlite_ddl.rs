// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use anyhow::Context;
use rusqlite::Connection;

fn normalize_create_table(sql: &str) -> anyhow::Result<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.starts_with("create table") {
        return Err(anyhow::anyhow!("unexpected table DDL: {sql}"));
    }

    if lower.contains("create table if not exists") {
        return Ok(sql.to_owned());
    }

    let prefix_len = "create table".len();

    Ok(format!("CREATE TABLE IF NOT EXISTS{}", &sql[prefix_len..]))
}

fn normalize_create_index(sql: &str) -> anyhow::Result<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.starts_with("create index") && !lower.starts_with("create unique index") {
        return Err(anyhow::anyhow!("unexpected index DDL: {sql}"));
    }

    if lower.contains("index if not exists") {
        return Ok(sql.to_owned());
    }

    let replaced = if lower.starts_with("create unique index") {
        let prefix_len = "create unique index".len();
        format!("CREATE UNIQUE INDEX IF NOT EXISTS{}", &sql[prefix_len..])
    } else {
        let prefix_len = "create index".len();
        format!("CREATE INDEX IF NOT EXISTS{}", &sql[prefix_len..])
    };

    Ok(replaced)
}

pub fn fetch_create_table_ddls(conn: &Connection, tables: &[&str]) -> anyhow::Result<Vec<String>> {
    let mut ddls = Vec::with_capacity(tables.len());

    for tbl in tables {
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?1",
                [*tbl],
                |row| row.get(0),
            )
            .with_context(|| format!("table `{tbl}` not found in source db"))?;

        let normalized = normalize_create_table(&sql)?;
        ddls.push(normalized);
    }

    Ok(ddls)
}

pub fn fetch_index_ddls(conn: &Connection, tables: &[&str]) -> anyhow::Result<Vec<String>> {
    let mut ddls = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT sql FROM sqlite_master
         WHERE type='index' AND tbl_name = ?1 AND sql IS NOT NULL",
    )?;

    for tbl in tables {
        let rows = stmt.query_map([*tbl], |row| row.get::<_, String>(0))?;
        for ddl in rows {
            let ddl = ddl?;
            let ddl = normalize_create_index(&ddl)?;
            ddls.push(ddl);
        }
    }

    Ok(ddls)
}

/// Generates a SQL query for merging data between tables with conflict handling.
///
/// Creates an `INSERT INTO ... SELECT ... ON CONFLICT(id) DO NOTHING` query that
/// copies all rows from the source table to the target table, skipping records
/// with an existing `id`.
pub fn create_merge_query(
    conn: &Connection,
    insert_table: &str,
    select_table: &str,
) -> anyhow::Result<String> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({insert_table})"))?;

    let columns: Vec<String> = stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            Ok(name)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("Failed to fetch column names")?;

    if columns.is_empty() {
        anyhow::bail!("Table '{insert_table}' has no columns or does not exist");
    }

    // let select_list = columns.join(", ");

    // exclude `rowid` value
    let columns_list = columns
        .iter()
        .filter_map(|col| (!col.eq_ignore_ascii_case("rowid")).then_some(col.as_str()))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "INSERT OR IGNORE INTO {insert_table} ({columns_list}) SELECT {columns_list} FROM {select_table}"
    );

    Ok(query)
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;

    #[test]
    fn test_create_merge_query() -> anyhow::Result<()> {
        let conn = Connection::open_in_memory()?;

        conn.execute(
            "CREATE TABLE blocks (
                rowid INTEGER PRIMARY KEY,
                id TEXT NOT NULL UNIQUE,
                seq_no INTEGER
            )",
            [],
        )?;

        let query = create_merge_query(&conn, "blocks", "src0.blocks")?;

        assert_eq!(
            query,
            "INSERT OR IGNORE INTO blocks (id, seq_no) SELECT id, seq_no FROM src0.blocks"
        );

        Ok(())
    }

    #[test]
    fn test_nonexistent_table() {
        let conn = Connection::open_in_memory().unwrap();

        let result = create_merge_query(&conn, "nonexistent", "src0.blocks");
        assert!(result.is_err());
    }
}
