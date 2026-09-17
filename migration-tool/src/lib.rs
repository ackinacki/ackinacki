// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::borrow::Cow;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use include_dir::include_dir;
use include_dir::Dir;
use indoc::formatdoc;
use rusqlite::Connection;
use rusqlite_migration::Migrations;

pub struct DbInfo {
    name: Cow<'static, str>,
    migrations: Dir<'static>,
}

impl DbInfo {
    pub const BM_ARCHIVE: Self =
        Self { name: Cow::Borrowed("bm-schema.db"), migrations: Self::BM_ARCHIVE_MIGRATIONS };
    pub const BM_ARCHIVE_MIGRATIONS: Dir<'static> =
        include_dir!("$CARGO_MANIFEST_DIR/migrations/bm-archive");

    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            name: Cow::Owned(db_path.as_ref().to_string_lossy().into_owned()),
            migrations: Self::BM_ARCHIVE_MIGRATIONS,
        }
    }
}

pub struct DbMaintenance {
    pub path: PathBuf,
    pub info: &'static DbInfo,
}

#[derive(Default, Clone)]
pub struct DbMaintenanceOptions {
    pub silent: bool,
    pub no_journal: bool,
    pub no_check_constraints: bool,
}

impl DbMaintenance {
    pub fn migrate_all_to_latest(
        db_dir: impl AsRef<Path>,
        options: DbMaintenanceOptions,
    ) -> anyhow::Result<()> {
        {
            let db = &DbInfo::BM_ARCHIVE;
            DbMaintenance::new(db, &db_dir).migrate(MigrateTo::Latest, options.clone())?;
        }
        Ok(())
    }

    pub fn new(info: &'static DbInfo, db_dir: impl AsRef<Path>) -> Self {
        Self { path: db_dir.as_ref().join(info.name.as_ref()), info }
    }

    fn migrate_up_nocheck(
        &self,
        conn: &mut Connection,
        from: usize,
        to: usize,
    ) -> anyhow::Result<()> {
        let mut dirs: Vec<_> = self.info.migrations.dirs().collect();
        dirs.sort_by_key(|d| d.path().to_string_lossy().to_string());

        let tx = conn.transaction()?;
        for version in from..to {
            let dir = dirs.get(version).with_context(|| {
                format!("migration directory not found for version {}", version + 1)
            })?;
            // Lexicographic order is only a proxy for the numeric one: a stray or
            // missing directory would shift every index, apply the wrong SQL and
            // then stamp the requested version on top of it.
            let prefix = dir
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.split_once('-'))
                .and_then(|(p, _)| p.parse::<usize>().ok());
            anyhow::ensure!(
                prefix == Some(version + 1),
                "migration directory {:?} does not carry version {}",
                dir.path(),
                version + 1
            );

            let sql = if let Some(f) = dir.get_file(dir.path().join("up-nocheck.sql")) {
                f.contents_utf8().with_context(|| {
                    format!("invalid utf8 in up-nocheck.sql for version {}", version + 1)
                })?
            } else {
                let f = dir
                    .get_file(dir.path().join("up.sql"))
                    .with_context(|| format!("up.sql not found for version {}", version + 1))?;
                f.contents_utf8().with_context(|| {
                    format!("invalid utf8 in up.sql for version {}", version + 1)
                })?
            };

            tx.execute_batch(sql)
                .with_context(|| format!("failed to execute migration version {}", version + 1))?;
        }
        tx.pragma_update(None, "user_version", to as u32)?;
        tx.commit().context("failed to commit migration")?;
        Ok(())
    }

    pub fn migrate(
        &self,
        migrate_to: MigrateTo,
        options: DbMaintenanceOptions,
    ) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("db path {:?}", self.path))?;
        }

        let mut conn = Connection::open(self.path.clone())?;
        // The vendored SQLite is a plain `bundled` build (SQLITE_TEMP_STORE=1),
        // so temp tables and sort spills already go to disk, under SQLITE_TMPDIR.
        // What the cache does control is the sorter's in-memory run (PMA) size,
        // which SQLite caps at 512 MiB: with the 2 MB default a CREATE INDEX over
        // a multi-billion-row table writes thousands of tiny runs and spends
        // hours merging them back; 1 GiB lets it work at the cap.
        conn.pragma_update(None, "cache_size", "-1048576")?; // ~1 GiB
                                                             // Never default to 0: open() is lazy, so on a locked or non-SQLite file
                                                             // the read fails and 0 would masquerade as "fresh DB, migrate from
                                                             // scratch" — the same trap already fixed in bm-archive-processor.
        let current_version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .with_context(|| format!("failed to read user_version from {:?}", self.path))?;

        let migrations = Migrations::from_directory(&self.info.migrations)?;
        migrations.validate()?;
        let latest_migrations_version = get_latest_migration_version(&self.info.migrations)?;

        if !options.silent {
            let info = formatdoc!(
                r"
                {} db migration
                    path: {}
                    current version: {current_version}
                    latest version: {latest_migrations_version}
                ",
                self.info.name,
                self.path.canonicalize()?.display()
            );
            print!("{info}");
        }

        let migrate_to_number = resolve_version(&migrate_to, latest_migrations_version)?;
        if current_version == migrate_to_number {
            if !options.silent {
                println!("    up to date");
            }
        } else if !matches!(migrate_to, MigrateTo::None) {
            if !options.silent {
                if current_version < migrate_to_number {
                    print!("    upgrading to {migrate_to_number}... ",);
                } else {
                    print!("    downgrading to {migrate_to_number}... ",);
                }
                if options.no_check_constraints {
                    print!("(no-check-constraints) ");
                }
            }
            // Only here, once a migration is actually going to run: journal_mode
            // is persisted in the file header, so applying it earlier would knock
            // an up-to-date live database out of WAL on an idempotent re-run.
            if options.no_journal {
                force_delete_journal(&conn, &self.path)?;
            }
            if options.no_check_constraints && current_version < migrate_to_number {
                self.migrate_up_nocheck(
                    &mut conn,
                    current_version as usize,
                    migrate_to_number as usize,
                )?;
            } else {
                migrations.to_version(&mut conn, migrate_to_number as usize)?;
            }
            if !options.silent {
                println!("done.");
            }
        }
        if !options.silent {
            println!();
        }

        Ok(())
    }
}

/// Switches the database to `journal_mode=DELETE` and verifies the switch took.
///
/// The pragma answers with the mode now in effect instead of failing: if the
/// WAL cannot be reset (another connection holds it), SQLite reports the old
/// mode with `SQLITE_OK`, and `execute_batch` would throw that answer away —
/// leaving a cold archive with its WAL2 header intact and unreadable for stock
/// SQLite consumers.
pub fn force_delete_journal(conn: &Connection, path: &Path) -> anyhow::Result<()> {
    let mode: String = conn
        .pragma_update_and_check(None, "journal_mode", "DELETE", |row| row.get(0))
        .with_context(|| format!("failed to set journal_mode=DELETE on {}", path.display()))?;
    anyhow::ensure!(
        mode.eq_ignore_ascii_case("delete"),
        "journal_mode=DELETE refused on {}: database still reports {mode:?}",
        path.display()
    );
    Ok(())
}

pub enum MigrateTo {
    None,
    Latest,
    Version(u32),
}

impl TryFrom<Option<String>> for MigrateTo {
    type Error = anyhow::Error;

    fn try_from(value: Option<String>) -> Result<Self, Self::Error> {
        match value.as_deref() {
            Some("latest") => Ok(Self::Latest),
            Some(s) => match s.parse() {
                Ok(number) => Ok(Self::Version(number)),
                Err(_err) => anyhow::bail!("incorrect version number"),
            },
            None => Ok(Self::None),
        }
    }
}

fn resolve_version(migrate_to: &MigrateTo, latest_migrations_version: u32) -> anyhow::Result<u32> {
    let version = match migrate_to {
        MigrateTo::Latest | MigrateTo::None => latest_migrations_version,
        MigrateTo::Version(number) => {
            if *number > latest_migrations_version {
                anyhow::bail!(
                    "unavailable version number {}. The highest version number is {latest_migrations_version}", *number
                );
            };
            *number
        }
    };
    Ok(version)
}

fn get_latest_migration_version(migrations_dir: &'static Dir<'static>) -> anyhow::Result<u32> {
    let version = migrations_dir.dirs().fold(i32::MIN, |a, b| {
        if let Some(dir_name) = b.path().to_str() {
            if let Some((prefix, _)) = dir_name.split_once('-') {
                if let Ok(b) = prefix.parse() {
                    return a.max(b);
                }
            }
        }
        a
    });

    Ok(version.try_into()?)
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use testdir::testdir;

    use super::*;

    lazy_static! {
        static ref MIGRATIONS_BM: Migrations<'static> =
            Migrations::from_directory(&DbInfo::BM_ARCHIVE.migrations).unwrap();
    }

    #[test]
    fn migrations_test() {
        assert!(MIGRATIONS_BM.validate().is_ok());
    }

    fn object_exists(
        conn: &Connection,
        object_type: &str,
        object_name: &str,
    ) -> rusqlite::Result<bool> {
        conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2
            )",
            [object_type, object_name],
            |row| row.get(0),
        )
    }

    fn column_exists(
        conn: &Connection,
        table_name: &str,
        column_name: &str,
    ) -> rusqlite::Result<bool> {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({table_name})"))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            if name == column_name {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn query_plan(conn: &Connection, sql: &str) -> rusqlite::Result<String> {
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let details = stmt
            .query_map([], |row| row.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(details.join("\n"))
    }

    fn insert_test_block(
        conn: &Connection,
        id: &str,
        block_merkle_leaves_len: usize,
    ) -> rusqlite::Result<usize> {
        let block_merkle_leaves = vec![0u8; block_merkle_leaves_len];
        conn.execute(
            "INSERT INTO blocks (id, status, seq_no, parent, block_merkle_leaves)
             VALUES (?1, 2, 1, 'parent', ?2)",
            (id, block_merkle_leaves),
        )
    }

    #[test]
    fn bm_archive_v3_migration_up_and_down_creates_and_drops_new_objects() -> anyhow::Result<()> {
        let root = testdir!();
        let db = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &root);
        let opts = DbMaintenanceOptions { silent: true, ..Default::default() };

        db.migrate(MigrateTo::Version(2), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(!object_exists(&conn, "table", "bk_set_updates")?);
        assert!(!object_exists(&conn, "table", "attestations")?);
        drop(conn);

        db.migrate(MigrateTo::Version(3), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(object_exists(&conn, "table", "bk_set_updates")?);
        assert!(object_exists(&conn, "table", "attestations")?);
        assert!(object_exists(&conn, "index", "idx_bk_set_updates_chain_order")?);
        assert!(object_exists(&conn, "index", "idx_bk_set_updates_thread_height")?);
        assert!(object_exists(&conn, "index", "idx_attestations_block_id")?);
        assert!(object_exists(&conn, "index", "idx_attestations_source_chain_order")?);
        assert!(object_exists(&conn, "index", "index_transactions_chain_order")?);
        drop(conn);

        db.migrate(MigrateTo::Version(2), opts)?;
        let conn = Connection::open(&db.path)?;
        assert!(!object_exists(&conn, "table", "bk_set_updates")?);
        assert!(!object_exists(&conn, "table", "attestations")?);
        assert!(!object_exists(&conn, "index", "idx_bk_set_updates_chain_order")?);
        assert!(!object_exists(&conn, "index", "idx_bk_set_updates_thread_height")?);
        assert!(!object_exists(&conn, "index", "idx_attestations_block_id")?);
        assert!(!object_exists(&conn, "index", "idx_attestations_source_chain_order")?);
        assert!(!object_exists(&conn, "index", "index_transactions_chain_order")?);

        Ok(())
    }

    #[test]
    fn bm_archive_v5_migration_up_and_down_creates_and_drops_events_index() -> anyhow::Result<()> {
        let root = testdir!();
        let db = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &root);
        let opts = DbMaintenanceOptions { silent: true, ..Default::default() };

        db.migrate(MigrateTo::Version(4), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(!object_exists(&conn, "index", "index_messages_ext_out_msg_chain_order")?);
        drop(conn);

        db.migrate(MigrateTo::Version(5), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(object_exists(&conn, "index", "index_messages_ext_out_msg_chain_order")?);
        drop(conn);

        db.migrate(MigrateTo::Version(4), opts)?;
        let conn = Connection::open(&db.path)?;
        assert!(!object_exists(&conn, "index", "index_messages_ext_out_msg_chain_order")?);

        Ok(())
    }

    #[test]
    fn bm_archive_v6_migration_up_and_down_creates_and_drops_new_indexes() -> anyhow::Result<()> {
        let root = testdir!();
        let db = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &root);
        let opts = DbMaintenanceOptions { silent: true, ..Default::default() };

        db.migrate(MigrateTo::Version(5), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(!object_exists(&conn, "index", "index_attestations_source_block_id")?);
        assert!(!object_exists(&conn, "index", "index_blocks_thread_chain_order")?);
        drop(conn);

        db.migrate(MigrateTo::Version(6), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(object_exists(&conn, "index", "index_attestations_source_block_id")?);
        assert!(object_exists(&conn, "index", "index_blocks_thread_chain_order")?);
        drop(conn);

        db.migrate(MigrateTo::Version(5), opts)?;
        let conn = Connection::open(&db.path)?;
        assert!(!object_exists(&conn, "index", "index_attestations_source_block_id")?);
        assert!(!object_exists(&conn, "index", "index_blocks_thread_chain_order")?);

        Ok(())
    }

    #[test]
    fn bm_archive_v8_migration_adds_final_poseidon_columns() -> anyhow::Result<()> {
        let root = testdir!();
        let db = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &root);
        let opts = DbMaintenanceOptions { silent: true, ..Default::default() };

        db.migrate(MigrateTo::Version(7), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(!column_exists(&conn, "blocks", "block_merkle_leaves")?);
        assert!(!column_exists(&conn, "blocks", "history_proofs")?);
        assert!(!column_exists(&conn, "blocks", "tracked_ext_out_messages_root")?);
        assert!(!column_exists(&conn, "blocks", "tracked_ext_out_message_hashes")?);
        assert!(!column_exists(&conn, "blocks", "proof_block_refs")?);
        drop(conn);

        db.migrate(MigrateTo::Version(8), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(column_exists(&conn, "blocks", "block_merkle_leaves")?);
        assert!(column_exists(&conn, "blocks", "history_proofs")?);
        assert!(column_exists(&conn, "blocks", "tracked_ext_out_messages_root")?);
        assert!(column_exists(&conn, "blocks", "tracked_ext_out_message_hashes")?);
        assert!(column_exists(&conn, "blocks", "proof_block_refs")?);
        insert_test_block(&conn, "new-512", 512)?;
        assert!(insert_test_block(&conn, "too-old-256", 256).is_err());
        drop(conn);

        db.migrate(MigrateTo::Version(7), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(!column_exists(&conn, "blocks", "block_merkle_leaves")?);
        assert!(!column_exists(&conn, "blocks", "history_proofs")?);
        assert!(!column_exists(&conn, "blocks", "tracked_ext_out_messages_root")?);
        assert!(!column_exists(&conn, "blocks", "tracked_ext_out_message_hashes")?);
        assert!(!column_exists(&conn, "blocks", "proof_block_refs")?);
        drop(conn);

        db.migrate(MigrateTo::Latest, opts)?;
        let conn = Connection::open(&db.path)?;
        let current_version: u32 =
            conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(current_version, 10);

        Ok(())
    }

    #[test]
    fn bm_archive_v9_migration_indexes_events_by_src_dapp_id_and_cursor() -> anyhow::Result<()> {
        let root = testdir!();
        let db = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &root);
        let opts = DbMaintenanceOptions { silent: true, ..Default::default() };

        db.migrate(MigrateTo::Version(8), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(!object_exists(
            &conn,
            "index",
            "index_messages_events_src_dapp_id_msg_chain_order"
        )?);
        drop(conn);

        db.migrate(MigrateTo::Version(9), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(object_exists(
            &conn,
            "index",
            "index_messages_events_src_dapp_id_msg_chain_order"
        )?);
        let plan = query_plan(
            &conn,
            "SELECT id FROM messages \
             WHERE msg_type IN (2,4) AND src_dapp_id = 'dapp-id' \
             AND msg_chain_order > '0001' \
             ORDER BY msg_chain_order LIMIT 10",
        )?;
        assert!(
            plan.contains("index_messages_events_src_dapp_id_msg_chain_order"),
            "unexpected events query plan: {plan}"
        );
        drop(conn);

        db.migrate(MigrateTo::Version(8), opts)?;
        let conn = Connection::open(&db.path)?;
        assert!(!object_exists(
            &conn,
            "index",
            "index_messages_events_src_dapp_id_msg_chain_order"
        )?);

        Ok(())
    }

    fn journal_mode(path: &Path) -> anyhow::Result<String> {
        let conn = Connection::open(path)?;
        Ok(conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?)
    }

    #[test]
    fn no_check_constraints_applies_the_nocheck_variant_and_stamps_the_version(
    ) -> anyhow::Result<()> {
        let root = testdir!();
        let db = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &root);
        let opts = DbMaintenanceOptions { silent: true, ..Default::default() };
        db.migrate(MigrateTo::Version(7), opts.clone())?;

        db.migrate(
            MigrateTo::Version(8),
            DbMaintenanceOptions { no_check_constraints: true, ..opts.clone() },
        )?;

        // The same step with CHECKs, for comparison.
        let checked = DbMaintenance::new(&DbInfo::BM_ARCHIVE, root.join("checked"));
        checked.migrate(MigrateTo::Version(8), opts)?;

        let blocks_ddl = |path: &Path| -> anyhow::Result<String> {
            let conn = Connection::open(path)?;
            Ok(conn.query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'blocks'",
                [],
                |row| row.get(0),
            )?)
        };
        let conn = Connection::open(&db.path)?;
        let version: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, 8);
        assert!(column_exists(&conn, "blocks", "block_merkle_leaves")?);
        assert!(column_exists(&conn, "blocks", "proof_block_refs")?);
        drop(conn);

        // Earlier migrations carry CHECKs of their own, so look for the ones
        // 008 adds rather than for the keyword.
        let poseidon_check = "length(block_merkle_leaves)";
        assert!(blocks_ddl(&checked.path)?.contains(poseidon_check));
        let ddl = blocks_ddl(&db.path)?;
        assert!(!ddl.contains(poseidon_check), "nocheck variant must skip 008's CHECKs: {ddl}");
        assert!(!ddl.contains("length(history_proofs)"), "{ddl}");
        Ok(())
    }

    #[test]
    fn no_journal_only_touches_the_journal_when_a_migration_runs() -> anyhow::Result<()> {
        let root = testdir!();
        let db = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &root);
        let opts = DbMaintenanceOptions { silent: true, ..Default::default() };
        db.migrate(MigrateTo::Version(9), opts.clone())?;
        Connection::open(&db.path)?.execute_batch("PRAGMA journal_mode=WAL")?;
        assert_eq!(journal_mode(&db.path)?, "wal");

        // An idempotent cron/Ansible re-run on an up-to-date database, and a
        // plain inspection: neither may knock a live database out of WAL.
        let no_journal = DbMaintenanceOptions { no_journal: true, ..opts };
        db.migrate(MigrateTo::Version(9), no_journal.clone())?;
        assert_eq!(journal_mode(&db.path)?, "wal");
        db.migrate(MigrateTo::None, no_journal.clone())?;
        assert_eq!(journal_mode(&db.path)?, "wal");

        db.migrate(MigrateTo::Version(10), no_journal)?;
        assert_eq!(journal_mode(&db.path)?, "delete");
        Ok(())
    }

    #[test]
    fn bm_archive_v10_migration_adds_transaction_currency_delta_columns() -> anyhow::Result<()> {
        let root = testdir!();
        let db = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &root);
        let opts = DbMaintenanceOptions { silent: true, ..Default::default() };

        db.migrate(MigrateTo::Version(9), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(!column_exists(&conn, "transactions", "balance_delta_other")?);
        assert!(!column_exists(&conn, "transactions", "total_fees_other")?);
        drop(conn);

        db.migrate(MigrateTo::Version(10), opts.clone())?;
        let conn = Connection::open(&db.path)?;
        assert!(column_exists(&conn, "transactions", "balance_delta_other")?);
        assert!(column_exists(&conn, "transactions", "total_fees_other")?);
        drop(conn);

        db.migrate(MigrateTo::Version(9), opts)?;
        let conn = Connection::open(&db.path)?;
        assert!(!column_exists(&conn, "transactions", "balance_delta_other")?);
        assert!(!column_exists(&conn, "transactions", "total_fees_other")?);

        Ok(())
    }
}
