// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fs;
use std::path::PathBuf;

use clap::Parser;
use include_dir::include_dir;
use include_dir::Dir;
use indoc::formatdoc;
use rusqlite::Connection;
use rusqlite_migration::Migrations;

static M_BLOCK_MANAGER: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations/bm-archive");

enum MTarget<'a> {
    BlockManager(Migrations<'a>),
}

/// Acki-Nacki DB (sqlite) migration tool
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The path to the DB file (bm-schema.db). Creates and
    /// inits to the latest version if they are missing
    #[arg(short = 'p')]
    db_path: String,

    /// Migrate the bm-schema.db to the specified DB schema version (default: latest)
    #[arg(long = "block-manager", value_name = "SCHEMA_VERSION", num_args = 0..=1, default_missing_value = "latest")]
    bm: Option<String>,
}

fn parse_version(v: Option<String>, target: MTarget) -> anyhow::Result<u32> {
    let latest = get_latest_migration_version(target)?;
    let version = match v.as_deref() {
        None | Some("latest") => latest,
        Some(x) => match x.parse() {
            Ok(n) => {
                if n > latest {
                    anyhow::bail!(
                        "unavailable version number {n}. The highest version number is {latest}"
                    );
                };
                n
            }
            Err(_err) => anyhow::bail!("incorrect version number"),
        },
    };
    Ok(version)
}

fn get_latest_migration_version(target: MTarget) -> anyhow::Result<u32> {
    let (migrations, migrations_dir) = match target {
        MTarget::BlockManager(m) => (m, M_BLOCK_MANAGER.clone()),
    };
    // check that migration set is correct
    if let Err(err) = migrations.validate() {
        anyhow::bail!(err);
    }
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

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let db_path = PathBuf::from(args.db_path);

    if let Err(err) = fs::create_dir_all(db_path.clone()) {
        eprintln!("Failed to create data dir {db_path:?}: {err}");
        std::process::exit(1);
    }

    let db_file = db_path.join("bm-schema.db");
    let mut conn = Connection::open(db_file.clone())?;
    let current_version =
        conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap_or(0);

    let migrations_block_manager =
        Migrations::from_directory(&M_BLOCK_MANAGER).expect("bm-archive");

    let m_version =
        get_latest_migration_version(MTarget::BlockManager(migrations_block_manager.clone()))?;

    let info = formatdoc!(
        r"
        db path: {}

        bm-schema.db:
          schema version: {current_version}
          latest migration version: {m_version}
    ",
        db_path.canonicalize()?.display()
    );
    println!("{info}");

    if args.bm.is_none() {
        std::process::exit(0);
    }

    let migrate_to =
        parse_version(args.bm.clone(), MTarget::BlockManager(migrations_block_manager.clone()))?;
    if current_version == migrate_to {
        println!("`bm-schema.db` is up to date");
    } else if args.bm.is_some() {
        if current_version < migrate_to {
            println!("Upgrading `bm-schema.db` to the schema version {migrate_to:?}... ");
        } else {
            println!("Downgrading `bm-schema.db` to the schema version {migrate_to:?}... ");
        }
        migrations_block_manager.to_version(&mut conn, migrate_to as usize)?;
        // conn.execute_batch(
        //     "
        //     PRAGMA page_size=16384;
        //     VACUUM;
        //     PRAGMA optimize=0x10002;
        // ")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use super::*;

    lazy_static! {
        static ref MIGRATIONS_BM: Migrations<'static> =
            Migrations::from_directory(&M_BLOCK_MANAGER).unwrap();
    }

    #[test]
    fn migrations_test() {
        assert!(MIGRATIONS_BM.validate().is_ok());
    }
}
