// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fs;
use std::path::PathBuf;

use clap::Parser;
use include_dir::include_dir;
use include_dir::Dir;
use indoc::formatdoc;
use rusqlite::Connection;
use rusqlite_migration::Migrations;

static MIGRATIONS_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations/node");
static M_BLOCK_KEEPER: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations/node-archive");
static M_BLOCK_MANAGER: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations/bm-archive");

enum MTarget<'a> {
    Node(Migrations<'a>),
    BlockKeeper(Migrations<'a>),
    BlockManager(Migrations<'a>),
}

/// Acki-Nacki DB (sqlite) migration tool
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The path to the DB files (node.db and node-archive.db). Creates and
    /// inits to the latest version if they are missing
    #[arg(short = 'p')]
    db_path: String,

    /// [DEPRECATED] Migrate the node.db to the specified DB schema version (default: latest)
    #[arg(short = 'n', long = "node", num_args = 0..=1, default_missing_value = "latest")]
    schema_version: Option<String>,

    /// Migrate the node-archive.db to the specified DB schema version (default:
    /// latest)
    #[arg(short = 'a', long = "archive", num_args = 0..=1, default_missing_value = "latest")]
    arc_schema_version: Option<String>,

    /// Migrate the bm-archive.db to the specified DB schema version (default: latest)
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
        MTarget::Node(m) => (m, MIGRATIONS_DIR.clone()),
        MTarget::BlockKeeper(m) => (m, M_BLOCK_KEEPER.clone()),
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

    let node_db_file = db_path.join("node.db");
    let mut conn = Connection::open(node_db_file.clone())?;
    let current_version =
        conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap_or(0);

    let arc_db_file = db_path.join("node-archive.db");
    let mut conn_arc = Connection::open(arc_db_file.clone())?;
    let current_arc_version =
        conn_arc.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap_or(0);

    let bm_db_file = db_path.join("bm-archive.db");
    let mut conn_bm = Connection::open(bm_db_file.clone())?;
    let current_bm_version =
        conn_bm.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap_or(0);

    let migrations = Migrations::from_directory(&MIGRATIONS_DIR).expect("node");
    let migrations_arc = Migrations::from_directory(&M_BLOCK_KEEPER).expect("node-archive");
    let migrations_block_manager =
        Migrations::from_directory(&M_BLOCK_MANAGER).expect("bm-archive");

    let m_version = get_latest_migration_version(MTarget::Node(migrations.clone()))?;
    let m_arc_version = get_latest_migration_version(MTarget::BlockKeeper(migrations_arc.clone()))?;
    let m_bm_version =
        get_latest_migration_version(MTarget::BlockManager(migrations_block_manager.clone()))?;

    let info = formatdoc!(
        r"
        db path: {}

        node.db:
          schema version: {current_version}
          latest migration version: {m_version}

        node-archive.db:
          schema version: {current_arc_version}
          latest migration version: {m_arc_version}

        bm-archive.db:
          schema version: {current_bm_version}
          latest migration version: {m_bm_version}
    ",
        db_path.canonicalize()?.display()
    );
    println!("{info}");

    if args.schema_version.is_none() && args.arc_schema_version.is_none() && args.bm.is_none() {
        std::process::exit(0);
    }

    let migrate_to = parse_version(args.schema_version.clone(), MTarget::Node(migrations.clone()))?;
    if current_version == migrate_to {
        println!("`node.db` is up to date");
    } else if args.schema_version.is_some() {
        if current_version < migrate_to {
            print!("Upgrading `node.db` to the schema version {migrate_to:?}... ");
        } else {
            print!("Downgrading `node.db` to the schema version {migrate_to:?}... ");
        }
        migrations.to_version(&mut conn, migrate_to as usize)?;
        println!("done.");
    }

    let migrate_arc_to = parse_version(
        args.arc_schema_version.clone(),
        MTarget::BlockKeeper(migrations_arc.clone()),
    )?;
    if current_arc_version == migrate_arc_to {
        println!("`node-archive.db` is up to date");
    } else if args.arc_schema_version.is_some() {
        if current_arc_version < migrate_arc_to {
            print!("Upgrading `node-archive.db` to the schema version {migrate_arc_to:?}... ");
        } else {
            print!("Downgrading `node-archive.db` to the schema version {migrate_arc_to:?}... ");
        }
        migrations_arc.to_version(&mut conn_arc, migrate_arc_to as usize)?;
        println!("done.");
    }

    let migrate_bm_to =
        parse_version(args.bm.clone(), MTarget::BlockManager(migrations_block_manager.clone()))?;
    if current_bm_version == migrate_bm_to {
        println!("`bm-archive.db` is up to date");
    } else if args.bm.is_some() {
        if current_bm_version < migrate_bm_to {
            print!("Upgrading `bm-archive.db` to the schema version {migrate_bm_to:?}... ");
        } else {
            print!("Downgrading `bm-archive.db` to the schema version {migrate_bm_to:?}... ");
        }
        migrations_block_manager.to_version(&mut conn_bm, migrate_bm_to as usize)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use super::*;

    lazy_static! {
        static ref MIGRATIONS: Migrations<'static> =
            Migrations::from_directory(&MIGRATIONS_DIR).unwrap();
        static ref MIGRATIONS_ARC: Migrations<'static> =
            Migrations::from_directory(&M_BLOCK_KEEPER).unwrap();
        static ref MIGRATIONS_BM: Migrations<'static> =
            Migrations::from_directory(&M_BLOCK_MANAGER).unwrap();
    }

    #[test]
    fn migrations_test() {
        assert!(MIGRATIONS.validate().is_ok());
        assert!(MIGRATIONS_ARC.validate().is_ok());
        assert!(MIGRATIONS_BM.validate().is_ok());
    }
}
