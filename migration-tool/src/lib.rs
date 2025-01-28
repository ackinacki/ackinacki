// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

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
    name: &'static str,
    migrations: Dir<'static>,
}

impl DbInfo {
    pub const BM_ARCHIVE: Self = Self {
        name: "bm-archive",
        migrations: include_dir!("$CARGO_MANIFEST_DIR/migrations/bm-archive"),
    };
    pub const NODE: Self =
        Self { name: "node", migrations: include_dir!("$CARGO_MANIFEST_DIR/migrations/node") };
    pub const NODE_ARCHIVE: Self = Self {
        name: "node-archive",
        migrations: include_dir!("$CARGO_MANIFEST_DIR/migrations/node-archive"),
    };
}

pub struct DbMaintenance {
    pub path: PathBuf,
    pub info: &'static DbInfo,
}

impl DbMaintenance {
    pub fn new(info: &'static DbInfo, db_dir: impl AsRef<Path>) -> Self {
        Self { path: db_dir.as_ref().join(format!("{}.db", info.name)), info }
    }

    pub fn migrate(&self, migrate_to: MigrateTo) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("db path {:?}", self.path))?;
        }

        let mut conn = Connection::open(self.path.clone())?;
        let current_version =
            conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap_or(0);

        let migrations = Migrations::from_directory(&self.info.migrations)?;
        migrations.validate()?;
        let latest_migrations_version = get_latest_migration_version(&self.info.migrations)?;

        let info = formatdoc!(
            r"
            {}
                db path: {}
                schema version: {current_version}
                latest migration version: {latest_migrations_version}
            ",
            self.info.name,
            self.path.canonicalize()?.display()
        );
        println!("{info}");

        let migrate_to_number = resolve_version(&migrate_to, latest_migrations_version)?;
        if current_version == migrate_to_number {
            println!("`{}.db` is up to date", self.info.name);
        } else if !matches!(migrate_to, MigrateTo::None) {
            if current_version < migrate_to_number {
                print!(
                    "Upgrading `{}.db` to the schema version {:?}... ",
                    migrate_to_number, self.info.name
                );
            } else {
                print!(
                    "Downgrading `{}.db` to the schema version {:?}... ",
                    migrate_to_number, self.info.name
                );
            }
            migrations.to_version(&mut conn, migrate_to_number as usize)?;
            println!("done.");
        }

        Ok(())
    }
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

    use super::*;

    lazy_static! {
        static ref MIGRATIONS: Migrations<'static> =
            Migrations::from_directory(&DbInfo::NODE.migrations).unwrap();
        static ref MIGRATIONS_ARC: Migrations<'static> =
            Migrations::from_directory(&DbInfo::NODE_ARCHIVE.migrations).unwrap();
        static ref MIGRATIONS_BM: Migrations<'static> =
            Migrations::from_directory(&DbInfo::BM_ARCHIVE.migrations).unwrap();
    }

    #[test]
    fn migrations_test() {
        assert!(MIGRATIONS.validate().is_ok());
        assert!(MIGRATIONS_ARC.validate().is_ok());
        assert!(MIGRATIONS_BM.validate().is_ok());
    }
}
