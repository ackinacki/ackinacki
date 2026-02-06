// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::QueryBuilder;
use sqlx::Sqlite;
use sqlx::SqlitePool;

use crate::defaults::MAX_POOL_CONNECTIONS;

pub struct DBConnector {
    pool: ArcSwap<SqlitePool>,
    attachments: ArcSwap<HashMap<String, String>>,
    db_path: PathBuf,
}

impl DBConnector {
    pub fn new(pool: SqlitePool, db_path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            pool: ArcSwap::new(Arc::new(pool)),
            attachments: ArcSwap::new(Arc::new(HashMap::new())),
            db_path,
        })
    }

    pub async fn update_attachments(&self, attachments: Vec<String>) -> anyhow::Result<()> {
        let mut new_attachments = HashMap::new();
        for (index, path) in attachments.iter().enumerate() {
            let attach_name = Path::new(path)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(|stem| stem.to_owned())
                .unwrap_or_else(|| format!("archive{index}"));

            if is_system_db(&attach_name) {
                continue;
            }

            let escaped_name = escape_identifier(&attach_name);
            let escaped_path = escape_literal(&format!("file:{}?mode=ro", path));
            new_attachments.insert(escaped_name, escaped_path);
        }

        self.attachments.store(Arc::new(new_attachments));

        let new_pool = self.create_pool_with_attachments().await?;
        let old_pool = self.pool.swap(Arc::new(new_pool));

        tokio::spawn(async move {
            let pool = (*old_pool).clone();
            pool.close().await;
        });

        Ok(())
    }

    async fn create_pool_with_attachments(&self) -> anyhow::Result<SqlitePool> {
        let db_path_str = self.db_path.display().to_string();
        let connect_string = format!("{db_path_str}?mode=ro");

        let connect_options =
            SqliteConnectOptions::from_str(&connect_string)?.create_if_missing(false);

        let attachments = self.attachments.load_full().clone();

        let pool = SqlitePoolOptions::new()
            .max_connections(MAX_POOL_CONNECTIONS)
            .after_connect(move |conn, _meta| {
                let attachments = attachments.clone();
                Box::pin(async move {
                    for (attach_name, path) in attachments.iter() {
                        let sql = format!("ATTACH DATABASE '{}' AS '{}'", path, attach_name);
                        tracing::trace!(sql);

                        if let Err(err) = QueryBuilder::new(sql).build().execute(&mut *conn).await {
                            tracing::error!("ATTACH failed: {err}");
                        }
                    }
                    Ok(())
                })
            })
            .connect_with(connect_options)
            .await?;

        Ok(pool)
    }

    pub async fn get_connection(&self) -> anyhow::Result<PoolConnection<Sqlite>> {
        let pool = self.pool.load_full();
        Ok(pool.acquire().await?)
    }

    pub fn attached_db_names(&self) -> Vec<String> {
        let mut list: Vec<String> = self.attachments.load_full().keys().cloned().collect();

        list.push("main".to_string());
        list
    }
}

fn is_system_db(name: &str) -> bool {
    matches!(name, "main" | "temp")
}

fn escape_identifier(value: &str) -> String {
    value.replace('"', "\"\"")
}

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::DBConnector;
    use crate::web;

    async fn setup_connector() -> (Arc<DBConnector>, std::path::PathBuf) {
        let root = PathBuf::from("./tests/fixtures/popitgame");
        let db_path = root.join("bm-archive.db");
        let pool = web::open_db(db_path.clone()).await.expect("open `main` DB");
        (DBConnector::new(pool, db_path), root)
    }

    #[tokio::test]
    async fn attach_new_dbs() {
        // crate::helpers::init_tracing();
        let (connector, root) = setup_connector().await;

        let archive1 = root.join("bm-archive-1.db");
        let archive2 = root.join("bm-archive-2.db");
        let archive3 = root.join("bm-archive-3.db");

        let attachments = vec![archive3.to_string_lossy().into_owned()];

        connector.update_attachments(attachments).await.expect("attach new dbs");

        let mut names = connector.attached_db_names();
        names.sort();
        assert_eq!(names, vec!["bm-archive-3".to_string(), "main".to_string()]);

        let attachments = vec![
            archive3.to_string_lossy().into_owned(),
            archive2.to_string_lossy().into_owned(),
            archive1.to_string_lossy().into_owned(),
        ];

        connector.update_attachments(attachments).await.expect("attach new dbs");

        let mut names = connector.attached_db_names();
        names.sort();
        assert_eq!(
            names,
            vec![
                "bm-archive-1".to_string(),
                "bm-archive-2".to_string(),
                "bm-archive-3".to_string(),
                "main".to_string()
            ]
        );

        let _conn = connector.get_connection().await.expect("get conn with attachments");
    }

    #[tokio::test]
    async fn attach_dbs_skips_system_names() {
        let (connector, root) = setup_connector().await;

        let main_db = root.join("main.db");
        let temp_db = root.join("temp.db");
        let normal_db = root.join("bm-archive-1.db");

        let attachments = vec![
            main_db.to_string_lossy().into_owned(),
            temp_db.to_string_lossy().into_owned(),
            normal_db.to_string_lossy().into_owned(),
        ];

        connector.update_attachments(attachments).await.expect("attach dbs");

        let mut names = connector.attached_db_names();
        names.sort();
        assert_eq!(names, vec!["bm-archive-1".to_string(), "main".to_string()]);
    }
}
