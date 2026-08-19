// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_graphql::dataloader::Loader;
use async_graphql::Error;
use futures::TryStreamExt;
use sqlx::QueryBuilder;

use crate::schema::db;
use crate::schema::db::DBConnector;

pub struct TransactionLoader {
    pub db_connector: Arc<DBConnector>,
    /// Cold-storage toggle. When enabled, the `boc` column is omitted from the
    /// projection because it is not stored on cold-storage servers and selecting
    /// it would fail before the field-level guard runs.
    pub cold_storage: Arc<AtomicBool>,
}

impl Loader<String> for TransactionLoader {
    type Error = Error;
    type Value = super::Transaction;

    async fn load(
        &self,
        keys: &[String],
    ) -> anyhow::Result<HashMap<String, Self::Value>, Self::Error> {
        let ids = keys.iter().map(|m| format!("{m:?}")).collect::<Vec<_>>().join(",");

        let db_names = self.db_connector.attached_db_names();

        if db_names.is_empty() {
            return Ok(HashMap::new());
        }

        let include_boc = !self.cold_storage.load(Ordering::Relaxed);
        let mut projection = db::Transaction::graphql_transaction_projection(include_boc);
        projection.add("id");
        let select = projection.select_list();
        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT {select} FROM \"{name}\".transactions WHERE id IN ({ids})"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT {select} FROM ({union_sql})");
        tracing::trace!(target: "data_loader",  "SQL: {sql}");
        let mut conn = self.db_connector.get_connection().await?;
        conn.set_sql(&sql);
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let messages = builder
            .build_query_as()
            .fetch(&mut *conn)
            .map_ok(|transaction: db::Transaction| {
                let transaction: Self::Value = transaction.into();
                let transaction_id = transaction.id.clone();
                (transaction_id, transaction)
            })
            .try_collect::<HashMap<String, Self::Value>>()
            .await?;

        Ok(messages)
    }
}
