// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use async_graphql::dataloader::Loader;
use async_graphql::Error;
use futures::TryStreamExt;
use sqlx::QueryBuilder;
use sqlx::SqlitePool;

use crate::schema::db;

pub struct TransactionLoader {
    pub pool: SqlitePool,
}

impl Loader<String> for TransactionLoader {
    type Error = Error;
    type Value = super::Transaction;

    async fn load(
        &self,
        keys: &[String],
    ) -> anyhow::Result<HashMap<String, Self::Value>, Self::Error> {
        let ids = keys.iter().map(|m| format!("{m:?}")).collect::<Vec<_>>().join(",");
        let sql = format!("SELECT * FROM transactions WHERE id IN ({ids})");
        tracing::trace!(target: "data_loader",  "SQL: {sql}");
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let messages = builder
            .build_query_as()
            .fetch(&self.pool)
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
