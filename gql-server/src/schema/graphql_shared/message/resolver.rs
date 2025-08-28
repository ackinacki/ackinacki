// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use async_graphql::dataloader::Loader;
use async_graphql::Error;
use futures::TryStreamExt;
use sqlx::SqlitePool;

use crate::schema::db;

pub struct MessageLoader {
    pub pool: SqlitePool,
}

impl Loader<String> for MessageLoader {
    type Error = Error;
    type Value = super::Message;

    async fn load(
        &self,
        keys: &[String],
    ) -> anyhow::Result<HashMap<String, Self::Value>, Self::Error> {
        let ids = keys.iter().map(|m| format!("{m:?}")).collect::<Vec<_>>().join(",");
        let sql = format!("SELECT * FROM messages WHERE id IN ({ids})");
        tracing::trace!(target: "data_loader",  "SQL: {sql}");
        let messages = sqlx::query_as(&sql)
            .fetch(&self.pool)
            .map_ok(|msg: db::Message| {
                let message: Self::Value = msg.into();
                let message_id = message.id.clone();
                (message_id, message)
            })
            .try_collect::<HashMap<String, Self::Value>>()
            .await?;

        Ok(messages)
    }
}
