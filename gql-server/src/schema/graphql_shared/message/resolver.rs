// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::dataloader::Loader;
use async_graphql::Error;
use futures::TryStreamExt;
use sqlx::QueryBuilder;

use crate::schema::db;
use crate::schema::db::DBConnector;

pub struct MessageLoader {
    pub db_connector: Arc<DBConnector>,
}

impl Loader<String> for MessageLoader {
    type Error = Error;
    type Value = super::Message;

    async fn load(
        &self,
        keys: &[String],
    ) -> anyhow::Result<HashMap<String, Self::Value>, Self::Error> {
        let ids = keys.iter().map(|m| format!("{m:?}")).collect::<Vec<_>>().join(",");

        let db_names = self.db_connector.attached_db_names();

        if db_names.is_empty() {
            return Ok(HashMap::new());
        }

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".messages WHERE id IN ({ids})"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql})");

        tracing::trace!(target: "data_loader",  "SQL: {sql}");
        let mut conn = self.db_connector.get_connection().await?;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let messages = builder
            .build_query_as()
            .fetch(&mut *conn)
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
