// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use async_graphql::dataloader::Loader;
use async_graphql::Error;
use futures::TryStreamExt;
use sqlx::SqlitePool;

use crate::schema::db;

pub struct BlockLoader {
    pub pool: SqlitePool,
}

impl Loader<String> for BlockLoader {
    type Error = Error;
    type Value = super::Block;

    async fn load(
        &self,
        keys: &[String],
    ) -> anyhow::Result<HashMap<String, Self::Value>, Self::Error> {
        let ids = keys.iter().map(|m| format!("{m:?}")).collect::<Vec<_>>().join(",");
        let sql = format!("SELECT * FROM blocks WHERE id IN ({})", ids);
        log::trace!(target: "data_loader",  "SQL: {sql}");
        let messages = sqlx::query_as(&sql)
            .fetch(&self.pool)
            .map_ok(|block: db::Block| {
                let mut block: Self::Value = block.into();
                let block_id = block.id;
                block.id = format!("block/{}", block_id);
                (block_id, block)
            })
            .try_collect::<HashMap<String, Self::Value>>()
            .await?;

        Ok(messages)
    }
}
