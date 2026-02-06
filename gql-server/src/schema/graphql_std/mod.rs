// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use async_graphql::dataloader::DataLoader;
use async_graphql::Context;
use async_graphql::FieldResult;
use async_graphql::Object;

mod account;
pub(crate) mod events;

use crate::helpers::query_order_by_str;
use crate::schema::db;
use crate::schema::db::DBConnector;
use crate::schema::graphql::block::Block;
use crate::schema::graphql::block::BlockFilter;
use crate::schema::graphql::filter::WhereOp;
use crate::schema::graphql::message::MessageLoader;
use crate::schema::graphql_ext::QueryOrderBy;
use crate::schema::graphql_shared::info::Info;
use crate::schema::graphql_std::account::AccountQuery;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn info(&self, ctx: &Context<'_>) -> FieldResult<Option<Info>> {
        tracing::info!("info query");
        let db_connector = ctx.data::<Arc<DBConnector>>()?;

        let gen_utime = if ctx.look_ahead().field("last_block_time").exists() {
            let block = db::Block::latest_block(db_connector).await?;
            match block {
                Some(db::Block { gen_utime, .. }) => gen_utime,
                None => None,
            }
        } else {
            None
        };
        Ok(Some(Info { last_block_time: Some(gen_utime.unwrap_or(0) as f64), ..Info::default() }))
    }

    async fn account(&self, address: String) -> Option<AccountQuery> {
        Some(AccountQuery { address, preloaded: None })
    }

    async fn blocks(
        &self,
        ctx: &Context<'_>,
        filter: Option<BlockFilter>,
        order_by: Option<Vec<Option<QueryOrderBy>>>,
        limit: Option<i32>,
    ) -> FieldResult<Option<Vec<Option<Block>>>> {
        let db_connector = ctx.data::<Arc<DBConnector>>()?;
        let filter = match filter {
            Some(f) => BlockFilter::to_where(&f).unwrap_or("".to_string()),
            None => "".to_string(),
        };
        let order_by_clause = query_order_by_str(order_by);
        let db_blocks: Vec<db::Block> =
            db::Block::list(db_connector, filter, order_by_clause, limit).await?;
        let mut blocks = db_blocks
            .into_iter()
            .map(|b| Some(Into::<Block>::into(b)))
            .collect::<Vec<Option<Block>>>();

        if ctx.look_ahead().field("in_msg_descr").exists() {
            for b in blocks.iter_mut().flatten() {
                let in_msgs = db::Message::in_block_msgs(db_connector, b.id.to_string()).await?;
                b.set_in_msg_descr(in_msgs);
            }
        }

        if ctx.look_ahead().field("in_msg_descr").exists() {
            let message_loader = ctx.data_unchecked::<DataLoader<MessageLoader>>();
            for b in blocks.iter_mut().flatten() {
                if let Some(out_msgs) = &b.out_msgs {
                    let ids: Vec<String> = serde_json::from_str(out_msgs)?;
                    let _out_msgs = message_loader.load_many(ids).await?;
                }
            }
        }

        // load_blocks_in_messages(pool, &mut blocks.clone()).await?;

        Ok(Some(blocks))
    }
}
