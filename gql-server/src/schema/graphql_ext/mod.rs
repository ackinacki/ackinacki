// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;
use std::sync::Arc;

use async_graphql::dataloader::DataLoader;
use async_graphql::Context;
use async_graphql::Enum;
use async_graphql::FieldResult;
use async_graphql::InputObject;
use async_graphql::Object;
use message::MessageLoader;
mod account;
pub mod blockchain_api;

use self::blockchain_api::BlockchainQuery;
use self::message::Message;
use self::message::MessageFilter;
use super::db;
use crate::helpers::query_order_by_str;
use crate::schema::db::DBConnector;
use crate::schema::graphql::account::Account;
use crate::schema::graphql::account::AccountFilter;
use crate::schema::graphql::block::Block;
use crate::schema::graphql::block::BlockFilter;
use crate::schema::graphql::info::Info;
use crate::schema::graphql::message;
use crate::schema::graphql::transaction::Transaction;
use crate::schema::graphql::transaction::TransactionFilter;
use crate::schema::graphql::transaction::TransactionLoader;
use crate::schema::graphql_ext::account::AccountQuery;
use crate::schema::graphql_shared::filter::WhereOp;

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Enum, Copy, Clone, Eq, PartialEq)]
/// Specify sort order direction
pub enum QueryOrderByDirection {
    /// Documents will be sorted in ascended order (e.g. from A to Z)
    ASC,
    /// Documents will be sorted in descendant order (e.g. from Z to A)
    DESC,
}

impl fmt::Display for QueryOrderByDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(InputObject, Debug)]
/// Specify how to sort results. You can sort documents in result set using more
/// than one field.
pub struct QueryOrderBy {
    /// Path to field which must be used as a sort criteria. If field resides
    /// deep in structure path items must be separated with dot (e.g.
    /// "foo.bar.baz").
    pub path: Option<String>,
    /// Sort order direction
    pub direction: Option<QueryOrderByDirection>,
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn info(&self, ctx: &Context<'_>) -> FieldResult<Option<Info>> {
        tracing::info!("info query");
        let db_connector = ctx.data::<Arc<DBConnector>>()?;

        let gen_utime = if ctx.look_ahead().field("lastBlockTime").exists() {
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
        _timeout: Option<f64>,
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

    async fn accounts(
        &self,
        ctx: &Context<'_>,
        filter: Option<AccountFilter>,
        order_by: Option<Vec<Option<QueryOrderBy>>>,
        limit: Option<i32>,
        _timeout: Option<f64>,
    ) -> FieldResult<Option<Vec<Option<Account>>>> {
        let db_connector = ctx.data::<Arc<DBConnector>>()?;
        let filter = match filter {
            Some(f) => AccountFilter::to_where(&f).unwrap_or("".to_string()),
            None => "".to_string(),
        };
        let order_by_clause = query_order_by_str(order_by);
        let db_accounts: Vec<db::Account> =
            db::Account::list(db_connector, filter, order_by_clause, limit).await?;
        let accounts: Vec<Option<Account>> =
            db_accounts.into_iter().map(|b| Some(b.into())).collect();

        Ok(Some(accounts))
    }

    async fn messages(
        &self,
        ctx: &Context<'_>,
        filter: Option<MessageFilter>,
        order_by: Option<Vec<Option<QueryOrderBy>>>,
        limit: Option<i32>,
        _timeout: Option<f64>,
    ) -> FieldResult<Option<Vec<Option<Message>>>> {
        let db_connector = ctx.data::<Arc<DBConnector>>()?;
        let filter = match filter {
            Some(f) => MessageFilter::to_where(&f).unwrap_or("".to_string()),
            None => "".to_string(),
        };
        let order_by_clause = query_order_by_str(order_by);
        let db_messages: Vec<db::Message> =
            db::Message::list(db_connector, filter, order_by_clause, limit).await?;
        let mut messages: Vec<Option<Message>> =
            db_messages.into_iter().map(|b| Some(b.into())).collect();

        let transaction_loader = ctx.data_unchecked::<DataLoader<TransactionLoader>>();
        if ctx.look_ahead().field("src_transaction").exists() {
            for message in messages.iter_mut().flatten() {
                if let Some(transaction_id) = &message.transaction_id {
                    message.src_transaction = transaction_loader
                        .load_one(transaction_id.to_string())
                        .await
                        .unwrap_or_else(|_| panic!("Failed to load transaction: {transaction_id}"))
                        .map(Box::new);
                }
            }
        }
        if ctx.look_ahead().field("dst_transaction").exists() {
            for message in messages.iter_mut().flatten() {
                let dst_transaction =
                    db::transaction::Transaction::by_in_message(db_connector, &message.id, None)
                        .await
                        .expect("Failed to load transaction by inbound message");

                if let Some(transaction) = dst_transaction {
                    message.dst_transaction = transaction_loader
                        .load_one(transaction.id.clone())
                        .await
                        .unwrap_or_else(|_| {
                            panic!("Failed to load transaction: {}", transaction.id)
                        })
                        .map(Box::new);
                }
            }
        }

        Ok(Some(messages))
    }

    async fn transactions(
        &self,
        ctx: &Context<'_>,
        filter: Option<TransactionFilter>,
        order_by: Option<Vec<Option<QueryOrderBy>>>,
        limit: Option<i32>,
        _timeout: Option<f64>,
    ) -> FieldResult<Option<Vec<Option<Transaction>>>> {
        let db_connector = ctx.data::<Arc<DBConnector>>()?;
        let filter = match filter {
            Some(f) => TransactionFilter::to_where(&f).unwrap_or("".to_string()),
            None => "".to_string(),
        };
        let order_by_clause = query_order_by_str(order_by);
        let db_transactions: Vec<db::Transaction> =
            db::Transaction::list(db_connector, filter, order_by_clause, limit).await?;
        let mut transactions: Vec<Option<Transaction>> =
            db_transactions.into_iter().map(|b| Some(b.into())).collect();

        let message_loader = ctx.data_unchecked::<DataLoader<MessageLoader>>();
        if ctx.look_ahead().field("in_message").exists() {
            for transaction in transactions.iter_mut().flatten() {
                transaction.in_message =
                    message_loader.load_one(transaction.in_msg.clone()).await?;
            }
        }
        if ctx.look_ahead().field("out_messages").exists() {
            for transaction in transactions.iter_mut().flatten() {
                let out_messages = message_loader.load_many(transaction.out_msgs.clone()).await?;
                transaction.out_messages = Some(out_messages.into_values().map(Some).collect());
            }
        }
        Ok(Some(transactions))
    }

    /// Blockchain-related information (blocks, transactions, etc.)
    async fn blockchain<'ctx>(
        &'ctx self,
        ctx: &'ctx Context<'ctx>,
    ) -> FieldResult<Option<BlockchainQuery<'ctx>>> {
        Ok(Some(BlockchainQuery { ctx }))
    }
}
