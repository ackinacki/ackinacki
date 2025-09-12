// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use async_graphql::connection::ConnectionNameType;
use async_graphql::connection::Edge;
use async_graphql::connection::EdgeNameType;
use async_graphql::connection::EmptyFields;
use async_graphql::dataloader::DataLoader;
use async_graphql::types::connection::query;
use async_graphql::types::connection::Connection;
use async_graphql::Context;
use async_graphql::Enum;
use async_graphql::InputObject;
use async_graphql::Object;
use async_graphql::OutputType;
use sqlx::SqlitePool;
use tvm_client::ClientContext;

use super::transactions::BlockchainTransaction;
use crate::schema::db;
use crate::schema::db::transaction::AccountTransactionsQueryArgs;
use crate::schema::graphql::account::Account;
use crate::schema::graphql::query::PaginationArgs;
use crate::schema::graphql::transaction::Transaction;
use crate::schema::graphql::transaction::TransactionLoader;
use crate::schema::graphql_ext::blockchain_api::transactions::BlockchainTransactionsConnection;
use crate::schema::graphql_ext::blockchain_api::transactions::BlockchainTransactionsEdge;
use crate::schema::graphql_ext::message::Message;

type BlockchainAccount = Account;
type BlockchainMessage = Message;

#[derive(Enum, Clone, Copy, PartialEq, Eq)]
#[graphql(rename_items = "PascalCase")]
pub enum BlockchainMessageTypeFilterEnum {
    /// External inbound
    ExtIn,
    /// External outbound
    ExtOut,
    /// Internal inbound
    IntIn,
    /// Internal outbound
    IntOut,
}

impl From<BlockchainMessageTypeFilterEnum> for u8 {
    fn from(value: BlockchainMessageTypeFilterEnum) -> Self {
        match value {
            BlockchainMessageTypeFilterEnum::ExtIn => 1,
            BlockchainMessageTypeFilterEnum::ExtOut => 2,
            BlockchainMessageTypeFilterEnum::IntIn | BlockchainMessageTypeFilterEnum::IntOut => 0,
        }
    }
}

#[derive(Clone, InputObject)]
pub struct BlockchainMasterSeqNoFilter {
    pub start: Option<i32>,
    pub end: Option<i32>,
}

struct BlockchainMessageEdge;

impl EdgeNameType for BlockchainMessageEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainMessageEdge".to_string()
    }
}

struct BlockchainMessagesConnection;

impl ConnectionNameType for BlockchainMessagesConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainMessagesConnection".to_string()
    }
}

pub struct BlockchainAccountEdge;

impl EdgeNameType for BlockchainAccountEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainAccountEdge".to_string()
    }
}

pub struct BlockchainAccountsConnection;

impl ConnectionNameType for BlockchainAccountsConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainAccountsConnection".to_string()
    }
}

#[derive(Clone)]
pub struct BlockchainAccountQuery<'a> {
    pub ctx: &'a Context<'a>,
    pub address: String,
    pub preloaded: Option<db::Account>,
}

#[Object]
impl BlockchainAccountQuery<'_> {
    /// Account information (e.g. boc).
    pub async fn info(
        &self,
        #[graphql(
            desc = "Optional block hash. If byBlock is specified then the account info will be returned from the shard state defined by the specified block. Otherwise the account info will be returned from the last known shard state.",
            default
        )]
        _by_block: Option<String>,
    ) -> Option<BlockchainAccount> {
        if let Some(preloaded) = &self.preloaded {
            return Some(preloaded.clone().into());
        }
        let pool = self.ctx.data::<SqlitePool>().unwrap();
        let client = self.ctx.data::<Arc<ClientContext>>().unwrap();

        db::Account::by_address(pool, client, Some(self.address.clone()))
            .await
            .unwrap()
            .map(|db_account| db_account.into())
    }

    #[allow(clippy::too_many_arguments)]
    /// This node could be used for a cursor-based pagination of account
    /// messages.
    pub async fn messages(
        &self,
        #[graphql(
            name = "allow_latest_inconsistent_data",
            desc = "By default there is special latency added for the fetched recent data (several seconds) to ensure impossibility of inserts before the latest fetched cursor (data consistency, for reliable pagination). It is possible to disable this guarantee and to reduce the latency of realtime data by setting this flag to true."
        )]
        allow_latest_inconsistent_data: Option<bool>,
        #[graphql(name = "master_seq_no_range")] block_seq_no_range: Option<
            BlockchainMasterSeqNoFilter,
        >,
        #[graphql(
            name = "counterparties",
            desc = "Filter messages by counterparties (max - 5 counterparties)."
        )]
        counterparties: Option<Vec<String>>,
        #[graphql(
            name = "msg_type",
            desc = "Filter messages by type (multiple are allowed, all messages if not specified)."
        )]
        msg_type: Option<Vec<BlockchainMessageTypeFilterEnum>>,
        #[graphql(
            name = "min_value",
            desc = "Optional filter by min value (unoptimized, query could be dropped by timeout)."
        )]
        min_value: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
        #[graphql(
            desc = "Defines query scope. If true then query performed on a maximum time range supported by the cloud. If false then query performed on a recent time range supported by the cloud. You can find an actual information about time ranges on evercloud documentation."
        )]
        _archive: Option<bool>,
    ) -> Option<
        Connection<
            String,
            BlockchainMessage,
            EmptyFields,
            EmptyFields,
            BlockchainMessagesConnection,
            BlockchainMessageEdge,
        >,
    > {
        query(after, before, first, last, |after, before, first, last| async move {
            tracing::debug!(
                "first={:?}, after={:?}, last={:?}, before={:?}",
                first,
                after,
                last,
                before
            );

            let msg_type = match msg_type {
                Some(list) if list.is_empty() => None,
                _ => msg_type,
            };
            let args = db::AccountMessagesQueryArgs::new(
                allow_latest_inconsistent_data,
                block_seq_no_range,
                counterparties,
                msg_type,
                min_value,
                PaginationArgs { first, after, last, before },
            );
            let mut messages = db::Message::account_messages(
                self.ctx.data::<SqlitePool>().unwrap(),
                self.address.clone(),
                &args,
            )
            .await?;

            let (has_previous_page, has_next_page) =
                args.pagination.get_bound_markers(messages.len());
            tracing::debug!("has_previous_page={:?}, after={:?}", has_previous_page, has_next_page);

            let mut connection: Connection<
                String,
                Message,
                EmptyFields,
                EmptyFields,
                BlockchainMessagesConnection,
                BlockchainMessageEdge,
            > = Connection::new(has_previous_page, has_next_page);

            args.pagination.shrink_portion(&mut messages);

            let selection_set = self
                .ctx
                .look_ahead()
                .field("account")
                .field("messages")
                .field("edges")
                .field("node");
            let is_parent_transaction = selection_set.field("src_transaction").exists();
            let is_child_transaction = selection_set.field("dst_transaction").exists();
            let transaction_loader = self.ctx.data_unchecked::<DataLoader<TransactionLoader>>();
            let mut edges: Vec<Edge<String, Message, EmptyFields, BlockchainMessageEdge>> = vec![];
            for message in messages {
                let parent_transaction = message.transaction_id.clone();
                let mut message: BlockchainMessage = message.into();
                if is_parent_transaction {
                    if let Some(parent_transaction) = parent_transaction {
                        message.src_transaction = transaction_loader
                            .load_one(parent_transaction.clone())
                            .await
                            .unwrap_or_else(|_| {
                                panic!("Failed to load transaction: {parent_transaction}")
                            })
                            .map(Box::new);
                    }
                }
                if is_child_transaction {
                    let dst_transaction = db::transaction::Transaction::by_in_message(
                        self.ctx.data::<SqlitePool>().unwrap(),
                        &message.id,
                        None,
                    )
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
                let cursor = message.dst_chain_order.clone().unwrap();
                let edge: Edge<String, Message, EmptyFields, BlockchainMessageEdge> =
                    Edge::with_additional_fields(cursor, message, EmptyFields);
                edges.push(edge);
            }
            connection.edges.extend(edges);
            Ok::<_, async_graphql::Error>(connection)
        })
        .await
        .ok()
    }

    #[allow(clippy::too_many_arguments)]
    /// This node could be used for a cursor-based pagination of account transactions.
    pub async fn transactions(
        &self,
        #[graphql(
            name = "allow_latest_inconsistent_data",
            desc = "By default there is special latency added for the fetched recent data (several seconds) to ensure impossibility of inserts before the latest fetched cursor (data consistency, for reliable pagination). It is possible to disable this guarantee and to reduce the latency of realtime data by setting this flag to true."
        )]
        allow_latest_inconsistent_data: Option<bool>,
        #[graphql(name = "master_seq_no_range")] block_seq_no_range: Option<
            BlockchainMasterSeqNoFilter,
        >,
        aborted: Option<bool>,
        #[graphql(
            name = "min_balance_delta",
            desc = "Optional filter by min balance_delta (unoptimized, query could be dropped by timeout)."
        )]
        min_balance_delta: Option<String>,
        #[graphql(
            name = "max_balance_delta",
            desc = "Optional filter by max balance_delta (unoptimized, query could be dropped by timeout)."
        )]
        max_balance_delta: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
        #[graphql(
            desc = "Defines query scope. If true then query performed on a maximum time range supported by the cloud. If false then query performed on a recent time range supported by the cloud. You can find an actual information about time ranges on evercloud documentation."
        )]
        _archive: Option<bool>,
    ) -> Option<
        Connection<
            String,
            BlockchainTransaction,
            EmptyFields,
            EmptyFields,
            BlockchainTransactionsConnection,
            BlockchainTransactionsEdge,
        >,
    > {
        query(after, before, first, last, |after, before, first, last| async move {
            tracing::debug!(
                "first={:?}, after={:?}, last={:?}, before={:?}",
                first,
                after,
                last,
                before
            );

            let args = AccountTransactionsQueryArgs::new(
                allow_latest_inconsistent_data,
                block_seq_no_range,
                aborted,
                min_balance_delta,
                max_balance_delta,
                PaginationArgs { first, after, last, before },
            );

            let mut transactions: Vec<db::Transaction> = db::Transaction::account_transactions(
                self.ctx.data::<SqlitePool>().unwrap(),
                self.address.clone(),
                &args,
            )
            .await?;

            let (has_previous_page, has_next_page) =
                args.pagination.get_bound_markers(transactions.len());
            tracing::debug!("has_previous_page={:?}, after={:?}", has_previous_page, has_next_page);

            let mut connection: Connection<
                String,
                Transaction,
                EmptyFields,
                EmptyFields,
                BlockchainTransactionsConnection,
                BlockchainTransactionsEdge,
            > = Connection::new(has_previous_page, has_next_page);

            args.pagination.shrink_portion(&mut transactions);

            connection.edges.extend(transactions.into_iter().map(|transaction| {
                let transaction: BlockchainTransaction = transaction.into();
                let cursor = transaction.chain_order.clone();
                let edge: Edge<String, Transaction, EmptyFields, BlockchainTransactionsEdge> =
                    Edge::with_additional_fields(cursor, transaction, EmptyFields);
                edge
            }));
            Ok::<_, async_graphql::Error>(connection)
        })
        .await
        .ok()
    }
}
