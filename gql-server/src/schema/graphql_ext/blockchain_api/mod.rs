// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use account::BlockchainAccountQuery;
use account::BlockchainMasterSeqNoFilter;
use async_graphql::connection::query;
use async_graphql::connection::Connection;
use async_graphql::connection::Edge;
use async_graphql::connection::EmptyFields;
use async_graphql::dataloader::DataLoader;
use async_graphql::Context;
use async_graphql::Object;
use bk_set_updates::BlockchainBkSetUpdate;
use bk_set_updates::BlockchainBkSetUpdatesConnection;
use bk_set_updates::BlockchainBkSetUpdatesEdge;
use bk_set_updates::BlockchainBkSetUpdatesQueryArgs;
use blocks::BlockchainBlock;
use blocks::BlockchainBlocksConnection;
use blocks::BlockchainBlocksEdge;
use blocks::BlockchainBlocksQueryArgs;
use transactions::BlockchainMessage;
use transactions::BlockchainTransaction;
use transactions::BlockchainTransactionsConnection;
use transactions::BlockchainTransactionsEdge;
use transactions::BlockchainTransactionsQueryArgs;

use super::message::MessageLoader;
use crate::helpers::pad_thread_id;
use crate::schema::db;
use crate::schema::db::account::BlockchainAccountsQueryArgs;
use crate::schema::db::DBConnector;
use crate::schema::graphql;
use crate::schema::graphql::block::BlockLoader;
use crate::schema::graphql::query::PaginationArgs;
use crate::schema::graphql::transaction::TransactionLoader;
use crate::schema::graphql_ext;
use crate::schema::graphql_ext::blockchain_api::account::BlockchainAccountEdge;
use crate::schema::graphql_ext::blockchain_api::account::BlockchainAccountsConnection;

pub mod account;
pub mod attestations;
pub mod bk_set_updates;
pub mod blocks;
pub mod transactions;

/// Blockchain-related information (blocks, transactions, etc.).
pub struct BlockchainQuery<'a> {
    pub ctx: &'a Context<'a>,
}

#[Object]
impl BlockchainQuery<'_> {
    #[graphql(name = "account")]
    /// Account-related information.
    async fn account(&self, address: String) -> Option<BlockchainAccountQuery<'_>> {
        Some(BlockchainAccountQuery { ctx: self.ctx, address, preloaded: None })
    }

    #[allow(clippy::too_many_arguments)]
    #[graphql(deprecation = "Use blockchain.account instead")]
    /// This node could be used for a cursor-based pagination of blocks.
    async fn accounts(
        &self,
        #[graphql(desc = "Filter by code hash")] code_hash: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
    ) -> Option<
        Connection<
            String,
            BlockchainAccountQuery<'_>,
            EmptyFields,
            EmptyFields,
            BlockchainAccountsConnection,
            BlockchainAccountEdge,
        >,
    > {
        let result = query(after, before, first, last, |after, before, first, last| async move {
            let query_args = BlockchainAccountsQueryArgs {
                code_hash: code_hash.clone(),
                pagination: PaginationArgs { first, after, last, before },
            };
            let mut accounts: Vec<db::Account> = db::account::Account::blockchain_accounts(
                self.ctx.data::<Arc<DBConnector>>().unwrap(),
                &query_args,
            )
            .await?;

            let (has_previous_page, has_next_page) =
                query_args.pagination.get_bound_markers(accounts.len());

            let mut connection: Connection<
                String,
                BlockchainAccountQuery,
                EmptyFields,
                EmptyFields,
                BlockchainAccountsConnection,
                BlockchainAccountEdge,
            > = Connection::new(has_previous_page, has_next_page);

            query_args.pagination.shrink_portion(&mut accounts);

            connection.edges.extend(accounts.into_iter().map(|account| {
                let cursor = account.id.clone();
                let address = account.id.clone();
                let account =
                    BlockchainAccountQuery { address, ctx: self.ctx, preloaded: Some(account) };
                let edge: Edge<String, BlockchainAccountQuery, EmptyFields, BlockchainAccountEdge> =
                    Edge::with_additional_fields(cursor, account, EmptyFields);
                edge
            }));

            Ok::<_, async_graphql::Error>(connection)
        })
        .await;
        match result {
            Ok(connection) => Some(connection),
            Err(e) => {
                println!("Failed to load accounts: {}", e.message);
                tracing::error!("Failed to load accounts: {}", e.message);
                None
            }
        }
    }

    async fn block(&self, hash: String) -> Option<BlockchainBlock> {
        let block_loader = self.ctx.data_unchecked::<DataLoader<BlockLoader>>();
        let message_loader = self.ctx.data_unchecked::<DataLoader<MessageLoader>>();
        let block = block_loader.load_one(hash).await.expect("Failed to load block");

        let block = block?;

        // if self.ctx.look_ahead().field("block").field("in_message").exists() {
        //     let in_message =
        //         message_loader.load_one(block.in_msg.clone()).await.expect("Failed to
        // load in_message");     block.in_message = in_message;
        // }

        if self.ctx.look_ahead().field("block").field("out_messages").exists() {
            let out_msg_ids = block.out_msgs.clone();
            let _out_messages =
                message_loader.load_many(out_msg_ids).await.expect("Failed to load out_messages");

            // block.out_msg_descr =
            //     Some(out_messages.into_values().map(|m| Some(m)).collect());
        }

        Some(block)
    }

    /// Returns a block uniquely identified by the thread ID and block height.
    async fn block_by_height(
        &self,
        #[graphql(
            name = "thread_id",
            desc = "A 68-character thread identifier, left-padded with zeros."
        )]
        thread_id: String,
        #[graphql(desc = "The block height is unique within a thread.")] height: u64,
    ) -> Option<BlockchainBlock> {
        let block_loader = self.ctx.data_unchecked::<DataLoader<BlockLoader>>();
        let message_loader = self.ctx.data_unchecked::<DataLoader<MessageLoader>>();
        let thread_id = pad_thread_id(thread_id);
        let block = block_loader.load_one((thread_id, height)).await.expect("Failed to load block");

        let block = block?;

        if self.ctx.look_ahead().field("block").field("out_messages").exists() {
            let out_msg_ids = block.out_msgs.clone();
            let _out_messages =
                message_loader.load_many(out_msg_ids).await.expect("Failed to load out_messages");
        }

        Some(block)
    }

    #[allow(clippy::too_many_arguments)]
    /// This node could be used for a cursor-based pagination of blocks.
    async fn blocks(
        &self,
        #[graphql(
            name = "allow_latest_inconsistent_data",
            desc = "By default there is special latency added for the fetched recent data (several seconds) to ensure impossibility of inserts before the latest fetched cursor (data consistency, for reliable pagination). It is possible to disable this guarantee and to reduce the latency of realtime data by setting this flag to true."
        )]
        _allow_latest_inconsistent_data: Option<bool>,
        #[graphql(name = "master_seq_no_range")] block_seq_no_range: Option<
            BlockchainMasterSeqNoFilter,
        >,
        #[graphql(
            name = "min_tr_count",
            desc = "Optional filter by minimum transactions in a block (unoptimized, query could be dropped by timeout)"
        )]
        min_tr_count: Option<i32>,
        #[graphql(
            name = "max_tr_count",
            desc = "Optional filter by maximum transactions in a block (unoptimized, query could be dropped by timeout)"
        )]
        max_tr_count: Option<i32>,
        #[graphql(
            name = "thread_id",
            desc = "A 68-character thread identifier, left-padded with zeros."
        )]
        thread_id: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
    ) -> Option<
        Connection<
            String,
            BlockchainBlock,
            EmptyFields,
            EmptyFields,
            BlockchainBlocksConnection,
            BlockchainBlocksEdge,
        >,
    > {
        query(after, before, first, last, |after, before, first, last| async move {
            let args = BlockchainBlocksQueryArgs {
                block_seq_no_range,
                min_tr_count,
                max_tr_count,
                thread_id,
                pagination: PaginationArgs { first, after, last, before },
            };
            let mut blocks: Vec<db::Block> = db::block::Block::blockchain_blocks(
                self.ctx.data::<Arc<DBConnector>>().unwrap(),
                &args,
            )
            .await?;

            let (has_previous_page, has_next_page) = (
                args.pagination.has_previous_page(blocks.len()),
                args.pagination.has_next_page(blocks.len()),
            );
            tracing::debug!("has_previous_page={:?}, after={:?}", has_previous_page, has_next_page);

            let mut connection: Connection<
                String,
                graphql_ext::Block,
                EmptyFields,
                EmptyFields,
                BlockchainBlocksConnection,
                BlockchainBlocksEdge,
            > = Connection::new(has_previous_page, has_next_page);

            args.pagination.shrink_portion(&mut blocks);

            connection.edges.extend(blocks.into_iter().map(|block| {
                let block: BlockchainBlock = block.into();
                let cursor = block.chain_order.clone().unwrap();
                let edge: Edge<String, graphql::block::Block, EmptyFields, BlockchainBlocksEdge> =
                    Edge::with_additional_fields(cursor, block, EmptyFields);
                edge
            }));

            Ok::<_, async_graphql::Error>(connection)
        })
        .await
        .ok()
    }

    #[allow(clippy::too_many_arguments)]
    #[graphql(name = "bkSetUpdates")]
    async fn bk_set_updates(
        &self,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
        #[graphql(name = "thread_id", desc = "Thread identifier.")] thread_id: Option<String>,
        #[graphql(name = "height_start", desc = "Include updates with height >= this value.")]
        height_start: Option<u64>,
        #[graphql(name = "height_end", desc = "Include updates with height <= this value.")]
        height_end: Option<u64>,
    ) -> Option<
        Connection<
            String,
            BlockchainBkSetUpdate,
            EmptyFields,
            EmptyFields,
            BlockchainBkSetUpdatesConnection,
            BlockchainBkSetUpdatesEdge,
        >,
    > {
        query(after, before, first, last, |after, before, first, last| async move {
            let thread_id = thread_id.map(pad_thread_id);
            let args = BlockchainBkSetUpdatesQueryArgs {
                pagination: PaginationArgs { first, after, last, before },
                thread_id,
                height_start,
                height_end,
            };
            let mut updates: Vec<db::BkSetUpdate> =
                db::bk_set_update::BkSetUpdate::blockchain_bk_set_updates(
                    self.ctx.data::<Arc<DBConnector>>().unwrap(),
                    &args,
                )
                .await?;

            let (has_previous_page, has_next_page) = (
                args.pagination.has_previous_page(updates.len()),
                args.pagination.has_next_page(updates.len()),
            );

            let mut connection: Connection<
                String,
                BlockchainBkSetUpdate,
                EmptyFields,
                EmptyFields,
                BlockchainBkSetUpdatesConnection,
                BlockchainBkSetUpdatesEdge,
            > = Connection::new(has_previous_page, has_next_page);

            args.pagination.shrink_portion(&mut updates);

            let mut edges = Vec::new();
            for update in updates {
                let update: BlockchainBkSetUpdate = update
                    .try_into()
                    .map_err(|e: anyhow::Error| async_graphql::Error::new(e.to_string()))?;
                let cursor = update.chain_order.clone();
                let edge: Edge<
                    String,
                    BlockchainBkSetUpdate,
                    EmptyFields,
                    BlockchainBkSetUpdatesEdge,
                > = Edge::with_additional_fields(cursor, update, EmptyFields);
                edges.push(edge);
            }
            connection.edges.extend(edges);

            Ok::<_, async_graphql::Error>(connection)
        })
        .await
        .ok()
    }

    async fn finalized_timestamp(&self) -> Option<u64> {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()?.as_secs();
        Some(now.saturating_sub(5 * 60))
    }

    async fn message(&self, hash: String) -> Option<BlockchainMessage> {
        let transaction_loader = self.ctx.data_unchecked::<DataLoader<TransactionLoader>>();
        let message_loader = self.ctx.data_unchecked::<DataLoader<MessageLoader>>();

        let message = message_loader.load_one(hash).await.expect("Failed to load message");
        let mut message = message?;
        if self.ctx.look_ahead().field("message").field("src_transaction").exists() {
            if let Some(transaction_id) = message.transaction_id.clone() {
                message.src_transaction = transaction_loader
                    .load_one(transaction_id.clone())
                    .await
                    .unwrap_or_else(|_| panic!("Failed to load transaction: {transaction_id}"))
                    .map(Box::new);
            }
        }

        if self.ctx.look_ahead().field("message").field("dst_transaction").exists() {
            let dst_transaction = db::transaction::Transaction::by_in_message(
                self.ctx.data::<Arc<DBConnector>>().unwrap(),
                &message.id,
                None,
            )
            .await
            .expect("Failed to load transaction by inbound message");

            if let Some(transaction) = dst_transaction {
                message.dst_transaction = transaction_loader
                    .load_one(transaction.id.clone())
                    .await
                    .unwrap_or_else(|_| panic!("Failed to load transaction: {}", transaction.id))
                    .map(Box::new);
            }
        }

        Some(message)
    }

    async fn transaction(&self, hash: String) -> Option<BlockchainTransaction> {
        let transaction_loader = self.ctx.data_unchecked::<DataLoader<TransactionLoader>>();
        let message_loader = self.ctx.data_unchecked::<DataLoader<MessageLoader>>();
        let transaction =
            transaction_loader.load_one(hash).await.expect("Failed to load transaction");

        let mut transaction = transaction?;

        if self.ctx.look_ahead().field("transaction").field("in_message").exists() {
            let in_message = message_loader
                .load_one(transaction.in_msg.clone())
                .await
                .expect("Failed to load in_message");
            transaction.in_message = in_message;
        }

        if self.ctx.look_ahead().field("transaction").field("out_messages").exists() {
            let out_msg_ids = transaction.out_msgs.clone();
            let out_messages =
                message_loader.load_many(out_msg_ids).await.expect("Failed to load out_messages");

            transaction.out_messages = Some(out_messages.into_values().map(Some).collect());
        }

        Some(transaction)
    }

    #[allow(clippy::too_many_arguments)]
    /// This node could be used for a cursor-based pagination of transactions.
    async fn transactions(
        &self,
        #[graphql(
            name = "allow_latest_inconsistent_data",
            desc = "By default there is special latency added for the fetched recent data (several seconds) to ensure impossibility of inserts before the latest fetched cursor (data consistency, for reliable pagination). It is possible to disable this guarantee and to reduce the latency of realtime data by setting this flag to true."
        )]
        _allow_latest_inconsistent_data: Option<bool>,
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
        #[graphql(
            name = "code_hash",
            desc = "Optional filter by code hash of the account before execution."
        )]
        code_hash: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
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
        query(
            after,
            before,
            first,
            last,
            |after: Option<String>, before: Option<String>, first, last| async move {
                let args = BlockchainTransactionsQueryArgs {
                    min_balance_delta,
                    max_balance_delta,
                    code_hash,
                    pagination: PaginationArgs { first, after, last, before },
                };
                let message_loader = self.ctx.data_unchecked::<DataLoader<MessageLoader>>();
                let mut transactions = db::transaction::Transaction::blockchain_transactions(
                    self.ctx.data::<Arc<DBConnector>>().unwrap(),
                    &args,
                )
                .await?;

                let (has_previous_page, has_next_page) = (
                    args.pagination.has_previous_page(transactions.len()),
                    args.pagination.has_next_page(transactions.len()),
                );
                tracing::debug!(
                    "has_previous_page={:?}, after={:?}",
                    has_previous_page,
                    has_next_page
                );

                let mut connection: Connection<
                    String,
                    crate::schema::graphql_ext::Transaction,
                    EmptyFields,
                    EmptyFields,
                    BlockchainTransactionsConnection,
                    BlockchainTransactionsEdge,
                > = Connection::new(has_previous_page, has_next_page);

                args.pagination.shrink_portion(&mut transactions);

                let selection_set =
                    self.ctx.look_ahead().field("transactions").field("edges").field("node");
                let mut edges = Vec::new();
                for transaction in transactions.into_iter() {
                    let mut transaction: BlockchainTransaction = transaction.into();

                    if selection_set.field("in_message").exists() {
                        let in_message =
                            message_loader.load_many(vec![transaction.in_msg.clone()]).await?;
                        transaction.in_message =
                            in_message.get(&transaction.in_msg).map(ToOwned::to_owned);
                    }

                    if selection_set.field("out_messages").exists() {
                        let out_msg_ids = transaction.out_msgs.clone();
                        let out_messages = message_loader.load_many(out_msg_ids).await?;
                        transaction.out_messages =
                            Some(out_messages.into_values().map(Some).collect());
                    }

                    let cursor = transaction.chain_order.clone();
                    let edge: Edge<
                        String,
                        graphql::transaction::Transaction,
                        EmptyFields,
                        BlockchainTransactionsEdge,
                    > = Edge::with_additional_fields(cursor, transaction, EmptyFields);
                    edges.push(edge);
                }

                connection.edges.extend(edges);

                Ok::<_, async_graphql::Error>(connection)
            },
        )
        .await
        .ok()
    }
}
