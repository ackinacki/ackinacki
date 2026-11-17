// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use account::account_id_from_archive_address;
use account::archive_address_from_account_id;
use account::BlockchainAccountQuery;
use account::BlockchainMasterSeqNoFilter;
use async_graphql::connection::query;
use async_graphql::connection::Connection;
use async_graphql::connection::Edge;
use async_graphql::connection::EmptyFields;
use async_graphql::dataloader::DataLoader;
use async_graphql::Context;
use async_graphql::Lookahead;
use async_graphql::Object;
use bk_set_updates::BlockchainBkSetUpdate;
use bk_set_updates::BlockchainBkSetUpdatesConnection;
use bk_set_updates::BlockchainBkSetUpdatesEdge;
use bk_set_updates::BlockchainBkSetUpdatesQueryArgs;
use blocks::BlockchainBlock;
use blocks::BlockchainBlocksConnection;
use blocks::BlockchainBlocksEdge;
use blocks::BlockchainBlocksQueryArgs;
use latest_bk_set::BlockchainLatestBkSet;
use transactions::BlockchainMessage;
use transactions::BlockchainTransaction;
use transactions::BlockchainTransactionsConnection;
use transactions::BlockchainTransactionsEdge;
use transactions::BlockchainTransactionsQueryArgs;

use super::message::MessageLoader;
use crate::defaults::THREAD_ZERO;
use crate::helpers::pad_thread_id;
use crate::schema::db;
use crate::schema::db::account::BlockchainAccountsQueryArgs;
use crate::schema::db::projection::normalize_graphql_field_name;
use crate::schema::db::DBConnector;
use crate::schema::graphql;
use crate::schema::graphql::block::BlockLoader;
use crate::schema::graphql::query::PaginationArgs;
use crate::schema::graphql::transaction::TransactionLoader;
use crate::schema::graphql_ext;
use crate::schema::graphql_ext::blockchain_api::account::BlockchainAccountEdge;
use crate::schema::graphql_ext::blockchain_api::account::BlockchainAccountsConnection;
use crate::schema::graphql_ext::events::BlockchainEventEdge;
use crate::schema::graphql_ext::events::BlockchainEventsConnection;
use crate::schema::graphql_ext::events::Event;
use crate::schema::graphql_ext::history_allowlist;
use crate::schema::graphql_ext::is_deprecated_api_visible;
use crate::schema::graphql_ext::strip_cold_pruned_transaction_fields;
use crate::schema::graphql_ext::DeprecatedApiGuard;

pub mod account;
pub mod attestations;
pub mod bk_set_updates;
pub mod blocks;
pub mod latest_bk_set;
pub mod layer_tree;
pub mod transactions;
pub mod voucher_events;

/// A history proofs store read failed.
fn store_error(err: anyhow::Error) -> async_graphql::Error {
    crate::schema::db::connector::map_db_error(async_graphql::Error::new(format!(
        "history proofs store: {err:#}"
    )))
}

/// A block from the history proofs store: the selection is checked against
/// the closed field list first (on this field's own selection, so aliased
/// calls are checked one by one), then the row's formats.
fn file_block(
    ctx: &Context<'_>,
    row: Option<db::history_store::HistoryBlockRow>,
) -> async_graphql::Result<Option<BlockchainBlock>> {
    let Some(row) = row else {
        return Ok(None);
    };
    history_allowlist::check_selection(
        ctx.look_ahead(),
        history_allowlist::BLOCK_FIELDS,
        &[("attestations", history_allowlist::ATTESTATION_FIELDS)],
    )?;
    row.validate().map_err(|err| history_allowlist::data_corrupt(row.height(), err))?;
    Ok(Some(BlockchainBlock::from_history_row(row)))
}

pub(super) fn selected_node_fields(ctx: &Context<'_>, root: &str) -> Vec<String> {
    selected_node_fields_at(ctx, &[root])
}

pub(super) fn selected_single_fields(ctx: &Context<'_>, root: &str) -> Vec<String> {
    selected_fields_at(ctx, &[root])
}

pub(super) fn selected_node_fields_at(ctx: &Context<'_>, path: &[&str]) -> Vec<String> {
    let mut node_path = Vec::with_capacity(path.len() + 2);
    node_path.extend_from_slice(path);
    node_path.extend_from_slice(&["edges", "node"]);
    let mut fields = selected_fields_at(ctx, &node_path);

    let mut nodes_path = Vec::with_capacity(path.len() + 1);
    nodes_path.extend_from_slice(path);
    nodes_path.push("nodes");
    for field in selected_fields_at(ctx, &nodes_path) {
        if !fields.contains(&field) {
            fields.push(field);
        }
    }

    fields
}

pub(super) fn selected_fields_at(ctx: &Context<'_>, path: &[&str]) -> Vec<String> {
    let mut lookahead = ctx.look_ahead();
    for segment in path {
        lookahead = lookahead.field(segment);
    }

    let mut fields = Vec::new();
    for field in lookahead.selection_fields() {
        for selected in field.selection_set() {
            let name = selected.name();
            if name.starts_with("__") {
                continue;
            }
            let normalized = normalize_graphql_field_name(name);
            if !fields.contains(&normalized) {
                fields.push(normalized);
            }
        }
    }
    fields
}

fn parse_hex32(value: &str, field_name: &str) -> async_graphql::Result<String> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(async_graphql::Error::new(format!(
            "Invalid {field_name}: expected 64 hex characters without prefix"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

/// Blockchain-related information (blocks, transactions, etc.).
pub struct BlockchainQuery<'a> {
    pub ctx: &'a Context<'a>,
}

pub(super) const MAX_RELATION_DEPTH: usize = 20;
type PopulateFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

pub(super) fn connection_node_selection<'a>(selection: Lookahead<'a>) -> Lookahead<'a> {
    let mut fields = selection.field("edges").field("node").selection_fields();
    fields.extend(selection.field("nodes").selection_fields());
    Lookahead::try_from(fields.as_slice()).unwrap_or(selection.field("edges").field("node"))
}

pub(super) fn populate_message_tree<'a>(
    ctx: &'a Context<'a>,
    message: &'a mut BlockchainMessage,
    selection: Lookahead<'a>,
    depth: usize,
) -> PopulateFuture<'a> {
    Box::pin(async move {
        if depth == 0 || !selection.exists() {
            return Ok(());
        }

        let transaction_loader = ctx.data_unchecked::<DataLoader<TransactionLoader>>();

        let src_selection = selection.field("src_transaction");
        if src_selection.exists() {
            if let Some(transaction_id) = message.transaction_id.clone() {
                if let Some(mut transaction) = transaction_loader
                    .load_one(transaction_id.clone())
                    .await
                    .unwrap_or_else(|_| panic!("Failed to load transaction: {transaction_id}"))
                {
                    populate_transaction_tree(ctx, &mut transaction, src_selection, depth - 1)
                        .await?;
                    message.src_transaction = Some(Box::new(transaction));
                }
            }
        }

        let dst_selection = selection.field("dst_transaction");
        if dst_selection.exists() {
            let projection = db::transaction::Transaction::projection_for_fields(["id"]);
            if let Some(transaction) = db::transaction::Transaction::by_in_message(
                ctx.data::<Arc<DBConnector>>().unwrap(),
                &projection,
                &message.id,
                None,
            )
            .await?
            {
                if let Some(mut transaction) = transaction_loader
                    .load_one(transaction.id.clone())
                    .await
                    .unwrap_or_else(|_| panic!("Failed to load transaction: {}", transaction.id))
                {
                    populate_transaction_tree(ctx, &mut transaction, dst_selection, depth - 1)
                        .await?;
                    message.dst_transaction = Some(Box::new(transaction));
                }
            }
        }

        Ok(())
    })
}

pub(super) fn populate_transaction_tree<'a>(
    ctx: &'a Context<'a>,
    transaction: &'a mut BlockchainTransaction,
    selection: Lookahead<'a>,
    depth: usize,
) -> PopulateFuture<'a> {
    Box::pin(async move {
        if depth == 0 || !selection.exists() {
            return Ok(());
        }

        let message_loader = ctx.data_unchecked::<DataLoader<MessageLoader>>();

        let in_message_selection = selection.field("in_message");
        if in_message_selection.exists() {
            transaction.in_message = message_loader
                .load_one(transaction.in_msg.clone())
                .await
                .map_err(|err| anyhow::anyhow!("{err:?}"))?;
            if let Some(in_message) = transaction.in_message.as_mut() {
                populate_message_tree(ctx, in_message, in_message_selection, depth - 1).await?;
            }
        }

        let out_messages_selection = selection.field("out_messages");
        if out_messages_selection.exists() {
            let loaded_messages = message_loader
                .load_many(transaction.out_msgs.clone())
                .await
                .map_err(|err| anyhow::anyhow!("{err:?}"))?;
            let mut out_messages: Vec<Option<BlockchainMessage>> =
                transaction.out_msgs.iter().map(|id| loaded_messages.get(id).cloned()).collect();
            for out_message in out_messages.iter_mut().flatten() {
                populate_message_tree(ctx, out_message, selection.field("out_messages"), depth - 1)
                    .await?;
            }
            transaction.out_messages = Some(out_messages);
        }

        Ok(())
    })
}

#[Object]
impl BlockchainQuery<'_> {
    #[graphql(name = "account")]
    /// Account-related information.
    async fn account(
        &self,
        #[graphql(name = "account_id")] account_id: String,
        #[graphql(name = "dapp_id")] dapp_id: String,
    ) -> async_graphql::Result<Option<BlockchainAccountQuery<'_>>> {
        let account_id = parse_hex32(&account_id, "account_id")?;
        let dapp_id = parse_hex32(&dapp_id, "dapp_id")?;
        let archive_address = archive_address_from_account_id(&account_id);
        Ok(Some(BlockchainAccountQuery {
            ctx: self.ctx,
            account_id,
            dapp_id,
            archive_address,
            preloaded: None,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    #[graphql(
        guard = "DeprecatedApiGuard",
        visible = "is_deprecated_api_visible",
        deprecation = "Use blockchain.account instead"
    )]
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
            let projection = db::Account::connection_projection_for_fields(selected_fields_at(
                self.ctx,
                &["accounts", "edges", "node", "info"],
            ));
            let mut accounts: Vec<db::Account> = db::account::Account::blockchain_accounts(
                self.ctx.data::<Arc<DBConnector>>()?,
                &projection,
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
                let archive_address = account.id.clone();
                let account_id = account_id_from_archive_address(&archive_address);
                let dapp_id = account.dapp_id.clone().unwrap_or_default();
                let account = BlockchainAccountQuery {
                    ctx: self.ctx,
                    account_id,
                    dapp_id,
                    archive_address,
                    preloaded: Some(account),
                };
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

    /// A block by its id. Thread-0 blocks that left the main DB and the
    /// archives are served from the history proofs store with a closed set of
    /// fields; the store holds no other thread.
    async fn block(
        &self,
        ctx: &Context<'_>,
        hash: String,
    ) -> async_graphql::Result<Option<BlockchainBlock>> {
        let block_loader = self.ctx.data_unchecked::<DataLoader<BlockLoader>>();
        let message_loader = self.ctx.data_unchecked::<DataLoader<MessageLoader>>();
        // Shared DataLoaders eagerly convert DB rows into GraphQL objects, so
        // keep their compatibility projections and use lookahead only for
        // optional nested loads here.
        let block = block_loader
            .load_one(hash.clone())
            .await
            .map_err(crate::schema::db::connector::map_db_error)?;

        let Some(block) = block else {
            let db_connector = ctx.data::<Arc<DBConnector>>()?;
            let row = db::history_store::HistoryBlockRow::by_block_id(db_connector, &hash)
                .await
                .map_err(store_error)?;
            return file_block(ctx, row);
        };

        // if self.ctx.look_ahead().field("block").field("in_message").exists() {
        //     let in_message =
        //         message_loader.load_one(block.in_msg.clone()).await.expect("Failed to
        // load in_message");     block.in_message = in_message;
        // }

        let selected_fields = selected_single_fields(self.ctx, "block");
        if selected_fields.iter().any(|field| field == "out_messages") {
            let out_msg_ids = block.out_msgs.clone();
            let _out_messages = message_loader
                .load_many(out_msg_ids)
                .await
                .map_err(crate::schema::db::connector::map_db_error)?;

            // block.out_msg_descr =
            //     Some(out_messages.into_values().map(|m| Some(m)).collect());
        }

        Ok(Some(block))
    }

    /// This node could be used for a cursor-based pagination of blockchain
    /// events (aka outgoing external messages).
    async fn events(
        &self,
        #[graphql(name = "src_dapp_id", desc = "Optional source dApp ID filter.")]
        src_dapp_id: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
    ) -> async_graphql::Result<
        Option<
            Connection<
                String,
                Event,
                EmptyFields,
                EmptyFields,
                BlockchainEventsConnection,
                BlockchainEventEdge,
            >,
        >,
    > {
        Ok(Some(
            query(after, before, first, last, |after, before, first, last| async move {
                tracing::debug!(
                    "first={:?}, after={:?}, last={:?}, before={:?}",
                    first,
                    after,
                    last,
                    before
                );

                let pagination = PaginationArgs { first, after, last, before };
                let projection = db::Message::event_projection_for_fields(selected_node_fields(
                    self.ctx, "events",
                ));
                let mut messages = db::Message::blockchain_events(
                    self.ctx.data::<Arc<DBConnector>>()?,
                    &projection,
                    src_dapp_id,
                    &pagination,
                )
                .await?;

                let (has_previous_page, has_next_page) =
                    pagination.get_bound_markers(messages.len());
                tracing::debug!(
                    "has_previous_page={:?}, after={:?}",
                    has_previous_page,
                    has_next_page
                );

                let mut connection: Connection<
                    String,
                    Event,
                    EmptyFields,
                    EmptyFields,
                    BlockchainEventsConnection,
                    BlockchainEventEdge,
                > = Connection::new(has_previous_page, has_next_page);

                pagination.shrink_portion(&mut messages);

                let mut edges: Vec<Edge<String, Event, EmptyFields, BlockchainEventEdge>> = vec![];
                for message in messages {
                    let Some(cursor) = message.msg_chain_order.clone() else {
                        tracing::warn!(
                            "ext_out message {} has no msg_chain_order, skipping",
                            message.id,
                        );
                        continue;
                    };
                    let event: Event = message.into();
                    let edge: Edge<String, Event, EmptyFields, BlockchainEventEdge> =
                        Edge::with_additional_fields(cursor, event, EmptyFields);
                    edges.push(edge);
                }
                connection.edges.extend(edges);
                Ok::<_, async_graphql::Error>(connection)
            })
            .await
            .map_err(crate::schema::db::connector::map_db_error)?,
        ))
    }

    /// Returns a block uniquely identified by the thread ID and block height.
    /// Thread-0 blocks that left the main DB and the archives are served from
    /// the history proofs store with a closed set of fields.
    async fn block_by_height(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            name = "thread_id",
            desc = "A 68-character thread identifier, left-padded with zeros."
        )]
        thread_id: String,
        #[graphql(desc = "The block height is unique within a thread.")] height: u64,
    ) -> async_graphql::Result<Option<BlockchainBlock>> {
        let block_loader = self.ctx.data_unchecked::<DataLoader<BlockLoader>>();
        let message_loader = self.ctx.data_unchecked::<DataLoader<MessageLoader>>();
        let thread_id = pad_thread_id(thread_id);
        // Shared DataLoaders eagerly convert DB rows into GraphQL objects, so
        // keep their compatibility projections and use lookahead only for
        // optional nested loads here.
        let block = block_loader
            .load_one((thread_id.clone(), height))
            .await
            .map_err(crate::schema::db::connector::map_db_error)?;

        let Some(block) = block else {
            if thread_id != THREAD_ZERO {
                return Ok(None);
            }
            let db_connector = ctx.data::<Arc<DBConnector>>()?;
            let row = db::history_store::HistoryBlockRow::by_height(db_connector, height)
                .await
                .map_err(store_error)?;
            return file_block(ctx, row);
        };

        let selected_fields = selected_single_fields(self.ctx, "block_by_height");
        if selected_fields.iter().any(|field| field == "out_messages") {
            let out_msg_ids = block.out_msgs.clone();
            let _out_messages = message_loader
                .load_many(out_msg_ids)
                .await
                .map_err(crate::schema::db::connector::map_db_error)?;
        }

        Ok(Some(block))
    }

    /// The 130 leaves of the history proof tree of `layer` whose root is
    /// recorded in the thread-0 block at `height` (`history_proofs[layer]`).
    /// The tree is rebuilt and checked against that root before it is
    /// returned. Errors carry `extensions.code`: `LAYER_TREE_BAD_ARGUMENT`,
    /// `LAYER_TREE_MISSING_DATA` (retry later or ask another server),
    /// `LAYER_TREE_ROOT_MISMATCH`, `HISTORY_DATA_CORRUPT`.
    #[graphql(name = "layerTree")]
    async fn layer_tree(
        &self,
        #[graphql(name = "thread_id", desc = "Thread identifier; only thread 0 has layer trees.")]
        thread_id: String,
        #[graphql(desc = "A multiple of 128^layer.")] height: u64,
        #[graphql(desc = "Layer number, 1..=10.")] layer: u8,
    ) -> async_graphql::Result<Option<layer_tree::LayerTree>> {
        layer_tree::layer_tree(self.ctx.data::<Arc<DBConnector>>()?, thread_id, height, layer)
            .await
            .map(Some)
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
    ) -> async_graphql::Result<
        Option<
            Connection<
                String,
                BlockchainBlock,
                EmptyFields,
                EmptyFields,
                BlockchainBlocksConnection,
                BlockchainBlocksEdge,
            >,
        >,
    > {
        Ok(Some(
            query(after, before, first, last, |after, before, first, last| async move {
                let args = BlockchainBlocksQueryArgs {
                    block_seq_no_range,
                    min_tr_count,
                    max_tr_count,
                    thread_id: thread_id.map(pad_thread_id),
                    pagination: PaginationArgs { first, after, last, before },
                };
                let projection = db::block::Block::connection_projection_for_fields(
                    selected_node_fields(self.ctx, "blocks"),
                );
                let mut blocks: Vec<db::Block> = db::block::Block::blockchain_blocks(
                    self.ctx.data::<Arc<DBConnector>>()?,
                    &projection,
                    &args,
                )
                .await?;

                let (has_previous_page, has_next_page) = (
                    args.pagination.has_previous_page(blocks.len()),
                    args.pagination.has_next_page(blocks.len()),
                );
                tracing::debug!(
                    "has_previous_page={:?}, after={:?}",
                    has_previous_page,
                    has_next_page
                );

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
                    let edge: Edge<
                        String,
                        graphql::block::Block,
                        EmptyFields,
                        BlockchainBlocksEdge,
                    > = Edge::with_additional_fields(cursor, block, EmptyFields);
                    edge
                }));

                Ok::<_, async_graphql::Error>(connection)
            })
            .await
            .map_err(crate::schema::db::connector::map_db_error)?,
        ))
    }

    #[doc = "Exact BK set (signer_index → BLS public key) that signs block `block_id` (`seq_no`, `height`) of thread `thread_id`; changes made by that block itself are not applied. Null when no usable snapshot is available: the snapshot table is missing or empty, its rows disagree, or the anchor block is outside the history this server serves (its main DB and the attached archives except the oldest one). Null is not an empty set: the last case clears once a newer snapshot is written, so retry later. The commitment covers the signer indexes and the x-coordinates of the keys only; the y sign bit of each compressed key is taken from the node API, and a wrong one shows up as a failing BLS signature check. Database errors are returned as GraphQL errors, not as null. To get the set at a target block T: apply `bkSetUpdates(thread_id, height_start: height, height_end: <height of T>)` in order, checking before each update that L2 of its block equals your current commitment and after it that the new commitment equals L3; finally, even if no updates were returned, check that the resulting commitment equals L3 of T (always L3: the bound is inclusive and L3 = L2 when T has no changes). L2 and L3 are entries 2 and 3 (0-based) of the block's `block_merkle_tree_leaves`. On any mismatch, discard the result and bootstrap again. The server does not guarantee completeness of `bkSetUpdates`."]
    #[graphql(name = "latestBkSet")]
    async fn latest_bk_set(&self) -> async_graphql::Result<Option<BlockchainLatestBkSet>> {
        let snapshot = crate::schema::db::latest_bk_set::load_latest_bk_set(
            self.ctx.data::<Arc<DBConnector>>()?,
        )
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))
        .map_err(crate::schema::db::connector::map_db_error)?;
        Ok(snapshot.map(Into::into))
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
    ) -> async_graphql::Result<
        Option<
            Connection<
                String,
                BlockchainBkSetUpdate,
                EmptyFields,
                EmptyFields,
                BlockchainBkSetUpdatesConnection,
                BlockchainBkSetUpdatesEdge,
            >,
        >,
    > {
        Ok(Some(
            query(after, before, first, last, |after, before, first, last| async move {
                let thread_id = thread_id.map(pad_thread_id);
                let args = BlockchainBkSetUpdatesQueryArgs {
                    pagination: PaginationArgs { first, after, last, before },
                    thread_id,
                    height_start,
                    height_end,
                };
                let projection = db::BkSetUpdate::connection_projection_for_fields(
                    selected_node_fields(self.ctx, "bkSetUpdates"),
                );
                let mut updates: Vec<db::BkSetUpdate> =
                    db::bk_set_update::BkSetUpdate::blockchain_bk_set_updates(
                        self.ctx.data::<Arc<DBConnector>>()?,
                        &projection,
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
            .map_err(crate::schema::db::connector::map_db_error)?,
        ))
    }

    async fn finalized_timestamp(&self) -> Option<u64> {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()?.as_secs();
        Some(now.saturating_sub(5 * 60))
    }

    /// A message by its hash. Bridge `WithdrawalInitiated` and RootPN
    /// `VoucherGenerated` events that left the main DB and the archives are
    /// served from the history proofs store with a closed set of fields; so
    /// are the `block_id` and the source transaction of such an event when its
    /// transaction left them.
    async fn message(
        &self,
        ctx: &Context<'_>,
        hash: String,
    ) -> async_graphql::Result<Option<BlockchainMessage>> {
        let message_loader = self.ctx.data_unchecked::<DataLoader<MessageLoader>>();
        let db_connector = ctx.data::<Arc<DBConnector>>()?;
        let selection = ctx.look_ahead();

        // Shared DataLoaders eagerly convert DB rows into GraphQL objects, so
        // keep their compatibility projections and use lookahead only for
        // optional nested loads here.
        let message = message_loader
            .load_one(hash.clone())
            .await
            .map_err(crate::schema::db::connector::map_db_error)?;
        let Some(mut message) = message else {
            let Some(event) = db::history_store::EventDbRow::by_hash(db_connector, &hash)
                .await
                .map_err(store_error)?
            else {
                return Ok(None);
            };
            history_allowlist::check_selection(
                selection,
                history_allowlist::MESSAGE_FIELDS,
                &[("src_transaction", history_allowlist::TRANSACTION_FIELDS)],
            )?;
            return Ok(Some(BlockchainMessage::from_stored_event(event)));
        };
        populate_message_tree(ctx, &mut message, ctx.look_ahead(), MAX_RELATION_DEPTH)
            .await
            .map_err(|err| async_graphql::Error::new(format!("{err:#}")))?;

        let transaction_selection = ctx.look_ahead().field("src_transaction");
        let transaction_missing =
            transaction_selection.exists() && message.src_transaction.is_none();
        if message.stored_block_id().is_none() || transaction_missing {
            if let Some(event) = db::history_store::EventDbRow::by_hash(db_connector, &hash)
                .await
                .map_err(store_error)?
            {
                if transaction_missing {
                    history_allowlist::check_selection(
                        transaction_selection,
                        history_allowlist::TRANSACTION_FIELDS,
                        &[],
                    )?;
                }
                message.fill_from_stored_event(&event, transaction_missing);
            }
        }

        Ok(Some(message))
    }

    #[doc = "RootPN `VoucherGenerated` events whose `skUCommit` is `sk_u_commit` (1 to 64 hex digits, with or without `0x`, any letter case), by height; normally one. Read from the history proofs store only, with the closed field list of messages served from it; a voucher is found as soon as block-manager has written its block. A server without the history proofs store returns the error `HISTORY_STORE_UNAVAILABLE`, never an empty list; a bad argument gives `VOUCHER_EVENTS_BAD_ARGUMENT`. An empty list means only that this server's store has no such voucher: the height may be in a gap of the block-manager that writes it, and only thread 0 is stored."]
    #[graphql(name = "voucherEvents")]
    async fn voucher_events(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "sk_u_commit")] sk_u_commit: String,
    ) -> async_graphql::Result<Vec<BlockchainMessage>> {
        use voucher_events::error;
        use voucher_events::CODE_HISTORY_STORE_UNAVAILABLE;
        use voucher_events::CODE_VOUCHER_EVENTS_BAD_ARGUMENT;

        let db_connector = ctx.data::<Arc<DBConnector>>()?;
        if !db_connector.history_store_attached() {
            return Err(error(
                CODE_HISTORY_STORE_UNAVAILABLE,
                "this server has no history proofs store: ask a server that has one".to_string(),
            ));
        }
        let sk = voucher_events::parse_sk_u_commit(&sk_u_commit)
            .map_err(|err| error(CODE_VOUCHER_EVENTS_BAD_ARGUMENT, err))?;
        history_allowlist::check_selection(
            ctx.look_ahead(),
            history_allowlist::MESSAGE_FIELDS,
            &[("src_transaction", history_allowlist::TRANSACTION_FIELDS)],
        )?;
        let rows = db::history_store::EventDbRow::vouchers_by_sk_u_commit(db_connector, sk)
            .await
            .map_err(store_error)?;
        Ok(rows.into_iter().map(BlockchainMessage::from_stored_event).collect())
    }

    async fn transaction(&self, hash: String) -> Option<BlockchainTransaction> {
        let transaction_loader = self.ctx.data_unchecked::<DataLoader<TransactionLoader>>();
        let transaction =
            transaction_loader.load_one(hash).await.expect("Failed to load transaction");

        let mut transaction = transaction?;
        let selection_set = self.ctx.look_ahead().field("transaction");
        populate_transaction_tree(self.ctx, &mut transaction, selection_set, MAX_RELATION_DEPTH)
            .await
            .expect("Failed to populate transaction relations");

        Some(transaction)
    }

    #[graphql(name = "transactionByInMsg")]
    async fn transaction_by_in_msg(&self, hash: String) -> Option<BlockchainTransaction> {
        let projection = db::transaction::Transaction::graphql_transaction_projection(true);
        let transaction = db::transaction::Transaction::by_in_message(
            self.ctx.data::<Arc<DBConnector>>().unwrap(),
            &projection,
            &hash,
            None,
        )
        .await
        .expect("Failed to load transaction by inbound message");

        let mut transaction: BlockchainTransaction = transaction?.into();
        let selection_set = self.ctx.look_ahead().field("transactionByInMsg");
        populate_transaction_tree(self.ctx, &mut transaction, selection_set, MAX_RELATION_DEPTH)
            .await
            .expect("Failed to populate transaction relations");
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
        #[graphql(desc = "This field is mutually exclusive with 'last'.")] first: Option<i32>,
        after: Option<String>,
        #[graphql(desc = "This field is mutually exclusive with 'first'.")] last: Option<i32>,
        before: Option<String>,
    ) -> async_graphql::Result<
        Option<
            Connection<
                String,
                BlockchainTransaction,
                EmptyFields,
                EmptyFields,
                BlockchainTransactionsConnection,
                BlockchainTransactionsEdge,
            >,
        >,
    > {
        Ok(Some(
            query(
                after,
                before,
                first,
                last,
                |after: Option<String>, before: Option<String>, first, last| async move {
                    let args = BlockchainTransactionsQueryArgs {
                        min_balance_delta,
                        max_balance_delta,
                        pagination: PaginationArgs { first, after, last, before },
                    };
                    let selected_fields = selected_node_fields(self.ctx, "transactions");
                    let projection = db::transaction::Transaction::connection_projection_for_fields(
                        strip_cold_pruned_transaction_fields(self.ctx, selected_fields.clone()),
                    );
                    let mut transactions = db::transaction::Transaction::blockchain_transactions(
                        self.ctx.data::<Arc<DBConnector>>()?,
                        &projection,
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

                    let mut edges = Vec::new();
                    for transaction in transactions {
                        let mut transaction: BlockchainTransaction = transaction.into();
                        populate_transaction_tree(
                            self.ctx,
                            &mut transaction,
                            connection_node_selection(self.ctx.look_ahead().field("transactions")),
                            MAX_RELATION_DEPTH,
                        )
                        .await?;
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
            .map_err(crate::schema::db::connector::map_db_error)?,
        ))
    }
}
