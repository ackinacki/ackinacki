// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::Context;
use async_graphql::dataloader::DataLoader;
use async_graphql::http::playground_source;
use async_graphql::http::GraphQLPlaygroundConfig;
use async_graphql::http::GraphiQLSource;
use async_graphql::EmptyMutation;
use async_graphql::EmptySubscription;
use async_graphql::Schema;
use async_graphql_warp::GraphQLBadRequest;
use async_graphql_warp::GraphQLResponse;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Pool;
use sqlx::Sqlite;
use tvm_client::ClientContext;
use warp::http::Response as HttpResponse;
use warp::http::StatusCode;
use warp::Filter;
use warp::Rejection;

use crate::metrics::GqlServerMetrics;
use crate::schema::db::DBConnector;
use crate::schema::graphql::block::BlockLoader;
use crate::schema::graphql::message::MessageLoader;
use crate::schema::graphql::transaction::TransactionLoader;
use crate::schema::graphql_ext;
use crate::schema::graphql_ext::DeprecatedApiEnabled;

pub async fn open_db(
    db_path: PathBuf,
    max_connections: u32,
    acquire_timeout: std::time::Duration,
    pragmas: String,
) -> anyhow::Result<Pool<Sqlite>> {
    let db_path_str = db_path.display().to_string();
    let connect_string = format!("{db_path_str}?mode=ro");

    let connect_options = SqliteConnectOptions::from_str(&connect_string)?.create_if_missing(false);

    tracing::info!(pragmas = pragmas.as_str(), "SQLite read PRAGMAs");

    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(acquire_timeout)
        .after_connect(move |conn, _meta| {
            let pragmas = pragmas.clone();
            Box::pin(async move {
                sqlx::raw_sql(sqlx::AssertSqlSafe(pragmas)).execute(&mut *conn).await?;
                // TODO: remove after verifying PRAGMAs in production
                tracing::info!("Applied SQLite read PRAGMAs to new connection");
                Ok(())
            })
        })
        .connect_with(connect_options)
        .await
        .with_context(|| format!("DB file: {db_path_str}"))?;

    Ok(pool)
}

pub async fn start(
    bind_to: String,
    db_connector: Arc<DBConnector>,
    sdk_client: Arc<ClientContext>,
    metrics: Option<GqlServerMetrics>,
    deprecated_api: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let socket_addr = bind_to.parse::<SocketAddr>()?;

    let graphql_playground = warp::path!("graphql_old").and(warp::get()).map(move || {
        HttpResponse::builder()
            .header("content-type", "text/html")
            .body(playground_source(GraphQLPlaygroundConfig::new("")))
    });

    let graphql_options = warp::path("graphql")
        .and(warp::options())
        .map(|| warp::reply::with_status("", StatusCode::NO_CONTENT));

    let graphiql = warp::path!("graphql").and(warp::get()).map(|| {
        HttpResponse::builder()
            .header("content-type", "text/html")
            .body(GraphiQLSource::build().endpoint("/graphql").finish())
    });

    let schema = Schema::build(graphql_ext::QueryRoot, EmptyMutation, EmptySubscription)
        .data(Arc::clone(&db_connector))
        .data(sdk_client)
        .data(DeprecatedApiEnabled(Arc::clone(&deprecated_api)))
        .data(DataLoader::new(
            BlockLoader { db_connector: Arc::clone(&db_connector) },
            tokio::spawn,
        ))
        .data(DataLoader::new(
            MessageLoader { db_connector: Arc::clone(&db_connector) },
            tokio::spawn,
        ))
        .data(DataLoader::new(
            TransactionLoader { db_connector: Arc::clone(&db_connector) },
            tokio::spawn,
        ))
        .with_sorted_fields()
        .finish();

    let graphql_post = async_graphql_warp::graphql(schema).and_then(
        move |(schema, request): (
            Schema<graphql_ext::QueryRoot, EmptyMutation, EmptySubscription>,
            async_graphql::Request,
        )| {
            let metrics = metrics.clone();
            async move {
                let start = std::time::Instant::now();
                let response = schema.execute(request).await;
                if let Some(m) = &metrics {
                    if response.errors.is_empty() {
                        m.report_query_success(start.elapsed().as_millis() as u64);
                    } else {
                        tracing::trace!("request error: {:?}", response.errors);
                        m.report_query_error();
                    }
                }
                Ok::<_, Infallible>(GraphQLResponse::from(response))
            }
        },
    );

    let routes = graphql_post.or(graphql_options).or(graphql_playground).or(graphiql).recover(
        |err: Rejection| async move {
            if let Some(GraphQLBadRequest(err)) = err.find() {
                return Ok::<_, Infallible>(warp::reply::with_status(
                    err.to_string(),
                    StatusCode::BAD_REQUEST,
                ));
            }

            Ok(warp::reply::with_status(
                "INTERNAL_SERVER_ERROR".to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        },
    );

    tracing::info!("[API:extended] Listening on: {}\n", bind_to);
    warp::serve(routes).run((socket_addr.ip(), socket_addr.port())).await;

    Ok(())
}
