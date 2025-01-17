// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use async_graphql::dataloader::DataLoader;
use async_graphql::http::playground_source;
use async_graphql::http::GraphQLPlaygroundConfig;
use async_graphql::http::GraphiQLSource;
use async_graphql::EmptySubscription;
use async_graphql::Schema;
use async_graphql_warp::GraphQLBadRequest;
use async_graphql_warp::GraphQLResponse;
use sqlx::Pool;
use sqlx::Sqlite;
use sqlx::SqlitePool;
use tokio::time;
use warp::http::Response as HttpResponse;
use warp::http::StatusCode;
use warp::Filter;
use warp::Rejection;

use crate::client::create_client;
use crate::defaults;
use crate::schema::graphql::block::BlockLoader;
use crate::schema::graphql::message::MessageLoader;
use crate::schema::graphql::mutations::NodeUrl;
use crate::schema::graphql::transaction::TransactionLoader;
use crate::schema::graphql::MutationRoot;
use crate::schema::graphql::QueryRoot;

async fn open_db(db_path: PathBuf) -> anyhow::Result<Pool<Sqlite>> {
    let db_path_str = db_path.display().to_string();
    let mut interval = time::interval(time::Duration::from_secs(3));
    let mut attempt: u16 = 0;
    let pool = loop {
        interval.tick().await;

        let res = SqlitePool::connect(&db_path_str)
            .await
            .with_context(|| format!("DB file: {db_path_str}"));

        match res {
            Ok(pool) => break pool,
            Err(err) => {
                if attempt >= 2 {
                    anyhow::bail!("Failed to open DB file {}: timeout", db_path_str);
                } else {
                    tracing::error!("{err:?}")
                }
            }
        }

        attempt += 1;
    };

    Ok(pool)
}

pub async fn start(bind_to: String, db_path: PathBuf) -> anyhow::Result<()> {
    let pool = open_db(db_path).await?;
    let node_url = match std::env::var("NODE_URL") {
        Ok(node_url) => node_url,
        _ => defaults::NODE_URL.to_string(),
    };
    let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(pool.clone())
        .data(NodeUrl::from(node_url))
        .data(create_client()?)
        .data(DataLoader::new(BlockLoader { pool: pool.clone() }, tokio::spawn))
        .data(DataLoader::new(MessageLoader { pool: pool.clone() }, tokio::spawn))
        .data(DataLoader::new(TransactionLoader { pool }, tokio::spawn))
        .with_sorted_fields()
        .finish();

    let graphql_post = async_graphql_warp::graphql(schema).and_then(
        |(schema, request): (
            Schema<QueryRoot, MutationRoot, EmptySubscription>,
            async_graphql::Request,
        )| async move {
            Ok::<_, Infallible>(GraphQLResponse::from(schema.execute(request).await))
        },
    );

    // let playground_graphql_cloned = playground_graphql.clone();
    let graphql_playground = warp::path!("graphql_old").and(warp::get()).map(move || {
        HttpResponse::builder()
            .header("content-type", "text/html")
            .body(playground_source(GraphQLPlaygroundConfig::new("")))
    });

    let graphiql = warp::path!("graphql").and(warp::get()).map(|| {
        HttpResponse::builder()
            .header("content-type", "text/html")
            .body(GraphiQLSource::build().endpoint("/graphql").finish())
    });

    let routes =
        graphql_post.or(graphql_playground).or(graphiql).recover(|err: Rejection| async move {
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
        });

    tracing::info!("Listening on: {}\n", bind_to);
    // tracing::info!("GraphQL Playground: {}", playground_graphql.clone());
    // tracing::info!("GraphQL IDE: {}", playground_graphql_ide);

    let socket_addr = bind_to.parse::<SocketAddr>()?;
    warp::serve(routes).run((socket_addr.ip(), socket_addr.port())).await;

    Ok(())
}
