// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::process::exit;
use std::sync::mpsc::channel;
use std::sync::LazyLock;
use std::thread;

use block_manager::signals::init_signals;
use clap::Parser;
use sqlx::migrate::MigrateDatabase;
use sqlx::Execute;
use sqlx::Sqlite;
use sqlx::SqlitePool;
use tokio::io::AsyncReadExt;
use url::Url;
use wtransport::ClientConfig;
use wtransport::Endpoint;

pub static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
{}
BUILD_GIT_BRANCH={}
BUILD_GIT_COMMIT={}
BUILD_GIT_DATE={}
BUILD_TIME={}",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_GIT_BRANCH"),
        env!("BUILD_GIT_COMMIT"),
        env!("BUILD_GIT_DATE"),
        env!("BUILD_TIME"),
    )
});

/// Acki-Nacki Lite Node
#[derive(Parser, Debug)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
struct Args {
    /// A string representing the URL of the HTTP/3 server to connect to. The
    /// scheme must be HTTPS, and the port number needs to be explicitly
    /// specified.
    #[arg(short, long)]
    pub stream_src_url: Url,

    /// URL for sqlite
    #[arg(env)]
    pub database_url: Url,
}

fn main() -> Result<(), std::io::Error> {
    dotenvy::dotenv().ok(); // ignore errors
    block_manager::tracing::init_tracing();
    tracing::info!("Tracing ON");
    let signals = init_signals()?;

    if let Err(err) = thread::Builder::new().name("tokio_main".into()).spawn(tokio_main)?.join() {
        tracing::error!("tokio main thread panicked: {:?}", err);
        exit(1);
    }

    if let Err(err) = signals.join() {
        tracing::error!("signal handler thread panicked: {:?}", err);
        exit(2);
    }

    exit(0);
}

#[tokio::main]
async fn tokio_main() -> anyhow::Result<()> {
    let args = Args::parse();

    if let Err(err) = execute(args).await {
        tracing::error!("{err}");
        exit(1);
    }

    exit(0);
}

async fn execute(args: Args) -> anyhow::Result<()> {
    tracing::info!("DATABASE_URL env var value is: {}", std::env::var("DATABASE_URL").unwrap());
    tracing::info!("Database url is: {}", args.database_url);
    if !Sqlite::database_exists(args.database_url.as_str()).await.unwrap_or(false) {
        tracing::info!("Creating database {}", args.database_url);
        match Sqlite::create_database(args.database_url.as_str()).await {
            Ok(_) => println!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        tracing::info!("Database already exists");
    }

    let db = SqlitePool::connect(args.database_url.as_str()).await.unwrap();
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let migrations = std::path::Path::new(&crate_dir).join("./migrations");
    tracing::info!("Running migrations from {:?}", migrations.as_path().canonicalize()?);
    let migration_results = sqlx::migrate::Migrator::new(migrations).await.unwrap().run(&db).await;
    match migration_results {
        Ok(_) => tracing::info!("Migration success"),
        Err(error) => {
            tracing::error!("{:?}", error);
            panic!("error: {}", error);
        }
    }
    tracing::info!("migration: {:?}", migration_results);

    let (tx, rx) = channel::<Vec<u8>>();

    let database = std::env::var("DATABASE_URL").expect("No DATABASE_URL env var");
    tracing::info!("database: {}", database);

    let db = SqlitePool::connect(args.database_url.as_str()).await.unwrap();

    let receive_handle = tokio::spawn(async move {
        loop {
            match rx.recv() {
                Ok(data) => {
                    let id = rand::random::<u64>().to_string();

                    let q =
                        sqlx::query!("INSERT INTO raw_blocks (id, data) VALUES (?, ?)", id, data);

                    tracing::info!("sql : {}", q.sql());

                    let _ = q.execute(&db).await.unwrap();
                }
                Err(err) => tracing::error!("Error receiving data: {}", err),
            };
        }
    });

    loop {
        if receive_handle.is_finished() {
            break;
        }
        let config = ClientConfig::builder() //
            .with_bind_default()
            .with_no_cert_validation()
            .build();

        tracing::info!(
            "Connecting to {} {:?}",
            args.stream_src_url.as_str(),
            args.stream_src_url.port(),
        );

        let Ok(connection) = Endpoint::client(config) //
            .expect("Constructor for client")
            .connect(args.stream_src_url.as_str())
            .await
        else {
            tracing::error!("connection error");
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            continue;
        };

        tracing::info!("Connection {:?} {:?}", connection.stable_id(), connection.session_id());

        loop {
            if receive_handle.is_finished() {
                break;
            }
            tracing::info!("Wait for incoming stream...");

            let Ok(mut stream) = connection.accept_uni().await else {
                tracing::warn!("Connection is closed");
                break;
            };
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::with_capacity(1024);
                stream.read_to_end(&mut buf).await.expect("stream read_to_end successful");

                tracing::info!("Received: {} bytes", buf.len());
                tx_clone.send(buf).unwrap();
            })
            .await
            .ok();
        }
    }

    Ok(())
}
