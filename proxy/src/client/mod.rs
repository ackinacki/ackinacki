use std::fs::File;
use std::io::BufReader;
use std::process::exit;
use std::thread;

use clap::Parser;
use rustls::version::TLS13;
use tokio::io::AsyncReadExt;
use wtransport::ClientConfig;
use wtransport::Endpoint;

pub mod cli;

pub fn run() -> Result<(), std::io::Error> {
    dotenvy::dotenv().ok(); // ignore all errors and load what we can
    crate::tracing::init_tracing();
    tracing::info!("Starting...");

    tracing::debug!("Installing default crypto provider...");
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    if let Err(err) = thread::Builder::new().name("tokio_main".into()).spawn(tokio_main)?.join() {
        tracing::error!("tokio main thread panicked: {:#?}", err);
        exit(1);
    }

    exit(0);
}

#[tokio::main]
async fn tokio_main() -> anyhow::Result<()> {
    let args = cli::CliArgs::parse();

    tracing::info!("Endpoint: {}", args.endpoint);

    // NOTE: doesn't catch panic!
    if let Err(err) = execute(args).await {
        tracing::error!("{err}");
        exit(1);
    }

    exit(0);
}

async fn execute(args: cli::CliArgs) -> anyhow::Result<()> {
    let cert = {
        let mut rb = BufReader::new(
            File::open("certs/server.ca.pem").expect("failed to read server.ca.pem"),
        );
        let mut iter = rustls_pemfile::certs(&mut rb);
        iter.next().unwrap().unwrap()
    };

    let client_cert = {
        let mut rb = BufReader::new(
            File::open("certs/client.ca.pem").expect("failed to read client.ca.pem"),
        );
        let mut iter = rustls_pemfile::certs(&mut rb);
        iter.next().unwrap().unwrap()
    };

    let private_key = {
        let mut rb = BufReader::new(
            File::open("certs/client.key.pem").expect("failed to read client.key.pem"),
        );
        rustls_pemfile::private_key(&mut rb).into_iter().next().unwrap().unwrap()
    };

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert).unwrap();

    // let tls_config =
    //     rustls::ClientConfig::builder().with_root_certificates(root_store).
    let tls_config = rustls::ClientConfig::builder_with_protocol_versions(&[&TLS13])
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![client_cert], private_key)
        .expect("tls config");

    loop {
        let config = ClientConfig::builder() //
            .with_bind_default()
            .with_custom_tls(tls_config.clone())
            .build();

        tracing::info!("Connecting to {}", args.endpoint.as_str());

        let Ok(connection) = Endpoint::client(config) //
            .expect("endpoint client")
            .connect(&args.endpoint)
            .await
        else {
            tracing::error!("connection error");
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            continue;
        };

        tracing::info!("Connection {:?} {:?}", connection.stable_id(), connection.session_id());

        loop {
            tracing::info!("Wait for incoming stream...");

            let Ok(mut stream) = connection.accept_uni().await else {
                tracing::warn!("Connection is closed");
                break;
            };
            tokio::spawn(async move {
                // TODO: use stack instead of heap
                let mut buf = Vec::with_capacity(1024);
                let n = stream.read_to_end(&mut buf).await.expect("stream read_to_end successful");

                tracing::info!("Received: {} bytes", n);
                tracing::info!("Received: {} bytes to buffer", buf.len());
            })
            .await
            .ok();
        }
    }
}
