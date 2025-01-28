use std::process::exit;
use std::thread;

use clap::Parser;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
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
    let cert = CertificateDer::from_pem_file("certs/server.ca.pem").unwrap();
    let client_cert = CertificateDer::from_pem_file("certs/client.ca.pem").unwrap();
    let private_key = PrivateKeyDer::from_pem_file("certs/client.key.pem").unwrap();

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert).unwrap();

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![client_cert], private_key)
        .expect("tls config");

    loop {
        let config =
            ClientConfig::builder().with_bind_default().with_custom_tls(tls_config.clone()).build();

        tracing::info!("Connecting to {}", args.endpoint.as_str());

        let connection = match Endpoint::client(config)
            .expect("endpoint client")
            .connect(&args.endpoint)
            .await
        {
            Ok(connection) => connection,
            Err(err) => {
                tracing::error!("connection error: {err:#?}");
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                continue;
            }
        };

        tracing::info!("Connection {:?} {:?}", connection.stable_id(), connection.session_id());

        // dummy reader
        tokio::spawn({
            let connection = connection.clone();
            async move {
                let mut buf = Vec::with_capacity(1024);
                loop {
                    let Ok(mut stream) = connection.accept_uni().await else {
                        tracing::warn!("Connection is closed");
                        break;
                    };
                    let n =
                        stream.read_to_end(&mut buf).await.expect("stream read_to_end successful");

                    tracing::info!("Received: {} bytes", n);
                    tracing::info!("Received: {} bytes to buffer", buf.len());
                }
            }
        });

        loop {
            tracing::info!("Sleep 300ms");
            tokio::time::sleep(std::time::Duration::from_millis(290)).await;
            tracing::info!("Wait for incoming stream...");

            let Ok(opening_uni_stream) = connection.open_uni().await else {
                tracing::warn!("Connection is closed");
                break;
            };
            tracing::info!("Prepare to open uni stream");

            let Ok(mut send_stream) = opening_uni_stream.await else {
                tracing::warn!("Connection is closed");
                break;
            };
            tracing::info!("Streamed opened");

            let buf = Vec::from(0x_DE_AD_BE_EF_u32.to_be_bytes());
            let buf = buf.repeat(200_000);
            send_stream.write_all(&buf).await?;
            send_stream.finish().await?;
            tracing::info!("Data sent");
        }
    }
}
