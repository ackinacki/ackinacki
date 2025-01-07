use std::net::SocketAddr;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::PrivateKeyDer;
use rustls::server::WebPkiClientVerifier;
use tokio::sync::watch;
use tracing::error;
use tracing::info;
use wtransport::endpoint::IncomingSession;
use wtransport::Endpoint;
use wtransport::ServerConfig;

use crate::config::store::ConfigStore;

const PROXY_CONNECTION_ALIVE_TIMEOUT: Duration = Duration::from_millis(300);

pub fn server_tls_config(config: &ConfigStore) -> anyhow::Result<rustls::ServerConfig> {
    let server_cert = config.config.server.cert.cert.clone();
    let private_key = PrivateKeyDer::try_from(config.config.server.key.key.secret_der().to_vec())
        .map_err(|err| anyhow::anyhow!("{err}"))?;

    let mut client_certs = Vec::new();
    for connection in config.config.connections.values() {
        client_certs.push(connection.cert.cert.clone());
    }

    let mut root_store = rustls::RootCertStore::empty();
    for cert in client_certs {
        root_store.add(cert.der().into()).unwrap();
    }
    let root_store = Arc::new(root_store);

    let tls_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(WebPkiClientVerifier::builder(root_store).build().unwrap())
        .with_single_cert(vec![server_cert.der().to_vec().into()], private_key)
        .expect("tls config error");

    Ok(tls_config)
}

fn generate_server_config(bind: SocketAddr, config: &ConfigStore) -> anyhow::Result<ServerConfig> {
    Ok(ServerConfig::builder() //
        .with_bind_address(bind)
        .with_custom_tls(server_tls_config(config)?)
        .keep_alive_interval(Some(PROXY_CONNECTION_ALIVE_TIMEOUT))
        .max_idle_timeout(Some(PROXY_CONNECTION_ALIVE_TIMEOUT * 2))?
        .build())
}

pub async fn quic_session_endpoint(
    bind: SocketAddr,
    session_pub: Sender<IncomingSession>,
    mut config_sub: watch::Receiver<ConfigStore>,
) -> anyhow::Result<()> {
    let config_store = config_sub.borrow_and_update().clone();
    let config = generate_server_config(bind, &config_store)?;
    let server = Endpoint::server(config)?;
    info!("Proxy subscribers started on port {}", bind.port());

    loop {
        tokio::select! {
            v = config_sub.changed() => {
                match v {
                    Ok(()) => {
                        info!("Config changed");
                        let config = generate_server_config(bind, &config_sub.borrow_and_update())?;
                        server.reload_config(config, false)?;
                        info!("Config reloaded");
                    }
                    Err(err) => {
                        error!("Config watcher error: {}", err);
                    }
                }
            }
            v = server.accept() => {
                info!("New session incoming");
                session_pub.send(v)?;
            }
        };
    }
}
