use crate::tls::client_tls_config;
use crate::NetCredential;

pub fn create_client_config(
    is_debug: bool,
    credential: &NetCredential,
    alpn_preferred: &[&str],
) -> Result<wtransport::ClientConfig, anyhow::Error> {
    let tls_config = client_tls_config(is_debug, credential, alpn_preferred)?;
    Ok(wtransport::ClientConfig::builder().with_bind_default().with_custom_tls(tls_config).build())
}
