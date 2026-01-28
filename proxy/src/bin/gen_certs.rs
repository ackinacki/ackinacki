use std::path::PathBuf;
use std::sync::LazyLock;

use base64::Engine;
use clap::Parser;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use transport_layer::generate_self_signed_cert;

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

// X.509 certificate generator
#[derive(Parser, Debug)]
#[command(author, long_version = &**LONG_VERSION, about, long_about = None)]
struct Cli {
    /// Comma-separated list of certificate subjects (e.g. 'localhost,*.example.com,127.0.0.1')
    #[arg(short, long, value_delimiter = ',')]
    subjects: Vec<String>,

    /// Certificate name (e.g. client)
    #[arg(short, long)]
    name: String,

    /// Output directory (default: current directory)
    #[arg(short, long)]
    output_dir: Option<PathBuf>,

    /// Optional secret key of the block keeper's owner wallet key pair.
    /// Should be represented as a 64-char hex.
    /// If specified, then ed_key_path should be omitted.
    #[arg(long)]
    ed_key_secret: Vec<String>,

    /// Optional path to the block keeper's owner wallet key file.
    /// Should be stored as json `{ "public": "64-char hex", "secret": "64-char hex" }`.
    /// If specified, then ed_key_secret should be omitted.
    #[arg(long)]
    ed_key_path: Vec<String>,

    /// Overwrite existing files
    #[arg(short, long)]
    force: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let ed_sign_keys = transport_layer::resolve_signing_keys(&cli.ed_key_secret, &cli.ed_key_path)?;

    generate_certs(cli.name, cli.subjects, &ed_sign_keys, cli.output_dir, cli.force)?;

    Ok(())
}

fn generate_certs(
    name: String,
    subjects: Vec<String>,
    ed_sing_keys: &[transport_layer::SigningKey],
    output_dir: Option<PathBuf>,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = output_dir.unwrap_or_else(|| PathBuf::from("./"));

    std::fs::create_dir_all(&output_dir)?;
    let (key, cert) = generate_self_signed_cert(Some(subjects), ed_sing_keys)?;

    let cert_filepath = output_dir.join(format!("{name}.ca.pem"));
    let key_filepath = output_dir.join(format!("{name}.key.pem"));

    if !force {
        if cert_filepath.exists() {
            eprintln!(
                "File already exists: {} (to overwrite use --force)",
                cert_filepath.to_string_lossy()
            );
            std::process::exit(1);
        }
        if key_filepath.exists() {
            eprintln!(
                "File already exists: {} (to overwrite use --force)",
                key_filepath.to_string_lossy()
            );
            std::process::exit(1);
        }
    }

    std::fs::write(cert_filepath, cert_der_to_pem(&cert))?;
    std::fs::write(key_filepath, key_der_to_pem(&key))?;

    Ok(())
}

fn cert_der_to_pem(cert: &CertificateDer<'_>) -> String {
    der_to_pem("CERTIFICATE", cert.as_ref())
}

fn key_der_to_pem(key: &PrivateKeyDer<'_>) -> String {
    der_to_pem("PRIVATE KEY", key.secret_der())
}

fn der_to_pem(name: &str, der: &[u8]) -> String {
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut pem = String::new();
    pem.push_str(&format!("-----BEGIN {name}-----\n"));

    for chunk in b64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap());
        pem.push('\n');
    }

    pem.push_str(&format!("-----END {name}-----\n"));
    pem
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;

    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::UnixTime;
    use rustls::server::WebPkiClientVerifier;
    use transport_layer::verify_cert;

    use crate::generate_certs;

    #[test]
    fn test() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client_ed_sign_key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
        let unknown_ed_sign_key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
        generate_certs(
            "client".to_string(),
            vec!["[::1]".to_string()],
            std::slice::from_ref(&client_ed_sign_key),
            Some(PathBuf::from("certs")),
            true,
        )
        .unwrap();
        generate_certs(
            "server".to_string(),
            vec!["[::1]".to_string()],
            &[],
            Some(PathBuf::from("certs")),
            true,
        )
        .unwrap();

        let _client_key = PrivateKeyDer::from_pem_file("certs/client.key.pem").unwrap();
        let server_key = PrivateKeyDer::from_pem_file("certs/server.key.pem").unwrap();

        let client_cert = CertificateDer::from_pem_file("certs/client.ca.pem").unwrap();
        let server_cert = CertificateDer::from_pem_file("certs/server.ca.pem").unwrap();

        let mut client_root_store = rustls::RootCertStore::empty();
        client_root_store.add(client_cert.clone()).unwrap();

        let mut server_root_store = rustls::RootCertStore::empty();
        server_root_store.add(server_cert.clone()).unwrap();

        let client_root_store = Arc::new(client_root_store);
        let client_auth = WebPkiClientVerifier::builder(client_root_store.clone()).build().unwrap();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(client_cert.clone()).unwrap();

        let roots = Arc::new(root_store);
        let _server_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(WebPkiClientVerifier::builder(roots).build().unwrap())
            .with_single_cert(vec![server_cert.clone()], server_key)
            .expect("server config");

        assert!(verify_cert(
            &client_cert,
            &HashSet::default(),
            &HashSet::from_iter([client_ed_sign_key.verifying_key()].into_iter())
        )
        .is_ok());
        assert!(verify_cert(
            &client_cert,
            &HashSet::default(),
            &HashSet::from_iter([unknown_ed_sign_key.verifying_key()].into_iter())
        )
        .is_err());
        assert!(client_auth.verify_client_cert(&client_cert, &[], UnixTime::now()).is_ok());
    }
}
