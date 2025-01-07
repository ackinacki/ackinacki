use std::path::PathBuf;
use std::sync::LazyLock;

use clap::Parser;

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

    /// Overwrite existing files
    #[arg(short, long)]
    force: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    generate_certs(cli.name, cli.subjects, cli.output_dir, cli.force)?;

    Ok(())
}

fn generate_certs(
    name: String,
    subjects: Vec<String>,
    output_dir: Option<PathBuf>,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = output_dir.unwrap_or_else(|| PathBuf::from("./"));

    std::fs::create_dir_all(&output_dir)?;
    let client = rcgen::generate_simple_self_signed(subjects)?;

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

    std::fs::write(cert_filepath, client.cert.pem())?;
    std::fs::write(key_filepath, client.key_pair.serialize_pem())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::UnixTime;
    use rustls::server::WebPkiClientVerifier;

    use crate::generate_certs;

    #[test]
    fn test() {
        generate_certs(
            "client".to_string(),
            vec!["[::1]".to_string()],
            Some(PathBuf::from("certs")),
            true,
        )
        .unwrap();
        generate_certs(
            "server".to_string(),
            vec!["[::1]".to_string()],
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

        assert!(client_auth.verify_client_cert(&client_cert, &[], UnixTime::now()).is_ok());
    }
}
