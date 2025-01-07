static DEFAULT_TLS_SUBJECT: [&str; 3] = ["localhost", "127.0.0.1", "[::1]"];

pub fn generate_tls_keypair() -> anyhow::Result<rcgen::KeyPair> {
    let cert = rcgen::generate_simple_self_signed(DEFAULT_TLS_SUBJECT.map(String::from).to_vec())?;
    Ok(cert.key_pair)
}
