// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub use ed25519_dalek::SecretKey;
pub use ed25519_dalek::Signature;
pub use ed25519_dalek::SigningKey;
pub use ed25519_dalek::VerifyingKey;
pub use host_port::HostPort;
pub use rcgen::KeyPair;
pub use transport::NetConnection;
pub use transport::NetIncomingRequest;
pub use transport::NetListener;
pub use transport::NetTransport;

pub use crate::tls::contains_any_pubkey;
pub use crate::tls::create_self_signed_cert_with_ed_signatures;
pub use crate::tls::generate_self_signed_cert;
pub use crate::tls::get_pubkeys_from_cert_der;
pub use crate::tls::hex_verifying_key;
pub use crate::tls::hex_verifying_keys;
pub use crate::tls::pubkeys_info;
pub use crate::tls::resolve_signing_keys;
pub use crate::tls::verify_cert;
pub use crate::tls::CertHash;
pub use crate::tls::NetCredential;
pub use crate::tls::TlsCertCache;

mod host_port;
pub mod msquic;
mod pkcs12;
pub mod server;
mod tls;
mod transport;
mod utils;
