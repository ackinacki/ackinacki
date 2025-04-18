// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
//! Usage: cargo run --example keygen
use rcgen::generate_simple_self_signed;

fn main() {
    let cert =
        generate_simple_self_signed(vec!["0.0.0.0".into(), "127.0.0.1".into(), "localhost".into()])
            .unwrap();
    std::fs::write("./network/certs/cert.der", cert.cert.der()).unwrap();
    std::fs::write("./network/certs/key.der", cert.key_pair.serialized_der()).unwrap();
}
