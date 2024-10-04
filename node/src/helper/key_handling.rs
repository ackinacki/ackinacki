// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;

use crate::bls::BLSSignatureScheme;

pub fn key_pair_from_file<T>(keys_path: String) -> (T::PubKey, T::Secret)
where
    T: BLSSignatureScheme,
    T::PubKey: FromStr + Sized,
    <T::PubKey as FromStr>::Err: Debug,
    T::Secret: FromStr + Sized,
    <T::Secret as FromStr>::Err: Debug,
{
    tracing::trace!("Reading keys from file: {}", keys_path);
    let json =
        std::fs::read_to_string(keys_path).expect("Should be able to read file with keypair");
    let map = serde_json::from_str::<HashMap<String, String>>(&json)
        .expect("File should contain map of keys");
    let public = map.get("public").expect("Failed to read public key from keys");
    let secret = map.get("secret").expect("Failed to read secret key from keys");
    (
        T::PubKey::from_str(public).expect("Failed to decode public key"),
        T::Secret::from_str(secret).expect("Failed to decode secret key"),
    )
}
