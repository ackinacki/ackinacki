// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::str::FromStr;

use crate::bls::BLSSignatureScheme;
use crate::types::RndSeed;

pub fn key_pairs_from_file<T>(keys_path: &str) -> HashMap<T::PubKey, (T::Secret, RndSeed)>
where
    T: BLSSignatureScheme,
    T::PubKey: FromStr + Sized + Hash + Eq,
    <T::PubKey as FromStr>::Err: Debug,
    T::Secret: FromStr + Sized,
    <T::Secret as FromStr>::Err: Debug,
{
    tracing::trace!("Reading keys from file: {}", keys_path);
    let json =
        std::fs::read_to_string(keys_path).expect("Should be able to read file with keypair");
    let map = serde_json::from_str::<Vec<HashMap<String, String>>>(&json)
        .expect("File should contain vec of map of keys");
    map.into_iter()
        .map(|json_dict| {
            let public = json_dict.get("public").expect("Failed to read public key from keys");
            let secret = json_dict.get("secret").expect("Failed to read secret key from keys");
            let rnd_seed = json_dict.get("rnd").expect("Failed to read random seed from keys");
            let public = T::PubKey::from_str(public).expect("Failed to decode public key");
            let secret = T::Secret::from_str(secret).expect("Failed to decode secret key");
            let rnd_seed = RndSeed::from_str(rnd_seed).expect("Failed to decode rnd seed");
            (public, (secret, rnd_seed))
        })
        .collect()
}
