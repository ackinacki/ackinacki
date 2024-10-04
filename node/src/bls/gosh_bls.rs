// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::Bytes;

use crate::bls::BLSSignatureScheme;

#[derive(Default, Clone)]
pub struct GoshBLS {}

#[serde_as]
#[derive(Deserialize, Serialize, Clone)]
pub struct Signature(#[serde_as(as = "Bytes")] [u8; gosh_bls_lib::bls::BLS_SIG_LEN]);

impl Default for Signature {
    fn default() -> Self {
        Self::empty()
    }
}

impl Signature {
    pub fn empty() -> Self {
        Signature([0u8; gosh_bls_lib::bls::BLS_SIG_LEN])
    }
}

#[serde_as]
#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct PubKey(#[serde_as(as = "Bytes")] [u8; gosh_bls_lib::bls::BLS_PUBLIC_KEY_LEN]);

impl Debug for PubKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl Default for PubKey {
    fn default() -> Self {
        PubKey([0u8; gosh_bls_lib::bls::BLS_PUBLIC_KEY_LEN])
    }
}

impl From<[u8; gosh_bls_lib::bls::BLS_PUBLIC_KEY_LEN]> for PubKey {
    fn from(value: [u8; gosh_bls_lib::bls::BLS_PUBLIC_KEY_LEN]) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for PubKey {
    fn from(value: &[u8]) -> Self {
        let data = <[u8; gosh_bls_lib::bls::BLS_PUBLIC_KEY_LEN]>::try_from(value)
            .expect("Failed to load pubkey from slice");
        Self(data)
    }
}

impl FromStr for PubKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut pubkey = [0_u8; gosh_bls_lib::bls::BLS_PUBLIC_KEY_LEN];
        hex::decode_to_slice(s, &mut pubkey)?;
        Ok(pubkey.into())
    }
}

impl AsRef<[u8]> for PubKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[serde_as]
#[derive(Deserialize, Serialize, Clone)]
pub struct Secret(#[serde_as(as = "Bytes")] [u8; gosh_bls_lib::bls::BLS_SECRET_KEY_LEN]);

impl Secret {
    pub fn take_as_seed(&self) -> [u8; 32] {
        self.0
    }
}

impl Debug for Secret {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl Default for Secret {
    fn default() -> Self {
        Secret([0u8; gosh_bls_lib::bls::BLS_SECRET_KEY_LEN])
    }
}

impl From<[u8; gosh_bls_lib::bls::BLS_SECRET_KEY_LEN]> for Secret {
    fn from(value: [u8; gosh_bls_lib::bls::BLS_SECRET_KEY_LEN]) -> Self {
        Secret(value)
    }
}

impl FromStr for Secret {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut pubkey = [0_u8; gosh_bls_lib::bls::BLS_SECRET_KEY_LEN];
        hex::decode_to_slice(s, &mut pubkey)?;
        Ok(pubkey.into())
    }
}

impl BLSSignatureScheme for GoshBLS {
    type PubKey = PubKey;
    type Secret = Secret;
    type Signature = Signature;

    fn sign<TData: Serialize>(
        secret: &Self::Secret,
        data: &TData,
    ) -> anyhow::Result<Self::Signature> {
        let buffer = bincode::serialize(&data)?;
        tracing::trace!("Sign data: {:?}", secret);
        gosh_bls_lib::bls::sign(&secret.0, &buffer)
            .map(|e| -> Self::Signature { Signature(e) })
            .map_err(|e| -> anyhow::Error {
                log::error!("{}", e);
                anyhow::anyhow!("Bls signatures inner signing process has failed")
            })
    }

    fn verify<TData: Serialize>(
        signature: &Self::Signature,
        pubkeys_occurrences: &mut dyn Iterator<Item = &(Self::PubKey, usize)>,
        data: &TData,
    ) -> anyhow::Result<bool> {
        let start = std::time::Instant::now();
        tracing::trace!("signature verification: start");
        let mut flattened_pubkeys: Vec<&[u8; gosh_bls_lib::bls::BLS_PUBLIC_KEY_LEN]> = vec![];
        for (pubkey, occurrences) in pubkeys_occurrences {
            for _i in 0..*occurrences {
                flattened_pubkeys.push(&pubkey.0);
            }
        }
        tracing::trace!("signature verification: flatten pubkeys: {}", start.elapsed().as_millis());
        let aggregated_public_key = gosh_bls_lib::bls::aggregate_public_keys(&flattened_pubkeys)
            .map_err(|e| -> anyhow::Error {
                anyhow::anyhow!("Pubkey aggregation failed: {}", e)
            })?;
        tracing::trace!(
            "signature verification: aggregate pubkeys: {}",
            start.elapsed().as_millis()
        );
        let buffer = bincode::serialize(&data)?;
        tracing::trace!("signature verification: serialize data: {}", start.elapsed().as_millis());
        let is_valid = gosh_bls_lib::bls::verify(&signature.0, &buffer, &aggregated_public_key)
            .map_err(|_e| -> anyhow::Error {
                anyhow::anyhow!("Bls signatures inner verification process has failed")
            })?;
        tracing::trace!("signature verification: finish: {}", start.elapsed().as_millis());
        Ok(is_valid)
    }

    fn merge(one: &Self::Signature, another: &Self::Signature) -> anyhow::Result<Self::Signature> {
        // Note:
        // This is a bullshit.
        // On one hand gosh_bls_lib has unbearable interface
        // On the other hand it uses absolutely undocumented blst lib.
        // In this case it's hard to make a choice on what to use.
        // --
        // In this situation gosh_bls_lib has method to merge 2 aggregated signatures
        // together. However, this method requires SERIALIZED objects! Instead of
        // a signature and a map of occurrences for each signer.
        // As a workaround I'm going to attach fake occurrences info "preserialized"
        // and ignore that part merged.
        let mut sig1 = one.0.to_vec();
        sig1.extend_from_slice(&2u16.to_be_bytes());
        sig1.extend_from_slice(&0u16.to_be_bytes());
        sig1.extend_from_slice(&1u16.to_be_bytes());
        let mut sig2 = another.0.to_vec();
        sig2.extend_from_slice(&2u16.to_be_bytes());
        sig2.extend_from_slice(&0u16.to_be_bytes());
        sig2.extend_from_slice(&1u16.to_be_bytes());

        let merged_signature_buffer: Vec<u8> = gosh_bls_lib::bls::aggregate_two_bls_signatures(
            &sig1, &sig2,
        )
        .map_err(|e| -> anyhow::Error {
            anyhow::anyhow!("BLS signatures inner merge process has failed: {}", e)
        })?;
        // Adding assertion in case of merging algorithm changes after upgrade or
        // whatever.
        let n = gosh_bls_lib::bls::BLS_SIG_LEN;
        let nodes_info = &merged_signature_buffer[n..n + 6];
        assert_eq!(nodes_info, &[0u8, 2u8, 0u8, 0u8, 0u8, 2u8]);

        let merged: [u8; gosh_bls_lib::bls::BLS_SIG_LEN] =
            merged_signature_buffer[..n].try_into().map_err(|_e| -> anyhow::Error {
                anyhow::anyhow!("BLS signature buffer size mismatch")
            })?;
        Ok(Signature(merged))
    }
}
