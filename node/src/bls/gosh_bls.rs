// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::str::FromStr;

use gosh_blst::min_pk::SecretKey;
use gosh_blst::BLS_PUBLIC_KEY_LEN;
use gosh_blst::BLS_SECRET_KEY_LEN;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;

use crate::bls::BLSSignatureScheme;
pub const DST: [u8; 43] = *b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";

#[derive(Default, Clone, PartialEq, Eq)]
pub struct GoshBLS {}

impl GoshBLS {
    pub fn merge_all(
        signatures: &[<GoshBLS as BLSSignatureScheme>::Signature],
    ) -> anyhow::Result<<GoshBLS as BLSSignatureScheme>::Signature> {
        let mut sig_refs: Vec<&gosh_blst::min_pk::Signature> = Vec::new();
        for sig in signatures {
            sig_refs.push(&sig.0);
        }
        match gosh_blst::min_pk::AggregateSignature::aggregate(sig_refs.as_slice(), true) {
            Ok(agg) => std::result::Result::Ok(Signature(agg.to_signature())),
            Err(err) => {
                Err(anyhow::anyhow!("BLS signatures inner merge process has failed: {:?}", err))
            }
        }
    }
}

#[serde_as]
#[derive(Hash, Deserialize, Serialize, Clone, PartialEq, Eq, Debug)]
pub struct Signature(gosh_blst::min_pk::Signature);

#[cfg(test)]
impl Default for Signature {
    fn default() -> Self {
        Self::empty()
    }
}

impl Signature {
    #[cfg(test)]
    pub fn empty() -> Self {
        use gosh_blst::BLS_SIG_LEN;

        let sig_bytes: [u8; BLS_SIG_LEN] = [
            165, 26, 40, 232, 168, 169, 142, 204, 6, 181, 28, 10, 149, 47, 2, 30, 2, 161, 245, 105,
            68, 162, 93, 249, 83, 224, 116, 184, 135, 221, 173, 34, 2, 200, 94, 47, 153, 222, 141,
            100, 22, 234, 22, 123, 39, 146, 7, 177, 8, 18, 97, 132, 146, 74, 87, 4, 40, 22, 228,
            171, 239, 149, 252, 89, 37, 51, 59, 198, 82, 193, 211, 164, 142, 26, 194, 163, 0, 32,
            65, 38, 176, 82, 222, 212, 194, 233, 215, 236, 248, 252, 59, 109, 95, 42, 143, 245,
        ]; // fixed signature priduced using secret key based on zero key material and zero 32-bytes msg
        Signature(gosh_blst::min_pk::Signature::from_bytes(&sig_bytes).unwrap())
    }
}

#[serde_as]
#[derive(Hash, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct PubKey(gosh_blst::min_pk::PublicKey);

impl Debug for PubKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let hexed_key = hex::encode(self.0.to_bytes());
        let (prefix, data) = hexed_key.split_at(4);
        let (_, postfix) = data.split_at(88);
        write!(f, "{prefix}...{postfix}")
    }
}

#[cfg(test)]
impl Default for PubKey {
    fn default() -> Self {
        let pk_bytes: [u8; BLS_PUBLIC_KEY_LEN] = [
            166, 149, 173, 50, 93, 252, 126, 17, 145, 251, 201, 241, 134, 245, 142, 255, 66, 166,
            52, 2, 151, 49, 177, 131, 128, 255, 137, 191, 66, 196, 100, 164, 44, 184, 202, 85, 178,
            0, 240, 81, 245, 127, 30, 24, 147, 198, 135, 89,
        ]; // fixed public key from zero key material
        PubKey(gosh_blst::min_pk::PublicKey::from_bytes(&pk_bytes).unwrap())
    }
}

impl From<gosh_blst::min_pk::PublicKey> for PubKey {
    fn from(value: gosh_blst::min_pk::PublicKey) -> Self {
        Self(value)
    }
}

impl From<[u8; BLS_PUBLIC_KEY_LEN]> for PubKey {
    fn from(value: [u8; BLS_PUBLIC_KEY_LEN]) -> Self {
        Self(gosh_blst::min_pk::PublicKey::from_bytes(&value).expect("BLST can not parse pubkey"))
    }
}

impl From<&[u8]> for PubKey {
    fn from(value: &[u8]) -> Self {
        let data =
            <[u8; BLS_PUBLIC_KEY_LEN]>::try_from(value).expect("Failed to load pubkey from slice");
        Self(gosh_blst::min_pk::PublicKey::from_bytes(&data).expect("BLST can not parse pubkey"))
    }
}

impl FromStr for PubKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut pubkey = [0_u8; BLS_PUBLIC_KEY_LEN];
        hex::decode_to_slice(s, &mut pubkey)?;
        match gosh_blst::min_pk::PublicKey::from_bytes(&pubkey) {
            Ok(pk) => Ok(PubKey(pk)),
            Err(err) => Err(anyhow::anyhow!("BLST can not parse pubkey: {:?}", err)),
        }
    }
}

impl AsRef<gosh_blst::min_pk::PublicKey> for PubKey {
    fn as_ref(&self) -> &gosh_blst::min_pk::PublicKey {
        &self.0
    }
}

#[serde_as]
#[derive(Deserialize, Serialize, Clone)]
pub struct Secret(gosh_blst::min_pk::SecretKey);

impl Secret {
    pub fn take_as_seed(&self) -> [u8; 32] {
        self.0.to_bytes()
    }
}

#[cfg(test)]
impl Default for Secret {
    fn default() -> Self {
        let sk_bytes: [u8; BLS_SECRET_KEY_LEN] = [
            77, 18, 154, 25, 223, 134, 160, 245, 52, 91, 173, 76, 198, 242, 73, 236, 42, 129, 156,
            204, 51, 134, 137, 91, 235, 79, 125, 152, 179, 219, 98, 53,
        ]; // fixed  secretkey from zero key material
        Secret(SecretKey::from_bytes(&sk_bytes).unwrap())
        // Secret(SecretKey::from_bytes(&[0u8; BLS_SECRET_KEY_LEN]).unwrap())
    }
}

impl From<[u8; BLS_SECRET_KEY_LEN]> for Secret {
    fn from(value: [u8; BLS_SECRET_KEY_LEN]) -> Self {
        Secret(SecretKey::from_bytes(&value).expect("BLST can not parse secretkey"))
    }
}

impl FromStr for Secret {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut secretkey = [0_u8; BLS_SECRET_KEY_LEN];
        hex::decode_to_slice(s, &mut secretkey)?;
        match gosh_blst::min_pk::SecretKey::from_bytes(&secretkey) {
            Ok(sk) => Ok(Secret(sk)),
            Err(err) => Err(anyhow::anyhow!("BLST can not parse secretkey: {:?}", err)),
        }
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
        anyhow::Ok(Signature(secret.0.sign(&buffer, &DST, &[])))
    }

    fn verify<TData: Serialize>(
        signature: &Self::Signature,
        pubkeys_occurrences: &mut dyn Iterator<Item = &(Self::PubKey, usize)>,
        data: &TData,
    ) -> anyhow::Result<bool> {
        #[cfg(feature = "timing")]
        let start = std::time::Instant::now();
        #[cfg(feature = "timing")]
        tracing::trace!("signature verification: start");
        let mut flattened_pubkeys: Vec<&gosh_blst::min_pk::PublicKey> = vec![];
        for (pubkey, occurrences) in pubkeys_occurrences {
            for _i in 0..*occurrences {
                flattened_pubkeys.push(&pubkey.0);
            }
        }
        #[cfg(feature = "timing")]
        tracing::trace!("signature verification: flatten pubkeys: {}", start.elapsed().as_millis());
        let aggregated_public_key =
            gosh_blst::min_pk::AggregatePublicKey::aggregate(&flattened_pubkeys, false).map_err(
                |e| -> anyhow::Error { anyhow::anyhow!("Pubkey aggregation failed: {:?}", e) },
            )?;
        #[cfg(feature = "timing")]
        tracing::trace!(
            "signature verification: aggregate pubkeys: {}",
            start.elapsed().as_millis()
        );
        let buffer = bincode::serialize(&data)?;
        #[cfg(feature = "timing")]
        tracing::trace!("signature verification: serialize data: {}", start.elapsed().as_millis());
        let is_valid = signature.0.verify(
            false,
            &buffer,
            &DST,
            &[],
            &aggregated_public_key.to_public_key(),
            false,
        );
        #[cfg(feature = "timing")]
        tracing::trace!("signature verification: finish: {}", start.elapsed().as_millis());
        Ok(is_valid == gosh_blst::BLST_ERROR::BLST_SUCCESS)
    }

    fn merge(one: &Self::Signature, another: &Self::Signature) -> anyhow::Result<Self::Signature> {
        let mut agg_sig = gosh_blst::min_pk::AggregateSignature::from_signature(&one.0);
        gosh_blst::min_pk::AggregateSignature::add_signature(&mut agg_sig, &another.0, false)
            .map_err(|e| -> anyhow::Error {
                anyhow::anyhow!("BLS signatures inner merge process has failed: {:?}", e)
            })?;

        Ok(Signature(agg_sig.to_signature()))
    }
}
