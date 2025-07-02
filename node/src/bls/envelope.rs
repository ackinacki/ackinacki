// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::clone::Clone;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::bls::BLSSignatureScheme;
use crate::node::SignerIndex;

pub trait BLSSignedEnvelope: Send + Sync + 'static {
    type BLS: BLSSignatureScheme;
    type Data: Send + Sync + 'static;
    type SignerIndex: Send + Sync + 'static;

    fn create<T: Into<Self::Data>>(
        aggregated_signature: <Self::BLS as BLSSignatureScheme>::Signature,
        signature_occurrences: HashMap<Self::SignerIndex, u16>,
        data: T,
    ) -> Self;

    fn add_signature(
        &mut self,
        signer_index: &Self::SignerIndex,
        secret: &<Self::BLS as BLSSignatureScheme>::Secret,
    ) -> anyhow::Result<()>;
    fn clone_signature_occurrences(&self) -> HashMap<Self::SignerIndex, u16>;
    fn verify_signatures(
        &self,
        signers: &HashMap<Self::SignerIndex, <Self::BLS as BLSSignatureScheme>::PubKey>,
    ) -> anyhow::Result<bool>;
    fn aggregated_signature(&self) -> &<Self::BLS as BLSSignatureScheme>::Signature;
    fn data(&self) -> &Self::Data;
    fn has_signer_index(&self, index: Self::SignerIndex) -> bool;
}

#[derive(Clone, PartialEq, Eq)]
pub struct Envelope<BLS, TData>
where
    BLS: BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TData: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    aggregated_signature: BLS::Signature,
    signature_occurrences: HashMap<SignerIndex, u16>,
    data: TData,
}

impl<BLS, TData> BLSSignedEnvelope for Envelope<BLS, TData>
where
    BLS: BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TData: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    type BLS = BLS;
    type Data = TData;
    type SignerIndex = SignerIndex;

    fn create<T: Into<Self::Data>>(
        aggregated_signature: BLS::Signature,
        signature_occurrences: HashMap<Self::SignerIndex, u16>,
        data: T,
    ) -> Self {
        Self { data: data.into(), aggregated_signature, signature_occurrences }
    }

    fn add_signature(
        &mut self,
        signer_index: &Self::SignerIndex,
        secret: &BLS::Secret,
    ) -> anyhow::Result<()> {
        let signature = BLS::sign(secret, &self.data)?;
        self.aggregated_signature = BLS::merge(&self.aggregated_signature, &signature)?;
        match self.signature_occurrences.get(signer_index) {
            Some(count) => {
                self.signature_occurrences.insert(*signer_index, count + 1);
            }
            None => {
                self.signature_occurrences.insert(*signer_index, 1);
            }
        }
        Ok(())
    }

    fn clone_signature_occurrences(&self) -> HashMap<Self::SignerIndex, u16> {
        self.signature_occurrences.clone()
    }

    fn has_signer_index(&self, index: Self::SignerIndex) -> bool {
        let Some(count) = self.signature_occurrences.get(&index) else {
            return false;
        };
        *count > 0
    }

    fn verify_signatures(
        &self,
        signers: &HashMap<Self::SignerIndex, BLS::PubKey>,
    ) -> anyhow::Result<bool> {
        let mut pubkeys_occurrences = Vec::<(BLS::PubKey, usize)>::new();
        let mut is_any_signature_exists = false;
        for signer_index in self.signature_occurrences.keys() {
            let count = *self.signature_occurrences.get(signer_index).unwrap() as usize;
            if count == 0 {
                continue;
            }
            match signers.get(signer_index) {
                None => {
                    // tracing::unknown_signer_index
                    tracing::trace!("Wrong signer index: {:?}", signer_index);
                    tracing::trace!(
                        "Verify signature: signers:{:?}, envelope_keys:{:?}",
                        signers.keys(),
                        self.signature_occurrences.keys(),
                    );
                    return Ok(false);
                }
                Some(pubkey) => {
                    pubkeys_occurrences.push((pubkey.clone(), count));
                }
            }
            is_any_signature_exists = true;
        }

        if !is_any_signature_exists {
            return Ok(false);
        }

        let is_valid =
            BLS::verify(&self.aggregated_signature, &mut pubkeys_occurrences.iter(), &self.data)?;
        if !is_valid {
            tracing::trace!("Signature verification failed");
            tracing::trace!(
                "Verify signature: signers:{:?}, envelope_keys:{:?}",
                signers.keys(),
                self.signature_occurrences.keys(),
            );
            tracing::trace!("Verify signature: {:?}", pubkeys_occurrences);
        }
        Ok(is_valid)
    }

    fn aggregated_signature(&self) -> &BLS::Signature {
        &self.aggregated_signature
    }

    fn data(&self) -> &TData {
        &self.data
    }
}

#[derive(Serialize, Deserialize)]
struct EnvelopeSerDe<TSignature, TData> {
    pub aggregated_signature: TSignature,
    pub signature_occurrences: Vec<(SignerIndex, u16)>,
    pub data: TData,
}

impl<BLS, TData> Display for Envelope<BLS, TData>
where
    BLS: BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TData: Serialize + for<'b> Deserialize<'b> + Display + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Envelope: data:{}, sigs:{:?}", self.data(), self.clone_signature_occurrences())
    }
}

impl<BLS, TData> Debug for Envelope<BLS, TData>
where
    BLS: BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TData: Serialize + for<'b> Deserialize<'b> + Debug + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("")
            .field("data", &self.data)
            .field("signature_occurrences", &self.signature_occurrences)
            .finish()
    }
}

impl<BLS, TData> Serialize for Envelope<BLS, TData>
where
    BLS: BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TData: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Sort signature occurrences to make serialization the same each time
        let mut signature_occurrences: Vec<(SignerIndex, u16)> =
            self.signature_occurrences.clone().into_iter().collect();
        signature_occurrences.sort_by(|(a, _), (b, _)| a.cmp(b));
        let envelope_serde = EnvelopeSerDe::<BLS::Signature, TData> {
            aggregated_signature: self.aggregated_signature.clone(),
            signature_occurrences,
            data: self.data.clone(),
        };
        envelope_serde.serialize(serializer)
    }
}

impl<'de, BLS, TData> Deserialize<'de> for Envelope<BLS, TData>
where
    BLS: BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TData: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let EnvelopeSerDe::<BLS::Signature, TData> {
            aggregated_signature,
            signature_occurrences,
            data,
        } = EnvelopeSerDe::<BLS::Signature, TData>::deserialize(deserializer)?;
        let signature_occurrences = HashMap::from_iter(signature_occurrences);
        Ok(Self { aggregated_signature, signature_occurrences, data })
    }
}
