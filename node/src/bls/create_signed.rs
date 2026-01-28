use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use super::envelope::Envelope;
use super::GoshBLS;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::gosh_bls::Secret;
use crate::bls::BLSSignatureScheme;
use crate::node::SignerIndex;

pub trait CreateSealed<T>
where
    T: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn sealed(
        data: T,
        secret: &Secret,
        signer_index: SignerIndex,
    ) -> anyhow::Result<Envelope<GoshBLS, T>>;
}

impl<TData> CreateSealed<TData> for Envelope<GoshBLS, TData>
where
    TData: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn sealed(data: TData, secret: &Secret, signer_index: SignerIndex) -> anyhow::Result<Self> {
        let signature = <GoshBLS as BLSSignatureScheme>::sign(secret, &data)?;
        let mut signature_occurrences = HashMap::new();
        signature_occurrences.insert(signer_index, 1);
        Ok(Envelope::create(signature, signature_occurrences, data))
    }
}
