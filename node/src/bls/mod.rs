// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;

use serde::Deserialize;
use serde::Serialize;
pub mod envelope;
pub mod gosh_bls;

pub use gosh_bls::GoshBLS;

pub trait BLSSignatureScheme: 'static + Clone {
    type PubKey: Clone + Debug;
    type Secret: Clone;
    type Signature: Clone + Serialize + for<'a> Deserialize<'a> + Send + Sync + Default;

    fn sign<TData: Serialize>(
        secret: &Self::Secret,
        data: &TData,
    ) -> anyhow::Result<Self::Signature>;
    fn verify<TData: Serialize>(
        signature: &Self::Signature,
        pubkeys_occurrences: &mut dyn Iterator<Item = &(Self::PubKey, usize)>,
        data: &TData,
    ) -> anyhow::Result<bool>;
    fn merge(one: &Self::Signature, another: &Self::Signature) -> anyhow::Result<Self::Signature>;
}
