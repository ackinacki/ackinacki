use std::collections::HashMap;

use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use super::envelope::Envelope;
use super::GoshBLS;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::BLSSignatureScheme;
use crate::helper::start_shutdown;
use crate::node::NodeIdentifier;
use crate::types::RndSeed;

pub trait CreateSealed<T>
where
    T: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn sealed(
        node_identifier: &NodeIdentifier,
        bk_set: &BlockKeeperSet,
        bls_keys_map: &HashMap<PubKey, (Secret, RndSeed)>,
        data: T,
    ) -> anyhow::Result<Envelope<GoshBLS, T>>;
}

impl<TData> CreateSealed<TData> for Envelope<GoshBLS, TData>
where
    TData: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn sealed(
        node_identifier: &NodeIdentifier,
        bk_set: &BlockKeeperSet,
        bls_keys_map: &HashMap<PubKey, (Secret, RndSeed)>,
        data: TData,
    ) -> anyhow::Result<Self> {
        let Some(bk_data) = bk_set.get_by_node_id(node_identifier).cloned() else {
            anyhow::bail!(
                "Node \"{node_identifier}\" is not in the bk set [{}]",
                bk_set.iter_node_ids().join(",")
            );
        };
        let Some((secret, _)) = bls_keys_map.get(&bk_data.pubkey).cloned() else {
            start_shutdown();
            anyhow::bail!("Bls keymap does not have secret stored");
        };
        let signature = <GoshBLS as BLSSignatureScheme>::sign(&secret, &data)?;
        let mut signature_occurrences = HashMap::new();
        signature_occurrences.insert(bk_data.signer_index, 1);
        Ok(Envelope::create(signature, signature_occurrences, data))
    }
}
