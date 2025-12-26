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
use crate::node::associated_types::NodeCredentials;
use crate::types::RndSeed;

pub trait CreateSealed<T>
where
    T: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn sealed(
        node_credentials: &NodeCredentials,
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
        node_credentials: &NodeCredentials,
        bk_set: &BlockKeeperSet,
        bls_keys_map: &HashMap<PubKey, (Secret, RndSeed)>,
        data: TData,
    ) -> anyhow::Result<Self> {
        let Some(bk_data) = bk_set.get_by_node_id(node_credentials.node_id()).cloned() else {
            anyhow::bail!(
                "Node \"{}\" is not in the bk set [{}]",
                node_credentials.node_id(),
                bk_set.iter_node_ids().join(",")
            );
        };
        if !bk_data.protocol_support.is_none()
            && &bk_data.protocol_support != node_credentials.protocol_version_support()
        {
            anyhow::bail!("Failed to seal envelope: bk data version support does not match node version support")
        }
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
