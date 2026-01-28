use std::collections::HashMap;

use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::create_signed::CreateSealed;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::GoshBLS;
use crate::helper::start_shutdown;
use crate::node::associated_types::NodeCredentials;
use crate::types::RndSeed;
use crate::versioning::ProtocolVersion;

pub trait TrySeal: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static {
    fn try_seal(
        self,
        node_credentials: &NodeCredentials,
        bk_set: &BlockKeeperSet,
        bls_keys_map: &HashMap<PubKey, (Secret, RndSeed)>,
        protocol_version: &ProtocolVersion,
    ) -> anyhow::Result<Envelope<GoshBLS, Self>>;
}

impl<T> TrySeal for T
where
    T: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn try_seal(
        self,
        node_credentials: &NodeCredentials,
        bk_set: &BlockKeeperSet,
        bls_keys_map: &HashMap<PubKey, (Secret, RndSeed)>,
        protocol_version: &ProtocolVersion,
    ) -> anyhow::Result<Envelope<GoshBLS, Self>> {
        let Some(bk_data) = bk_set.get_by_node_id(node_credentials.node_id()).cloned() else {
            anyhow::bail!(
                "Node \"{}\" is not in the bk set [{}]",
                node_credentials.node_id(),
                bk_set.iter_node_ids().join(",")
            );
        };
        if !bk_data.protocol_support.is_none()
            && !node_credentials.protocol_version_support().supports_version(protocol_version)
        {
            anyhow::bail!("Failed to seal envelope: bk data version support does not match node version support")
        }
        let Some((secret, _)) = bls_keys_map.get(&bk_data.pubkey).cloned() else {
            start_shutdown();
            anyhow::bail!("Bls keymap does not have secret stored");
        };
        Envelope::sealed(self, &secret, bk_data.signer_index)
    }
}
