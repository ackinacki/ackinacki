// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::block_keeper_system::BlockKeeperSetChange;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct Directives {
    pub share_state_resource_address: Option<String>,
}

#[derive(Clone)]
pub struct CommonSection<TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo>
where
    TBLSSignatureScheme: BLSSignatureScheme,
    TBLSSignatureScheme::Signature:
        Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    pub directives: Directives,
    pub block_attestations:
        Vec<Envelope<TBLSSignatureScheme, AttestationData<TBlockIdentifier, TBlockSeqNo>>>,
    pub producer_id: NodeIdentifier,
    pub block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    pub verify_complexity: SignerIndex,
    pub acks: Vec<Envelope<TBLSSignatureScheme, AckData<TBlockIdentifier, TBlockSeqNo>>>,
    pub nacks: Vec<Envelope<TBLSSignatureScheme, NackData<TBlockIdentifier, TBlockSeqNo>>>,
    pub producer_group: Vec<NodeIdentifier>,
}

impl<TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo>
    CommonSection<TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo>
where
    TBLSSignatureScheme: BLSSignatureScheme,
    TBLSSignatureScheme::Signature:
        Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    pub fn new(
        producer_id: NodeIdentifier,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
    ) -> Self {
        CommonSection {
            block_attestations: vec![],
            directives: Directives::default(),
            producer_id,
            block_keeper_set_changes,
            verify_complexity,
            acks: vec![],
            nacks: vec![],
            producer_group: vec![],
        }
    }

    fn wrap_serialize(&self) -> WrappedCommonSection {
        let block_attestations_data = bincode::serialize(&self.block_attestations)
            .expect("Failed to serialize last finalized blocks");
        let acks_data =
            bincode::serialize(&self.acks).expect("Failed to serialize last finalized blocks");
        let nacks_data =
            bincode::serialize(&self.nacks).expect("Failed to serialize last finalized blocks");
        WrappedCommonSection {
            directives: self.directives.clone(),
            block_attestations: block_attestations_data,
            producer_id: self.producer_id,
            block_keeper_set_changes: self.block_keeper_set_changes.clone(),
            verify_complexity: self.verify_complexity,
            acks: acks_data,
            nacks: nacks_data,
            producer_group: self.producer_group.clone(),
        }
    }

    fn wrap_deserialize(data: WrappedCommonSection) -> Self {
        let block_attestations: Vec<
            Envelope<TBLSSignatureScheme, AttestationData<TBlockIdentifier, TBlockSeqNo>>,
        > = bincode::deserialize(&data.block_attestations)
            .expect("Failed to deserialize block attestations");
        let acks: Vec<Envelope<TBLSSignatureScheme, AckData<TBlockIdentifier, TBlockSeqNo>>> =
            bincode::deserialize(&data.acks).expect("Failed to deserialize acks");
        let nacks: Vec<Envelope<TBLSSignatureScheme, NackData<TBlockIdentifier, TBlockSeqNo>>> =
            bincode::deserialize(&data.nacks).expect("Failed to deserialize nacks");
        Self {
            directives: data.directives,
            block_attestations,
            producer_id: data.producer_id,
            block_keeper_set_changes: data.block_keeper_set_changes,
            verify_complexity: data.verify_complexity,
            acks,
            nacks,
            producer_group: data.producer_group,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct WrappedCommonSection {
    pub directives: Directives,
    pub block_attestations: Vec<u8>,
    pub producer_id: NodeIdentifier,
    pub block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    pub verify_complexity: SignerIndex,
    pub acks: Vec<u8>,
    pub nacks: Vec<u8>,
    pub producer_group: Vec<NodeIdentifier>,
}

impl<TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo> Serialize
    for CommonSection<TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo>
where
    TBLSSignatureScheme: BLSSignatureScheme,
    TBLSSignatureScheme::Signature:
        Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wrap_serialize().serialize(serializer)
    }
}

impl<'de, TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo> Deserialize<'de>
    for CommonSection<TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo>
where
    TBLSSignatureScheme: BLSSignatureScheme,
    TBLSSignatureScheme::Signature:
        Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wrapped_data = WrappedCommonSection::deserialize(deserializer)?;
        Ok(CommonSection::wrap_deserialize(wrapped_data))
    }
}

impl<TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo> Debug
    for CommonSection<TBLSSignatureScheme, TBlockIdentifier, TBlockSeqNo>
where
    TBLSSignatureScheme: BLSSignatureScheme,
    TBLSSignatureScheme::Signature:
        Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static + Debug,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommonSection")
            .field("directives", &self.directives)
            .field("block_attestations", &self.block_attestations)
            .field("producer_id", &self.producer_id)
            .field("block_keeper_set_changes", &self.block_keeper_set_changes)
            .field("verify_complexity", &self.verify_complexity)
            .field("acks", &self.acks)
            .field("nacks", &self.nacks)
            .field("producer_group", &self.producer_group)
            .finish()
    }
}
