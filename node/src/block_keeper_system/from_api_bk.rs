// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::str::FromStr;

use http_server::ApiBk;
use http_server::ApiBkStatus;
use http_server::ApiPubKey;
use http_server::ApiUInt256;
use num_bigint::BigUint;

use super::BlockKeeperData;
use super::BlockKeeperSet;
use super::BlockKeeperStatus;
use crate::bls::gosh_bls::PubKey;
use crate::node::SignerIndex;
use crate::types::AccountAddress;
use crate::versioning::ProtocolVersionSupport;

impl From<ApiBk> for BlockKeeperData {
    fn from(value: ApiBk) -> Self {
        Self {
            pubkey: PubKey::from(value.pubkey.0),
            epoch_finish_seq_no: value.epoch_finish_seq_no,
            wait_step: value.wait_step,
            status: value.status.into(),
            address: value.address,
            stake: BigUint::from_str(&value.stake).unwrap_or_default(),
            owner_address: AccountAddress(value.owner_address.0.into()),
            signer_index: value.signer_index as SignerIndex,
            owner_pubkey: value.owner_pubkey.0,
            protocol_support: ProtocolVersionSupport::from_str(&value.protocol_version_support)
                .unwrap(),
        }
    }
}

impl From<BlockKeeperStatus> for ApiBkStatus {
    fn from(value: BlockKeeperStatus) -> Self {
        match value {
            BlockKeeperStatus::Active => Self::Active,
            BlockKeeperStatus::CalledToFinish => Self::CalledToFinish,
            BlockKeeperStatus::Expired => Self::Expired,
            BlockKeeperStatus::PreEpoch => Self::PreEpoch,
        }
    }
}

impl From<ApiBkStatus> for BlockKeeperStatus {
    fn from(value: ApiBkStatus) -> Self {
        match value {
            ApiBkStatus::Active => Self::Active,
            ApiBkStatus::CalledToFinish => Self::CalledToFinish,
            ApiBkStatus::Expired => Self::Expired,
            ApiBkStatus::PreEpoch => Self::PreEpoch,
        }
    }
}

impl From<&BlockKeeperSet> for Vec<ApiBk> {
    fn from(value: &BlockKeeperSet) -> Self {
        value.by_signer.values().map(|x| x.clone().into()).collect()
    }
}

impl From<Vec<ApiBk>> for BlockKeeperSet {
    fn from(value: Vec<ApiBk>) -> Self {
        let mut set = BlockKeeperSet::new();
        for bk in value {
            set.insert(bk.signer_index as SignerIndex, bk.into());
        }
        set
    }
}

impl From<BlockKeeperData> for ApiBk {
    fn from(value: BlockKeeperData) -> Self {
        Self {
            pubkey: ApiPubKey(value.pubkey.as_ref().to_bytes()),
            epoch_finish_seq_no: value.epoch_finish_seq_no,
            wait_step: value.wait_step,
            status: value.status.into(),
            address: value.address,
            stake: value.stake.to_string(),
            owner_address: ApiUInt256(*value.owner_address.0.as_array()),
            signer_index: value.signer_index as usize,
            owner_pubkey: ApiUInt256(value.owner_pubkey),
            ttl_seq_no: None,
            protocol_version_support: value.protocol_support.to_string(),
        }
    }
}
