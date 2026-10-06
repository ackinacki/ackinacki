// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use http_server::NotQueuedExtMessage;
use inbound_external_messages::InboundMessage;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use tvm_abi::Contract;
use tvm_block::GetRepresentationHash;
use tvm_block::Message;
use tvm_types::UInt256;

const MINER_ACCEPT_TAP_FUNCTION_ID: u32 = 0x7de2f437;
const MINER_CANCEL_COMMIT_DATA_FUNCTION_ID: u32 = 0x1619e4dc;
const MINER_SET_COMMIT_DATA_FUNCTION_ID: u32 = 0x50c7c49a;

#[cfg(not(test))]
const LOW_PRIORITY_EXTERNAL_MESSAGE_FUNCTION_IDS: &[u32] = &[
    MINER_ACCEPT_TAP_FUNCTION_ID,
    MINER_CANCEL_COMMIT_DATA_FUNCTION_ID,
    MINER_SET_COMMIT_DATA_FUNCTION_ID,
];
#[cfg(test)]
const LOW_PRIORITY_EXTERNAL_MESSAGE_FUNCTION_IDS: &[u32] = &[0x1122_3344];

const MINER_ABI_JSON: &[u8] =
    include_bytes!("../../../contracts/0.79.3_compiled/mvsystem/Miner.abi.json");

lazy_static::lazy_static! {
    static ref MINER_CONTRACT_ABI: Contract =
        Contract::load(MINER_ABI_JSON).expect("embedded Miner ABI must load");
}

#[derive(Clone, Debug)]
enum Status {
    Pending,  // Just queued and not processed yet external messages
    Included, // External messages already included in block in undergoing validation
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Priority {
    Normal,
    Low,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExtMessageDst {
    pub account_id: AccountIdentifier,
    pub dapp_id: Option<DAppIdentifier>,
}

impl ExtMessageDst {
    pub fn new(account_id: AccountIdentifier, dapp_id: Option<DAppIdentifier>) -> Self {
        Self { account_id, dapp_id }
    }

    pub fn from_message(
        message: &Message,
        dapp_id: Option<DAppIdentifier>,
    ) -> anyhow::Result<Self> {
        Ok(Self::new(
            message
                .int_dst_account_id()
                .map(AccountIdentifier::from)
                .ok_or_else(|| anyhow::anyhow!("Message doesn't have destination account ID"))?,
            dapp_id,
        ))
    }
}

#[derive(Clone, Debug)]
pub struct QueuedExtMessage {
    _status: Status, // Not used yet.
    priority: Priority,
    tvm_message: Message,
    hash: UInt256,
    dst: ExtMessageDst,
}

impl QueuedExtMessage {
    fn try_new(
        status: Status,
        dapp_id: Option<DAppIdentifier>,
        message: Message,
    ) -> anyhow::Result<Self> {
        let hash = message
            .hash()
            .map_err(|err| anyhow::anyhow!("Failed to calculate message hash: {err}"))?;
        let dst = ExtMessageDst::from_message(&message, dapp_id)?;
        let priority = priority_from_message(&message);
        Ok(Self { _status: status, priority, tvm_message: message, hash, dst })
    }

    pub fn try_from_incoming(ext_message: NotQueuedExtMessage) -> anyhow::Result<Self> {
        Self::try_new(Status::Pending, Some(ext_message.dapp_id()), ext_message.into_tvm_message())
    }

    pub fn try_from_block(tvm_message: Message) -> anyhow::Result<Self> {
        Self::try_new(Status::Included, None, tvm_message)
    }

    pub fn hash(&self) -> &UInt256 {
        &self.hash
    }

    pub fn tvm_message(&self) -> &Message {
        &self.tvm_message
    }

    pub fn into_tvm_message(self) -> Message {
        self.tvm_message
    }

    pub fn dst(&self) -> &ExtMessageDst {
        &self.dst
    }

    pub fn is_low_priority(&self) -> bool {
        self.priority == Priority::Low
    }

    pub fn function_id(&self) -> Option<u32> {
        message_function_id(&self.tvm_message)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(dst: ExtMessageDst) -> Self {
        Self {
            _status: Status::Pending,
            priority: Priority::Normal,
            tvm_message: Message::default(),
            hash: UInt256::default(),
            dst,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_low_priority_for_test(dst: ExtMessageDst) -> Self {
        Self { priority: Priority::Low, ..Self::new_for_test(dst) }
    }
}

fn priority_from_message(message: &Message) -> Priority {
    message_function_id(message)
        .filter(|function_id| is_low_priority_external_message_function_id(*function_id))
        .map(|_| Priority::Low)
        .unwrap_or(Priority::Normal)
}

/// Returns whether a function ID is scheduled with low priority by the external-message queue.
pub fn is_low_priority_external_message_function_id(function_id: u32) -> bool {
    LOW_PRIORITY_EXTERNAL_MESSAGE_FUNCTION_IDS.contains(&function_id)
}

fn message_function_id(message: &Message) -> Option<u32> {
    miner_message_function_id(message).or_else(|| message_raw_function_id(message))
}

/// Decodes the function ID from an incoming Miner call using its ABI.
///
/// Normalize an inline body into its own cell, as `tvm-cli` does, so ABI layout checks do not
/// count the enclosing message header as bits already consumed from the body.
pub fn miner_message_function_id(message: &Message) -> Option<u32> {
    let body = tvm_types::SliceData::load_cell(message.body()?.into_cell()).ok()?;
    // Match the CLI's external-message mode first; allow internal-layout fallback for messages
    // that were encoded with an internal ABI header despite being imported externally.
    [false, true].into_iter().find_map(|is_internal| {
        let decoded = MINER_CONTRACT_ABI.decode_input(body.clone(), is_internal, true).ok()?;
        MINER_CONTRACT_ABI
            .functions()
            .get(&decoded.function_name)
            .map(|function| function.get_input_id())
    })
}

fn message_raw_function_id(message: &Message) -> Option<u32> {
    let mut body = message.body()?;
    if body.remaining_bits() < u32::BITS as usize {
        return None;
    }
    body.get_next_u32().ok()
}

impl InboundMessage for QueuedExtMessage {
    type Account = AccountIdentifier;
    type DApp = DAppIdentifier;
    type Destination = ExtMessageDst;

    fn destination(&self) -> Self::Destination {
        *self.dst()
    }

    fn dapp_id(destination: &Self::Destination) -> Self::DApp {
        destination.dapp_id.unwrap_or_else(|| destination.account_id.redirect_dapp_id())
    }

    fn account_id(destination: &Self::Destination) -> Self::Account {
        destination.account_id
    }

    fn is_low_priority(&self) -> bool {
        QueuedExtMessage::is_low_priority(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::hash::Hasher;
    use std::str::FromStr;

    use node_types::AccountIdentifier;
    use node_types::DAppIdentifier;
    use tvm_abi::param::Param;
    use tvm_abi::param_type::ParamType;
    use tvm_block::ExternalInboundMessageHeader;
    use tvm_block::MsgAddressExt;
    use tvm_block::MsgAddressInt;
    use tvm_types::base64_decode;
    use tvm_types::read_single_root_boc;
    use tvm_types::AccountId;
    use tvm_types::SliceData;

    use super::message_function_id;
    use super::miner_message_function_id;
    use super::ExtMessageDst;
    use super::QueuedExtMessage;
    use super::MINER_ACCEPT_TAP_FUNCTION_ID;
    use super::MINER_CANCEL_COMMIT_DATA_FUNCTION_ID;
    use super::MINER_CONTRACT_ABI;
    use super::MINER_SET_COMMIT_DATA_FUNCTION_ID;

    fn hash_of(dst: &ExtMessageDst) -> u64 {
        let mut hasher = DefaultHasher::new();
        dst.hash(&mut hasher);
        hasher.finish()
    }

    fn ext_in_message_with_body(body: SliceData) -> tvm_block::Message {
        let account_id = AccountIdentifier::from_str(&"ab".repeat(32)).unwrap();
        let dst = MsgAddressInt::with_standart(
            None,
            0,
            AccountId::from_raw(account_id.as_slice().to_vec(), 256),
        )
        .unwrap();
        let header = ExternalInboundMessageHeader::new(MsgAddressExt::AddrNone, dst);
        let mut message = tvm_block::Message::with_ext_in_header(header);
        message.set_body(body);
        message
    }

    #[test]
    fn message_function_id_reads_first_body_word() {
        let message =
            ext_in_message_with_body(SliceData::from_raw(vec![0x11, 0x22, 0x33, 0x44, 0xaa], 40));

        assert_eq!(message_function_id(&message), Some(0x1122_3344));
    }

    #[test]
    fn miner_function_id_matches_tvm_cli_set_commit_data_decode() {
        // Body emitted by `tvm-cli decode msg --abi Miner.abi.json` for a
        // `setCommitData` external call. It includes the ABI 2.4 header.
        let body_boc = base64_decode(
            "te6ccgEBAwEAqQABIQAAAGhC59BDmrDfY5Qx8SagAQFw//////////////////////////////////////////4AAAAAAAABSgAAAAAAAABGOe3PSKrm6AACALAgAAAAJjtWMa8uiuBClqTJG/FG1uFToRebA1+DER1mwG3PumdKAQAAAAAAACAAAABC9OspFujRt8d5kxM7bA3k0xu4Js87jRimOHUYEr+NSUYAAAAAAAAA",
        )
        .unwrap();
        let body = SliceData::load_cell(read_single_root_boc(&body_boc).unwrap()).unwrap();
        let message = ext_in_message_with_body(body);

        assert_eq!(miner_message_function_id(&message), Some(MINER_SET_COMMIT_DATA_FUNCTION_ID));
    }

    #[test]
    fn embedded_miner_abi_matches_expected_header() {
        let abi = &*MINER_CONTRACT_ABI;
        assert_eq!(abi.version().to_string(), "2.4");
        assert_eq!(
            abi.header(),
            &vec![
                Param::new("pubkey", ParamType::PublicKey),
                Param::new("time", ParamType::Time),
                Param::new("expire", ParamType::Expire),
            ]
        );
    }

    #[test]
    fn low_priority_function_id_matches_miner_abi() {
        let abi = &*MINER_CONTRACT_ABI;
        let mut matching_functions: Vec<_> = abi
            .functions()
            .iter()
            .filter_map(|(name, function)| {
                [
                    MINER_ACCEPT_TAP_FUNCTION_ID,
                    MINER_CANCEL_COMMIT_DATA_FUNCTION_ID,
                    MINER_SET_COMMIT_DATA_FUNCTION_ID,
                ]
                .contains(&function.get_input_id())
                .then_some(name.as_str())
            })
            .collect();
        matching_functions.sort_unstable();

        assert_eq!(matching_functions, vec!["acceptTap", "cancelCommitData", "setCommitData"]);
    }

    #[test]
    fn incoming_message_is_low_priority_for_configured_function_id() {
        let message =
            ext_in_message_with_body(SliceData::from_raw(vec![0x11, 0x22, 0x33, 0x44], 32));
        let queued = QueuedExtMessage::try_new(super::Status::Pending, None, message).unwrap();

        assert!(queued.is_low_priority());
    }

    #[test]
    fn eq_and_hash_include_dapp_id() {
        let account = AccountIdentifier::from_str(&"ab".repeat(32)).unwrap();
        let dapp1 = DAppIdentifier::from_str(&"11".repeat(32)).unwrap();
        let dapp2 = DAppIdentifier::from_str(&"22".repeat(32)).unwrap();

        let none = ExtMessageDst::new(account, None);
        let some1 = ExtMessageDst::new(account, Some(dapp1));
        let some2 = ExtMessageDst::new(account, Some(dapp2));

        assert_ne!(none, some1);
        assert_ne!(some1, some2);

        assert_ne!(hash_of(&none), hash_of(&some1));
        assert_ne!(hash_of(&some1), hash_of(&some2));
    }

    #[test]
    fn hashmap_keeps_same_account_in_different_dapps_separate() {
        let account = AccountIdentifier::from_str(&"cd".repeat(32)).unwrap();
        let dapp = DAppIdentifier::from_str(&"33".repeat(32)).unwrap();

        let mut map: HashMap<ExtMessageDst, u32> = HashMap::new();
        *map.entry(ExtMessageDst::new(account, Some(dapp))).or_default() += 1;
        *map.entry(ExtMessageDst::new(account, None)).or_default() += 1;

        assert_eq!(map.len(), 2);
        assert_eq!(map.values().sum::<u32>(), 2);
    }
}
