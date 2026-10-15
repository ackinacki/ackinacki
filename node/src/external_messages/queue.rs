// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use http_server::NotQueuedExtMessage;
use inbound_external_messages::InboundMessage;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use tvm_abi::Contract;
use tvm_abi::TokenValue;
use tvm_block::GetRepresentationHash;
use tvm_block::Message;
use tvm_types::UInt256;

const MINER_ACCEPT_TAP_FUNCTION_ID: u32 = 0x7de2f437;
const MINER_CANCEL_COMMIT_DATA_FUNCTION_ID: u32 = 0x1619e4dc;
const MINER_SET_COMMIT_DATA_FUNCTION_ID: u32 = 0x50c7c49a;
const MINER_GET_REWARD_FUNCTION_ID: u32 = 0x3072364c;

#[cfg(not(test))]
const LOW_PRIORITY_EXTERNAL_MESSAGE_FUNCTION_IDS: &[u32] = &[
    MINER_ACCEPT_TAP_FUNCTION_ID,
    MINER_CANCEL_COMMIT_DATA_FUNCTION_ID,
    MINER_SET_COMMIT_DATA_FUNCTION_ID,
    MINER_GET_REWARD_FUNCTION_ID,
];
#[cfg(test)]
const LOW_PRIORITY_EXTERNAL_MESSAGE_FUNCTION_IDS: &[u32] = &[0x1122_3344];

const MINER_ABI_JSON: &[u8] =
    include_bytes!("../../../contracts/0.79.3_compiled/mvsystem/Miner.abi.json");
const WALLET_ABI_JSONS: &[&[u8]] =
    &[include_bytes!("../../../contracts/0.79.3_compiled/mvsystem/Mvmultifactor.abi.json")];
const ROOT_PN_ACCOUNT: [u8; 32] =
    hex_literal::hex!("1010101010101010101010101010101010101010101010101010101010101010");
const WALLET_DAPP_ID: DAppIdentifier = DAppIdentifier::new(hex_literal::hex!(
    "0000000000000000000000000000000000000000000000000000000000000001"
));

lazy_static::lazy_static! {
    static ref MINER_CONTRACT_ABI: Contract =
        Contract::load(MINER_ABI_JSON).expect("embedded Miner ABI must load");
    static ref WALLET_CONTRACT_ABIS: Vec<Contract> = WALLET_ABI_JSONS
        .iter()
        .map(|json| Contract::load(*json).expect("embedded wallet ABI must load"))
        .collect();
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
        let priority = priority_from_message(&message, dapp_id);
        if priority == Priority::Low {
            trace_low_priority(&status, &hash, &dst, &message);
        }
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

fn priority_from_message(message: &Message, dapp_id: Option<DAppIdentifier>) -> Priority {
    if is_low_priority_external_message(message, dapp_id) {
        Priority::Low
    } else {
        Priority::Normal
    }
}

/// Applies the external-message queue's low-priority rules to the full message.
/// Wallet decoding is skipped for known destination dApps other than 1.
/// When the dApp is unavailable (for example, in a block BOC), decode as before.
pub fn is_low_priority_external_message(
    message: &Message,
    dapp_id: Option<DAppIdentifier>,
) -> bool {
    is_low_priority_wallet_transfer(message, dapp_id)
        || message_function_id(message).is_some_and(is_low_priority_external_message_function_id)
}

fn trace_low_priority(status: &Status, hash: &UInt256, dst: &ExtMessageDst, message: &Message) {
    if !tracing::enabled!(
        target: crate::helper::EXT_MESSAGES_QUEUE_DETAILED_TARGET,
        tracing::Level::TRACE
    ) {
        return;
    }
    let reason = match low_priority_wallet_transfer(message, dst.dapp_id) {
        Some((function_name, transfer_dest)) => {
            format!("wallet {function_name} to {transfer_dest}")
        }
        None => format!("function_id {:#010x}", message_function_id(message).unwrap_or_default()),
    };
    tracing::trace!(
        target: crate::helper::EXT_MESSAGES_QUEUE_DETAILED_TARGET,
        "ext_message_low_priority: status={:?}, hash={}, account={}, reason={}",
        status,
        hash.to_hex_string(),
        dst.account_id.to_hex_string(),
        reason
    );
}

fn is_low_priority_wallet_transfer(message: &Message, dapp_id: Option<DAppIdentifier>) -> bool {
    low_priority_wallet_transfer(message, dapp_id).is_some()
}

fn low_priority_wallet_transfer(
    message: &Message,
    dapp_id: Option<DAppIdentifier>,
) -> Option<(String, tvm_block::MsgAddress)> {
    if dapp_id.is_some_and(|id| id != WALLET_DAPP_ID) || !message.is_inbound_external() {
        return None;
    }
    let body =
        message.body().and_then(|body| tvm_types::SliceData::load_cell(body.into_cell()).ok())?;
    WALLET_CONTRACT_ABIS.iter().find_map(|abi| {
        let decoded = abi.decode_input(body.clone(), false, false).ok()?;
        if !matches!(decoded.function_name.as_str(), "sendTransaction" | "submitTransaction") {
            return None;
        }
        let has_payload = decoded.tokens.iter().any(|token| {
            token.name == "payload"
                && matches!(&token.value, TokenValue::Cell(cell)
                    if cell.bit_length() != 0 || cell.references_count() != 0)
        });
        if !has_payload {
            return None;
        }
        decoded.tokens.iter().find_map(|token| match &token.value {
            TokenValue::Address(address)
                if token.name == "dest" && !is_root_pn_or_mirror(address) =>
            {
                Some((decoded.function_name.clone(), address.clone()))
            }
            _ => None,
        })
    })
}

fn is_root_pn_or_mirror(address: &tvm_block::MsgAddress) -> bool {
    let (workchain, anycast, account) = match address {
        tvm_block::MsgAddress::AddrStd(address) => {
            (address.workchain_id as i32, &address.anycast, &address.address)
        }
        tvm_block::MsgAddress::AddrVar(address) => {
            (address.workchain_id, &address.anycast, &address.address)
        }
        _ => return false,
    };
    if workchain != 0 || anycast.is_some() {
        return false;
    }
    if account.remaining_bits() != 256 {
        return false;
    }
    let bytes = account.get_bytestring(0);
    bytes == ROOT_PN_ACCOUNT
        // Mirrors use the 244-bit prefix 0x2000…000; the final 12 bits hold the index.
        || (bytes[0] == 0x20 && bytes[1..30].iter().all(|byte| *byte == 0) && bytes[30] & 0xf0 == 0)
}

/// Checks the low-priority function IDs; use `is_low_priority_external_message` for all queue rules.
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
    use tvm_block::Deserializable;
    use tvm_block::ExternalInboundMessageHeader;
    use tvm_block::GetRepresentationHash;
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
    use super::MINER_GET_REWARD_FUNCTION_ID;
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

    fn wallet_transfer_message(
        abi: &tvm_abi::Contract,
        method: &str,
        destination: &str,
        with_payload: bool,
        signed: bool,
    ) -> tvm_block::Message {
        let function = abi.function(method).unwrap();
        let mut tokens = tvm_abi::token::Tokenizer::tokenize_all_params(
            function.input_params(),
            &serde_json::json!({
                "epk_expire_at": "1805130122", "dest": destination, "value": "100000000",
                "cc": {}, "bounce": true, "flags": 1, "allBalance": false,
                "payload": "te6ccgEBAQEAAgAAAA==",
            }),
        )
        .unwrap();
        if with_payload {
            // Priority depends on the destination, not on the payload's function ID.
            tokens.iter_mut().find(|token| token.name == "payload").unwrap().value =
                tvm_abi::TokenValue::Cell(SliceData::from_raw(vec![0xaa], 8).into_cell());
        }
        let header = HashMap::from([
            ("pubkey".to_owned(), tvm_abi::TokenValue::PublicKey(Some([0x11; 32]))),
            ("time".to_owned(), tvm_abi::TokenValue::Time(1791835688810)),
            ("expire".to_owned(), tvm_abi::TokenValue::Expire(1791835748)),
        ]);
        let body = if signed {
            let address = MsgAddressInt::from_str(&format!("0:{}", "ab".repeat(32))).unwrap();
            let (body, _) = function
                .create_unsigned_call(&header, &tokens, false, true, Some(address))
                .unwrap();
            // Only the wire layout is relevant here; the queue does not verify signatures.
            tvm_abi::Function::fill_sign(abi.version(), Some(&[0x55; 64]), None, body).unwrap()
        } else {
            function.encode_input(&header, &tokens, false, None, None).unwrap()
        };
        ext_in_message_with_body(SliceData::load_builder(body).unwrap())
    }

    #[test]
    fn wallet_transfers_with_payload_outside_root_pn_and_mirrors_are_low_priority() {
        for abi in super::WALLET_CONTRACT_ABIS.iter() {
            for method in ["sendTransaction", "submitTransaction"] {
                for destination in [
                    format!("0:{}", "ab".repeat(32)),
                    format!("-1:{}", "10".repeat(32)),
                    format!("-1:2{:063x}", 1),
                    format!("0:2{:063x}", 4096),
                ] {
                    for signed in [false, true] {
                        let message =
                            wallet_transfer_message(abi, method, &destination, true, signed);
                        let (function_name, transfer_dest) =
                            super::low_priority_wallet_transfer(&message, None).unwrap();
                        assert_eq!(function_name, method);
                        assert_eq!(transfer_dest.to_string(), destination);
                        let queued = QueuedExtMessage::try_from_block(message).unwrap();
                        assert!(
                            queued.is_low_priority(),
                            "{method} to {destination}, signed={signed}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn wallet_transfers_without_payload_or_to_root_pn_and_mirrors_are_normal_priority() {
        let root = format!("0:{}", "10".repeat(32));
        for abi in super::WALLET_CONTRACT_ABIS.iter() {
            for method in ["sendTransaction", "submitTransaction"] {
                for destination in
                    [root.clone(), format!("0:2{:063x}", 1), format!("0:{}", "ab".repeat(32))]
                {
                    let message = wallet_transfer_message(abi, method, &destination, false, true);
                    assert!(!QueuedExtMessage::try_from_block(message).unwrap().is_low_priority());
                }
                for destination in [
                    root.clone(),
                    format!("0:2{:063x}", 0),
                    format!("0:2{:063x}", 1),
                    format!("0:2{:063x}", 1000),
                    format!("0:2{:063x}", 4095),
                ] {
                    for signed in [false, true] {
                        let message =
                            wallet_transfer_message(abi, method, &destination, true, signed);
                        assert!(!QueuedExtMessage::try_from_block(message)
                            .unwrap()
                            .is_low_priority());
                    }
                }
                let mut truncated = wallet_transfer_message(abi, method, &root, true, true);
                truncated.set_body(SliceData::from_raw(vec![0xff], 8));
                assert!(!super::is_low_priority_wallet_transfer(&truncated, None));
            }
        }
    }

    #[test]
    fn wallet_priority_depends_on_known_destination_dapp() {
        for abi in super::WALLET_CONTRACT_ABIS.iter() {
            for method in ["sendTransaction", "submitTransaction"] {
                let destination = format!("0:{}", "ab".repeat(32));
                let message = wallet_transfer_message(abi, method, &destination, true, true);
                for (dapp_id, expected_low_priority) in [
                    (Some(super::WALLET_DAPP_ID), true),
                    (Some(DAppIdentifier::ZERO), false),
                    (Some(DAppIdentifier::from_str(&format!("{:064x}", 2)).unwrap()), false),
                    (None, true),
                ] {
                    assert_eq!(
                        super::is_low_priority_external_message(&message, dapp_id),
                        expected_low_priority
                    );
                    let queued =
                        QueuedExtMessage::try_new(super::Status::Pending, dapp_id, message.clone())
                            .unwrap();
                    assert_eq!(queued.is_low_priority(), expected_low_priority);
                }
            }
        }
    }

    #[test]
    fn mainnet_submit_transaction_with_payload_is_low_priority() {
        // Mainnet message d6ddbc7700ad63113efd4374d435877aec71c3a68e59d87cd0820ada507e7432.
        let boc = base64_decode(
            "te6ccgECBAEAAR0AAUWIAF0UGjF17wE33kR1m5Sb+nHVkM9NlxgSu23aHmxYtf68DAEB4Zk4gpWpmNtGee2khwoGY9N21cPGA76BSuGcQifdAM1H7BxMpqqpohLDDCuztCyGVUrPz8QqnB5TdeQuvTC8loTd3lWIJ/fPUWUgkgRTRuaXAIDMOP1HXdIC2KvA110tCUAAAGhNiuKbWrOYWVZfIn6gAgFzAAAAAGuXqQqAG2KxlBYoVnUcc/PsBAO0bu+IvDPEpbdSP7VhFBmF2TsgAAAAAAAAAAAAAAAAvrwgCgMAiDRWo8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAUHK+MCS+G4ygMLdn3yaYSIqXzAPtzCx7M/bfGGT3duN",
        )
        .unwrap();
        let message =
            tvm_block::Message::construct_from_cell(read_single_root_boc(&boc).unwrap()).unwrap();
        assert_eq!(
            message.hash().unwrap().to_hex_string(),
            "d6ddbc7700ad63113efd4374d435877aec71c3a68e59d87cd0820ada507e7432"
        );
        let (method, destination) = super::low_priority_wallet_transfer(&message, None).unwrap();
        assert_eq!(method, "submitTransaction");
        assert_eq!(
            destination.to_string(),
            "0:db158ca0b142b3a8e39f9f60201da3777c45e19e252dba91fdab08a0cc2ec9d9"
        );
        assert!(super::is_low_priority_external_message(&message, None));
        assert!(QueuedExtMessage::try_from_block(message).unwrap().is_low_priority());
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
                    MINER_GET_REWARD_FUNCTION_ID,
                ]
                .contains(&function.get_input_id())
                .then_some(name.as_str())
            })
            .collect();
        matching_functions.sort_unstable();

        assert_eq!(
            matching_functions,
            vec!["acceptTap", "cancelCommitData", "getReward", "setCommitData"]
        );
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
