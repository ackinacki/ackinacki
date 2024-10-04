// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::RandomState;
use std::sync::Arc;

use tvm_block::Account;
use tvm_block::AccountBlock;
use tvm_block::AccountStatus;
use tvm_block::Block;
use tvm_block::BlockProcessingStatus;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_block::Message;
use tvm_block::MessageId;
use tvm_block::MessageProcessingStatus;
use tvm_block::MsgAddrStd;
use tvm_block::MsgAddressInt;
use tvm_block::Serializable;
use tvm_block::ShardStateUnsplit;
use tvm_block::Transaction;
use tvm_block::TransactionProcessingStatus;
use tvm_block_json::u64_to_string;
use tvm_types::write_boc;
use tvm_types::AccountId;
use tvm_types::BuilderData;
use tvm_types::Cell;
use tvm_types::HashmapType;
use tvm_types::SliceData;
use tvm_types::UInt256;

use super::archive::ArchMessage;
use crate::block::producer::builder::EngineTraceInfoData;
use crate::block::Block as ANBlock;
use crate::block::WrappedBlock;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::database::documents_db::DocumentsDb;
use crate::database::documents_db::SerializedItem;

lazy_static::lazy_static!(
    static ref ACCOUNT_NONE_HASH: UInt256 = Account::default().serialize().unwrap().repr_hash();
    pub static ref MINTER_ADDRESS: MsgAddressInt =
        MsgAddressInt::AddrStd(MsgAddrStd::with_address(None, 0, [0; 32].into()));
);

#[derive(Default, Debug)]
pub struct MessageSerializationSet {
    pub message: Message,
    pub id: MessageId,
    pub block_id: Option<UInt256>,
    pub transaction_id: Option<UInt256>,
    pub transaction_now: Option<u32>,
    pub status: MessageProcessingStatus,
    pub boc: Vec<u8>,
    pub proof: Option<Vec<u8>>,
}

pub fn reflect_block_in_db(
    archive: Arc<dyn DocumentsDb>,
    envelope: Envelope<GoshBLS, WrappedBlock>,
    shard_state: Arc<ShardStateUnsplit>,
    transaction_traces: &mut HashMap<UInt256, Vec<EngineTraceInfoData>, RandomState>,
) -> anyhow::Result<HashMap<String, SerializedItem>> {
    let now_all = std::time::Instant::now();

    let block_root = envelope.data().block.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
    let serialized_block = tvm_types::write_boc(&block_root).unwrap();

    let add_proof = false;
    let block_id = &block_root.repr_hash();
    let block_id_hex = block_id.to_hex_string();
    log::info!(target: "database", "reflect_block_in_db block_id: {}", block_id_hex);
    let file_hash = UInt256::calc_file_hash(&serialized_block);
    let block = &envelope.data().block;
    let block_extra = block.read_extra().map_err(|e| anyhow::format_err!("{e}"))?;
    let block_boc = &serialized_block;
    let info = block.read_info().map_err(|e| anyhow::format_err!("{e}"))?;
    let block_index = block_index(block)?;
    let workchain_id = info.shard().workchain_id();
    let shard_accounts = shard_state.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;

    // Prepare sorted tvm_block transactions and addresses of changed accounts
    let mut changed_acc = HashSet::new();
    let mut deleted_acc = HashSet::new();
    let mut acc_last_trans_chain_order = HashMap::new();
    let now = std::time::Instant::now();
    let mut tr_count = 0;
    let mut transactions = BTreeMap::new();
    block_extra
        .read_account_blocks()
        .map_err(|e| anyhow::format_err!("{e}"))?
        .iterate_objects(|account_block: AccountBlock| {
            // extract ids of changed accounts
            let state_upd = account_block.read_state_update()?;
            let mut check_account_existed = false;
            if state_upd.new_hash == *ACCOUNT_NONE_HASH {
                log::trace!(target: "database",
                    "reflect_block_in_db: deleted acc: {}",
                    account_block.account_id().to_hex_string()
                );
                deleted_acc.insert(account_block.account_id().clone());
                if state_upd.old_hash == *ACCOUNT_NONE_HASH {
                    check_account_existed = true;
                }
            } else {
                log::trace!(target: "database",
                    "reflect_block_in_db: changed acc: {}",
                    account_block.account_id().to_hex_string()
                );
                changed_acc.insert(account_block.account_id().clone());
            }

            let mut account_existed = false;
            account_block.transactions().iterate_slices(|_, transaction_slice| {
                // extract transactions
                let cell = transaction_slice.reference(0)?;
                let transaction =
                    Transaction::construct_from(&mut SliceData::load_cell(cell.clone())?)?;
                log::trace!(target: "database",
                    "reflect_block_in_db: prepare transaction: {}",
                    transaction.hash().unwrap().to_hex_string()
                );
                let ordering_key = (transaction.logical_time(), transaction.account_id().clone());
                if transaction.orig_status != AccountStatus::AccStateNonexist
                    || transaction.end_status != AccountStatus::AccStateNonexist
                {
                    account_existed = true;
                }
                transactions.insert(ordering_key, (cell, transaction));
                tr_count += 1;

                Ok(true)
            })?;

            if check_account_existed && !account_existed {
                deleted_acc.remove(account_block.account_id());
            }

            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("{e}"))?;
    log::info!(target: "database",
        "TIME: preliminary prepare {} transactions {}ms;",
        tr_count,
        now.elapsed().as_millis(),
    );

    // Iterate tvm_block transactions to:
    // - prepare messages and transactions for external db
    // - prepare last_trans_chain_order for accounts
    let now = std::time::Instant::now();
    let mut messages = Default::default();
    let mut transaction_docs = vec![];
    for (index, (_, (cell, transaction))) in transactions.into_iter().enumerate() {
        let transaction_index =
            format!("{}{}", block_index, tvm_block_json::u64_to_string(index as u64));

        prepare_messages_from_transaction(
            &transaction,
            block_id.clone(),
            cell.repr_hash(),
            &transaction_index,
            add_proof.then_some(&block_root),
            &mut messages,
        )?;

        let account_id = transaction.account_id().clone();
        acc_last_trans_chain_order.insert(account_id, transaction_index.clone());
        let trace = transaction_traces.remove(&cell.repr_hash());

        let mut doc = prepare_transaction_json(
            cell,
            transaction,
            &block_root,
            block_id.clone(),
            workchain_id,
            add_proof,
            trace,
        )
        .map_err(|e| anyhow::format_err!("{e}"))?;
        doc.insert("chain_order".to_string(), transaction_index.into());
        transaction_docs.push(doc_to_item(doc).data);
    }
    if !transaction_docs.is_empty() {
        archive.put_transactions(transaction_docs).map_err(|e| anyhow::format_err!("{e}"))?;
    }
    log::info!(target: "database", "TIME: prepare {} transactions {}ms;", tr_count, now.elapsed().as_millis());
    log::info!(target: "database",
        "Block seq_no: {}, tr_count: {}, id: {}",
        info.seq_no(),
        tr_count,
        block_id_hex
    );

    // Prepare accounts (changed and deleted)
    let now = std::time::Instant::now();
    let mut updated_acc_to_repo = HashMap::new();
    let mut account_docs = vec![];
    for account_id in changed_acc.iter() {
        log::trace!(target: "database", "reflect_block_in_db: prepare account: {}", account_id.to_hex_string());
        let acc = shard_accounts
            .account(account_id)
            .map_err(|e| anyhow::format_err!("{e}"))?
            .ok_or_else(|| {
                anyhow::format_err!(
                    "Block and shard state mismatch: \
                        state doesn't contain changed account"
                )
            })?;
        let last_trans_hash = acc.last_trans_hash().clone();
        let acc = acc.read_account().map_err(|e| anyhow::format_err!("{e}"))?;

        let prepared_account = prepare_account_record(
            acc.clone(),
            None,
            acc_last_trans_chain_order.remove(account_id),
            last_trans_hash,
        )?;
        if let Some(MsgAddressInt::AddrStd(address)) = acc.get_addr() {
            let fmt_addr = format!("{}:{}", address.workchain_id, address.address.to_hex_string());
            updated_acc_to_repo.insert(fmt_addr.to_string(), prepared_account.clone());
        }
        account_docs.push(prepared_account.data.clone());
    }

    for account_id in deleted_acc {
        let last_trans_chain_order = acc_last_trans_chain_order.remove(&account_id);
        let prepared_account =
            prepare_deleted_account_record(account_id, workchain_id, None, last_trans_chain_order)?;
        account_docs.push(prepared_account.data);
    }
    if !account_docs.is_empty() {
        archive.put_accounts(account_docs).map_err(|e| anyhow::format_err!("{e}"))?;
    }
    log::info!(target: "database", "TIME: accounts {} {}ms;", changed_acc.len(), now.elapsed().as_millis());

    // Prepare messages
    let now = std::time::Instant::now();
    let msg_count = messages.len(); // is 0 if not process_message
    let mut message_docs = vec![];
    let mut arch_messages = vec![];
    for (_, (json_map, arch_msg)) in messages {
        let item = doc_to_item(json_map);
        message_docs.push(item.data.clone());
        arch_messages.push(arch_msg);
    }
    if !message_docs.is_empty() {
        archive.put_arch_messages(arch_messages).map_err(|e| anyhow::format_err!("{e}"))?;
    }
    log::info!(target: "database", "TIME: prepare {} messages {}ms;", msg_count, now.elapsed().as_millis(),);

    // Block
    let now = std::time::Instant::now();
    let item =
        prepare_block_record(envelope, &block_root, block_boc, &file_hash, block_index.clone())
            .map_err(|e| anyhow::format_err!("{e}"))?;
    archive.put_block(item.clone()).map_err(|e| anyhow::format_err!("{e}"))?;
    log::info!(target: "database", "TIME: block({}) {}ms;", block_id_hex, now.elapsed().as_millis());
    log::info!(target: "database",
        "TIME: prepare & build jsons {}ms;   {}",
        now_all.elapsed().as_millis(),
        block_id_hex
    );
    Ok(updated_acc_to_repo)
}

pub(crate) fn prepare_messages_from_transaction(
    transaction: &Transaction,
    block_id: UInt256,
    transaction_id: UInt256,
    transaction_index: &str,
    block_root_for_proof: Option<&Cell>,
    messages: &mut HashMap<
        UInt256,
        (serde_json::value::Map<String, serde_json::Value>, ArchMessage),
    >,
) -> anyhow::Result<()> {
    if let Some(message_cell) = transaction.in_msg_cell() {
        let message_id = message_cell.repr_hash();

        let mut doc = if let Some(doc) = messages.get_mut(&message_id) {
            doc.to_owned()
        } else {
            let message = Message::construct_from_cell(message_cell.clone())
                .map_err(|e| anyhow::format_err!("{e}"))?;
            (
                prepare_message_json(
                    message_cell.clone(),
                    message.clone(),
                    block_root_for_proof,
                    block_id.clone(),
                    transaction_id.clone(),
                    Some(transaction.now()),
                )
                .map_err(|e| anyhow::format_err!("{e}"))?,
                prepare_message_archive_struct(
                    message_cell,
                    message,
                    block_root_for_proof,
                    block_id.clone(),
                    transaction_id.clone(),
                    Some(transaction.now()),
                )
                .map_err(|e| anyhow::format_err!("{e}"))?,
            )
        };

        doc.0.insert(
            "dst_chain_order".to_string(),
            format!("{}{}", transaction_index, tvm_block_json::u64_to_string(0)).into(),
        );
        doc.1.dst_chain_order =
            Some(format!("{}{}", transaction_index, tvm_block_json::u64_to_string(0)));

        messages.insert(message_id, doc);
    };

    let mut index: u64 = 1;
    transaction
        .out_msgs
        .iterate_slices(|slice| {
            let message_cell = slice.reference(0)?;
            let message_id = message_cell.repr_hash();

            let mut doc = if let Some(doc) = messages.get_mut(&message_id) {
                doc.to_owned()
            } else {
                let message = Message::construct_from_cell(message_cell.clone())?;
                (
                    prepare_message_json(
                        message_cell.clone(),
                        message.clone(),
                        block_root_for_proof,
                        block_id.clone(),
                        transaction_id.clone(),
                        None, // transaction_now affects ExtIn messages only
                    )?,
                    prepare_message_archive_struct(
                        message_cell,
                        message,
                        block_root_for_proof,
                        block_id.clone(),
                        transaction_id.clone(),
                        None, // transaction_now affects ExtIn messages only
                    )?,
                )
            };

            // messages are ordered by created_lt
            doc.0.insert(
                "src_chain_order".to_string(),
                format!("{}{}", transaction_index, tvm_block_json::u64_to_string(index)).into(),
            );
            doc.1.src_chain_order =
                Some(format!("{}{}", transaction_index, tvm_block_json::u64_to_string(index)));

            index += 1;
            messages.insert(message_id.clone(), doc);
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("{e}"))?;

    Ok(())
}

pub(crate) fn prepare_message_json(
    message_cell: Cell,
    message: Message,
    block_root_for_proof: Option<&Cell>,
    block_id: UInt256,
    transaction_id: UInt256,
    transaction_now: Option<u32>,
) -> tvm_types::Result<serde_json::value::Map<String, serde_json::Value>> {
    log::debug!("TIME: prepared_json...");
    let boc = write_boc(&message_cell)?;
    let proof = block_root_for_proof
        .map(|cell| write_boc(&message.prepare_proof(true, cell)?))
        .transpose()?;

    let set = tvm_block_json::MessageSerializationSet {
        message: message.clone(),
        id: message_cell.repr_hash(),
        block_id: Some(block_id.clone()),
        transaction_id: Some(transaction_id), /* it would be ambiguous for internal or replayed
                                               * messages */
        status: MessageProcessingStatus::Finalized,
        boc,
        proof,
        transaction_now, // affects ExtIn messages only
    };
    let mut doc = tvm_block_json::db_serialize_message("id", &set)?;
    if let Some(header) = message.int_header() {
        let src_dapp_id = header.src_dapp_id().as_ref().map(|dapp_id| dapp_id.to_hex_string());
        doc.insert("src_dapp_id".to_owned(), src_dapp_id.into());
    }
    Ok(doc)
}

pub(crate) fn prepare_message_archive_struct(
    message_cell: Cell,
    message: Message,
    block_root_for_proof: Option<&Cell>,
    block_id: UInt256,
    transaction_id: UInt256,
    transaction_now: Option<u32>,
) -> tvm_types::Result<ArchMessage> {
    log::debug!("TIME: prepared_struct...");
    let now = std::time::Instant::now();
    let boc = write_boc(&message_cell)?;
    let proof = block_root_for_proof
        .map(|cell| write_boc(&message.prepare_proof(true, cell)?))
        .transpose()?;

    let set = MessageSerializationSet {
        message,
        id: message_cell.repr_hash(),
        block_id: Some(block_id.clone()),
        transaction_id: Some(transaction_id), /* it would be ambiguous for internal or replayed
                                               * messages */
        status: MessageProcessingStatus::Finalized,
        boc,
        proof,
        transaction_now, // affects ExtIn messages only
    };
    let set = set.into();

    log::debug!("TIME: prepared_struct message {}ms", now.elapsed().as_millis());
    Ok(set)
}

pub(crate) fn doc_to_item(
    mut doc: serde_json::value::Map<String, serde_json::Value>,
) -> SerializedItem {
    let id_value = doc.get("id").expect("Item for db must contain id");
    let id = id_value.as_str().unwrap().to_owned();
    doc.insert("_key".to_string(), id_value.clone());

    let res = SerializedItem { id, data: serde_json::json!(&doc) };
    drop(doc);
    res
}

pub(crate) fn prepare_transaction_json(
    tr_cell: Cell,
    transaction: Transaction,
    block_root: &Cell,
    block_id: UInt256,
    workchain_id: i32,
    add_proof: bool,
    trace: Option<Vec<EngineTraceInfoData>>,
) -> anyhow::Result<serde_json::value::Map<String, serde_json::Value>> {
    let boc = write_boc(&tr_cell).map_err(|e| anyhow::format_err!("{e}"))?;
    let proof = if add_proof {
        Some(
            write_boc(
                &transaction.prepare_proof(block_root).map_err(|e| anyhow::format_err!("{e}"))?,
            )
            .map_err(|e| anyhow::format_err!("{e}"))?,
        )
    } else {
        None
    };
    let set = tvm_block_json::TransactionSerializationSet {
        transaction,
        id: tr_cell.repr_hash(),
        status: TransactionProcessingStatus::Finalized,
        block_id: Some(block_id.clone()),
        workchain_id,
        boc,
        proof,
    };
    let mut doc = tvm_block_json::db_serialize_transaction("id", &set)
        .map_err(|e| anyhow::format_err!("{e}"))?;
    if let Some(trace) = trace {
        doc.insert(
            "trace".to_owned(),
            serde_json::to_value(trace).map_err(|e| anyhow::format_err!("{e}"))?,
        );
    }
    Ok(doc)
}

pub(crate) fn prepare_account_record(
    account: Account,
    prev_account_state: Option<Account>,
    last_trans_chain_order: Option<String>,
    last_trans_hash: UInt256,
) -> anyhow::Result<SerializedItem> {
    let mut boc1 = None;
    if account.init_code_hash().is_some() {
        // new format
        let mut builder = BuilderData::new();
        account.write_original_format(&mut builder).map_err(|e| anyhow::format_err!("{e}"))?;
        boc1 = Some(
            write_boc(&builder.into_cell().map_err(|e| anyhow::format_err!("{e}"))?)
                .map_err(|e| anyhow::format_err!("{e}"))?,
        );
    }
    let boc = account.write_to_bytes().map_err(|e| anyhow::format_err!("{e}"))?;

    let prev_code_hash = prev_account_state.and_then(|account| account.get_code_hash());
    let set = tvm_block_json::AccountSerializationSet {
        account: account.clone(),
        prev_code_hash,
        proof: None,
        boc,
        boc1,
    };

    let mut doc =
        tvm_block_json::db_serialize_account("id", &set).map_err(|e| anyhow::format_err!("{e}"))?;
    if let Some(last_trans_chain_order) = last_trans_chain_order {
        doc.insert("last_trans_chain_order".to_owned(), last_trans_chain_order.into());
    }

    doc.insert("last_trans_hash".to_owned(), last_trans_hash.to_hex_string().into());
    let dapp_id = account.get_dapp_id().cloned().map(|dapp_id| dapp_id.to_hex_string());
    doc.insert("dapp_id".to_owned(), dapp_id.into());
    Ok(doc_to_item(doc))
}

pub(crate) fn prepare_deleted_account_record(
    account_id: AccountId,
    workchain_id: i32,
    prev_account_state: Option<Account>,
    last_trans_chain_order: Option<String>,
) -> anyhow::Result<SerializedItem> {
    let prev_code_hash = prev_account_state.and_then(|account| account.get_code_hash());
    let set =
        tvm_block_json::DeletedAccountSerializationSet { account_id, workchain_id, prev_code_hash };

    let mut doc = tvm_block_json::db_serialize_deleted_account("id", &set)
        .map_err(|e| anyhow::format_err!("{e}"))?;
    if let Some(last_trans_chain_order) = last_trans_chain_order {
        doc.insert("last_trans_chain_order".to_owned(), last_trans_chain_order.into());
    }
    Ok(doc_to_item(doc))
}

pub(crate) fn prepare_block_record(
    envelope: Envelope<GoshBLS, WrappedBlock>,
    block_root: &Cell,
    boc: &[u8],
    file_hash: &UInt256,
    block_order: String,
) -> anyhow::Result<SerializedItem> {
    let block = &envelope.data().block;
    let set = tvm_block_json::BlockSerializationSetFH {
        block,
        id: &block_root.repr_hash(),
        status: BlockProcessingStatus::Finalized,
        boc,
        file_hash: Some(file_hash),
    };
    let mut doc =
        tvm_block_json::db_serialize_block("id", set).map_err(|e| anyhow::format_err!("{e}"))?;
    doc.insert("chain_order".to_owned(), block_order.into());
    doc.insert("parent".to_owned(), envelope.data().parent().to_string().into());
    let aggregated_signature = bincode::serialize(envelope.aggregated_signature())?;
    doc.insert("aggregated_signature".to_owned(), aggregated_signature.into());
    let signature_occurrences = bincode::serialize(&envelope.clone_signature_occurrences())?;
    doc.insert("signature_occurrences".to_owned(), signature_occurrences.into());
    let share_state_resource_address = envelope.data().directives().share_state_resource_address;
    doc.insert("share_state_resource_address".to_owned(), share_state_resource_address.into());

    let block_info = block.read_info().map_err(|e| anyhow::format_err!("{e}"))?;
    doc.insert("flags".to_owned(), block_info.flags().into());
    doc.insert("gen_utime_ms_part".to_owned(), block_info.gen_utime_ms_part().into());
    let (gen_software_version, gen_software_capabilities) = match block_info.gen_software() {
        Some(gen_software) => (gen_software.version, gen_software.capabilities.to_string()),
        None => (0, "".to_string()),
    };
    doc.insert("gen_software_version".to_owned(), gen_software_version.into());
    doc.insert("gen_software_capabilities".to_owned(), gen_software_capabilities.into());
    let root_hash = block_root.repr_hash().to_hex_string();
    doc.insert("root_hash".to_owned(), root_hash.into());

    let extra = envelope.data().block.read_extra().map_err(|e| anyhow::format_err!("{e}"))?;
    let mut out_msgs: Vec<String> = Vec::new();
    let mut in_msgs: Vec<String> = Vec::new();
    extra
        .read_out_msg_descr()
        .map_err(|e| anyhow::format_err!("{e}"))?
        .iterate_with_keys(|id, _| {
            out_msgs.push(id.to_hex_string());
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("{e}"))?;
    extra
        .read_in_msg_descr()
        .map_err(|e| anyhow::format_err!("{e}"))?
        .iterate_with_keys(|id, _| {
            in_msgs.push(id.to_hex_string());
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("{e}"))?;
    doc.insert("in_msgs".to_owned(), serde_json::to_string(&in_msgs).ok().into());
    doc.insert("out_msgs".to_owned(), serde_json::to_string(&out_msgs).ok().into());
    let data = bincode::serialize(&envelope.data())?;
    doc.insert("data".to_owned(), data.into());

    Ok(doc_to_item(doc))
}

pub(crate) fn block_index(block: &Block) -> anyhow::Result<String> {
    let info = block.read_info().map_err(|e| anyhow::format_err!("{e}"))?;
    let gen_utime: u32 = info.gen_utime().into();
    let block_index = u64_to_string(gen_utime as u64);
    let placeholder = "00";
    let thread_prefix = u64_to_string(info.shard().shard_prefix_with_tag().reverse_bits());
    let thread_seq_no = u64_to_string(info.seq_no() as u64);

    Ok(block_index + placeholder + &thread_prefix + &thread_seq_no)
}
