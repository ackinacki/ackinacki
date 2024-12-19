// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::RandomState;
use std::sync::Arc;

use database::documents_db::DocumentsDb;
use database::serialization::AccountSerializationSet;
use database::serialization::BlockSerializationSetFH;
use database::serialization::DeletedAccountSerializationSet;
use database::serialization::MessageSerializationSet;
use database::serialization::TransactionSerializationSet;
use database::sqlite::ArchAccount;
use database::sqlite::ArchBlock;
use database::sqlite::ArchMessage;
use database::sqlite::ArchTransaction;
use tvm_block::Account;
use tvm_block::AccountBlock;
use tvm_block::AccountStatus;
use tvm_block::Block;
use tvm_block::BlockProcessingStatus;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_block::Message;
use tvm_block::MessageProcessingStatus;
use tvm_block::MsgAddrStd;
use tvm_block::MsgAddressInt;
use tvm_block::Serializable;
use tvm_block::ShardStateUnsplit;
use tvm_block::Transaction;
use tvm_block::TransactionProcessingStatus;
use tvm_types::write_boc;
use tvm_types::AccountId;
use tvm_types::Cell;
use tvm_types::HashmapType;
use tvm_types::SliceData;
use tvm_types::UInt256;

use crate::block::producer::builder::EngineTraceInfoData;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::types::AckiNackiBlock;

lazy_static::lazy_static!(
    static ref ACCOUNT_NONE_HASH: UInt256 = Account::default().serialize().unwrap().repr_hash();
    pub static ref MINTER_ADDRESS: MsgAddressInt =
        MsgAddressInt::AddrStd(MsgAddrStd::with_address(None, 0, [0; 32].into()));
);

pub fn reflect_block_in_db(
    archive: Arc<dyn DocumentsDb>,
    envelope: Envelope<GoshBLS, AckiNackiBlock<GoshBLS>>,
    shard_state: Arc<ShardStateUnsplit>,
    transaction_traces: &mut HashMap<UInt256, Vec<EngineTraceInfoData>, RandomState>,
) -> anyhow::Result<()> {
    let now_all = std::time::Instant::now();

    let block = &envelope.data().tvm_block();
    let block_root =
        block.serialize().map_err(|e| anyhow::format_err!("Failed to serialize block: {e}"))?;
    let serialized_block = tvm_types::write_boc(&block_root).unwrap();

    let add_proof = false;
    let block_id = &block_root.repr_hash();
    let block_id_hex = block_id.to_hex_string();
    tracing::info!(target: "database", "reflect_block_in_db block_id: {}", block_id_hex);
    let file_hash = UInt256::calc_file_hash(&serialized_block);
    let block_extra =
        block.read_extra().map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
    let block_boc = &serialized_block;
    let info =
        block.read_info().map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?;
    let block_index = block_index(block)?;
    let workchain_id = info.shard().workchain_id();
    let shard_accounts = shard_state
        .read_accounts()
        .map_err(|e| anyhow::format_err!("Failed to read block accounts: {e}"))?;

    // Prepare sorted tvm_block transactions and addresses of changed accounts
    let mut changed_acc = HashSet::new();
    let mut deleted_acc = HashSet::new();
    let mut acc_last_trans_chain_order = HashMap::new();
    let now = std::time::Instant::now();
    let mut tr_count = 0;
    let mut transactions = BTreeMap::new();
    block_extra
        .read_account_blocks()
        .map_err(|e| anyhow::format_err!("Failed to read account blocks: {e}"))?
        .iterate_objects(|account_block: AccountBlock| {
            // extract ids of changed accounts
            let state_upd = account_block.read_state_update()?;
            let mut check_account_existed = false;
            if state_upd.new_hash == *ACCOUNT_NONE_HASH {
                tracing::trace!(target: "database",
                    "reflect_block_in_db: deleted acc: {}",
                    account_block.account_id().to_hex_string()
                );
                deleted_acc.insert(account_block.account_id().clone());
                if state_upd.old_hash == *ACCOUNT_NONE_HASH {
                    check_account_existed = true;
                }
            } else {
                tracing::trace!(target: "database",
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
                tracing::trace!(target: "database",
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
        .map_err(|e| anyhow::format_err!("Failed to iterate account blocks: {e}"))?;
    tracing::info!(target: "database",
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
        let transaction_index = format!("{}{}", block_index, u64_to_string(index as u64));

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

        let mut doc = prepare_transaction_archive_struct(
            cell,
            transaction,
            &block_root,
            block_id.clone(),
            workchain_id,
            add_proof,
            trace,
        )
        .map_err(|e| anyhow::format_err!("{e}"))?;
        doc.chain_order = transaction_index;
        transaction_docs.push(doc);
    }
    if !transaction_docs.is_empty() {
        archive
            .put_transactions(transaction_docs)
            .map_err(|e| anyhow::format_err!("Failed to put tx: {e}"))?;
    }
    tracing::info!(target: "database", "TIME: prepare {} transactions {}ms;", tr_count, now.elapsed().as_millis());
    tracing::info!(target: "database",
        "Block seq_no: {}, tr_count: {}, id: {}",
        info.seq_no(),
        tr_count,
        block_id_hex
    );

    // Prepare accounts (changed and deleted)
    let now = std::time::Instant::now();
    let mut account_docs = vec![];
    for account_id in changed_acc.iter() {
        tracing::trace!(target: "database", "reflect_block_in_db: prepare account: {}", account_id.to_hex_string());
        let acc = shard_accounts
            .account(account_id)
            .map_err(|e| anyhow::format_err!("Failed to read account: {e}"))?;

        // TODO remove this workaround after implementing state parsing in the BM
        if acc.is_none() {
            tracing::error!(
                "Block and shard state mismatch: state doesn't contain changed account"
            );
            continue;
        }

        let acc = acc.unwrap();
        let last_trans_hash = acc.last_trans_hash().clone();
        let acc =
            acc.read_account().map_err(|e| anyhow::format_err!("Failed to read account: {e}"))?;

        if acc.get_addr().is_some() {
            let prepared_account = prepare_account_archive_struct(
                acc.clone(),
                None,
                acc_last_trans_chain_order.remove(account_id),
                last_trans_hash,
            )?;
            account_docs.push(prepared_account);
        } else {
            tracing::debug!("account does not have an address: {acc:?}");
        }
    }

    for account_id in deleted_acc {
        let last_trans_chain_order = acc_last_trans_chain_order.remove(&account_id);
        let prepared_account = prepare_deleted_account_archive_struct(
            account_id,
            workchain_id,
            None,
            last_trans_chain_order,
        )?;
        account_docs.push(prepared_account);
    }
    if !account_docs.is_empty() {
        archive
            .put_accounts(account_docs)
            .map_err(|e| anyhow::format_err!("Failed to put account: {e}"))?;
    }
    tracing::info!(target: "database", "TIME: accounts {} {}ms;", changed_acc.len(), now.elapsed().as_millis());

    // Prepare messages
    let now = std::time::Instant::now();
    let msg_count = messages.len(); // is 0 if not process_message
    let mut arch_messages = vec![];
    for (_, arch_msg) in messages {
        arch_messages.push(arch_msg);
    }
    if !arch_messages.is_empty() {
        archive.put_messages(arch_messages).map_err(|e| anyhow::format_err!("{e}"))?;
    }
    tracing::info!(target: "database", "TIME: prepare {} messages {}ms;", msg_count, now.elapsed().as_millis(),);

    // Block
    let now = std::time::Instant::now();
    let item = prepare_block_archive_struct(
        envelope,
        &block_root,
        block_boc,
        &file_hash,
        block_index.clone(),
    )
    .map_err(|e| anyhow::format_err!("{e}"))?;
    archive.put_block(item).map_err(|e| anyhow::format_err!("{e}"))?;
    tracing::info!(target: "database", "TIME: block({}) {}ms;", block_id_hex, now.elapsed().as_millis());
    tracing::info!(target: "database",
        "TIME: prepare & build jsons {}ms;   {}",
        now_all.elapsed().as_millis(),
        block_id_hex
    );
    Ok(())
}

pub(crate) fn prepare_messages_from_transaction(
    transaction: &Transaction,
    block_id: UInt256,
    transaction_id: UInt256,
    transaction_index: &str,
    block_root_for_proof: Option<&Cell>,
    messages: &mut HashMap<UInt256, ArchMessage>,
) -> anyhow::Result<()> {
    if let Some(message_cell) = transaction.in_msg_cell() {
        let message_id = message_cell.repr_hash();

        let mut doc = if let Some(doc) = messages.get_mut(&message_id) {
            doc.to_owned()
        } else {
            let message = Message::construct_from_cell(message_cell.clone())
                .map_err(|e| anyhow::format_err!("{e}"))?;
            prepare_message_archive_struct(
                message_cell,
                message,
                block_root_for_proof,
                block_id.clone(),
                None,
                Some(transaction.now()),
            )
            .map_err(|e| anyhow::format_err!("{e}"))?
        };

        doc.dst_chain_order = Some(format!("{}{}", transaction_index, u64_to_string(0)));

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
                prepare_message_archive_struct(
                    message_cell,
                    message,
                    block_root_for_proof,
                    block_id.clone(),
                    Some(transaction_id.clone()),
                    None, // transaction_now affects ExtIn messages only
                )?
            };

            // messages are ordered by created_lt
            doc.src_chain_order = Some(format!("{}{}", transaction_index, u64_to_string(index)));

            index += 1;
            messages.insert(message_id.clone(), doc);
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("Failed to iterate out messages: {e}"))?;

    Ok(())
}

pub(crate) fn prepare_message_archive_struct(
    message_cell: Cell,
    message: Message,
    block_root_for_proof: Option<&Cell>,
    block_id: UInt256,
    transaction_id: Option<UInt256>,
    transaction_now: Option<u32>,
) -> tvm_types::Result<ArchMessage> {
    let boc = write_boc(&message_cell)?;
    let proof = block_root_for_proof
        .map(|cell| write_boc(&message.prepare_proof(true, cell)?))
        .transpose()?;

    let set = MessageSerializationSet {
        message,
        id: message_cell.repr_hash(),
        block_id: Some(block_id.clone()),
        transaction_id, // it would be ambiguous for internal or replayed messages
        status: MessageProcessingStatus::Finalized,
        boc,
        proof,
        transaction_now, // affects ExtIn messages only
    };

    Ok(set.into())
}

pub(crate) fn prepare_transaction_archive_struct(
    tr_cell: Cell,
    transaction: Transaction,
    block_root: &Cell,
    block_id: UInt256,
    workchain_id: i32,
    add_proof: bool,
    _trace: Option<Vec<EngineTraceInfoData>>,
) -> anyhow::Result<ArchTransaction> {
    let boc = write_boc(&tr_cell).map_err(|e| anyhow::format_err!("{e}"))?;
    let proof = if add_proof {
        Some(
            write_boc(
                &transaction
                    .prepare_proof(block_root)
                    .map_err(|e| anyhow::format_err!("Failed to prepare tx proof: {e}"))?,
            )
            .map_err(|e| anyhow::format_err!("Failed to write boc to bytes: {e}"))?,
        )
    } else {
        None
    };
    let set = TransactionSerializationSet {
        transaction,
        id: tr_cell.repr_hash(),
        status: TransactionProcessingStatus::Finalized,
        block_id: Some(block_id.clone()),
        workchain_id,
        boc,
        proof,
    };

    // if let Some(trace) = trace {
    //     set.trace = serde_json::to_value(trace).map_err(|e| anyhow::format_err!("{e}"))?;
    // }

    Ok(set.into())
}

pub(crate) fn prepare_account_archive_struct(
    account: Account,
    prev_account_state: Option<Account>,
    last_trans_chain_order: Option<String>,
    last_trans_hash: UInt256,
) -> anyhow::Result<ArchAccount> {
    let boc = account.write_to_bytes().map_err(|e| anyhow::format_err!("{e}"))?;

    let prev_code_hash = prev_account_state.and_then(|account| account.get_code_hash());
    let set = AccountSerializationSet {
        account: account.clone(),
        prev_code_hash,
        proof: None,
        boc,
        boc1: None,
    };
    let mut set: ArchAccount = set.into();
    if let Some(last_trans_chain_order) = last_trans_chain_order {
        set.last_trans_chain_order = last_trans_chain_order.into();
    }

    set.last_trans_hash = last_trans_hash.to_hex_string().into();

    Ok(set)
}

pub(crate) fn prepare_deleted_account_archive_struct(
    account_id: AccountId,
    workchain_id: i32,
    prev_account_state: Option<Account>,
    last_trans_chain_order: Option<String>,
) -> anyhow::Result<ArchAccount> {
    let prev_code_hash = prev_account_state.and_then(|account| account.get_code_hash());
    let set = DeletedAccountSerializationSet { account_id, workchain_id, prev_code_hash };

    let mut set: ArchAccount = set.into();
    if let Some(last_trans_chain_order) = last_trans_chain_order {
        set.last_trans_chain_order = last_trans_chain_order.into();
    }

    Ok(set)
}

pub(crate) fn prepare_block_archive_struct(
    envelope: Envelope<GoshBLS, AckiNackiBlock<GoshBLS>>,
    block_root: &Cell,
    boc: &[u8],
    file_hash: &UInt256,
    block_order: String,
) -> anyhow::Result<ArchBlock> {
    let block = envelope.data().tvm_block();
    let set = BlockSerializationSetFH {
        block: block.clone(),
        id: block_root.repr_hash(),
        status: BlockProcessingStatus::Finalized,
        boc: boc.to_vec(),
        file_hash: Some(file_hash.clone()),
    };

    let mut set: ArchBlock = set.into();

    set.chain_order = Some(block_order);
    set.parent = envelope.data().parent().to_string();
    set.aggregated_signature = Some(bincode::serialize(envelope.aggregated_signature())?);
    set.signature_occurrences = Some(bincode::serialize(&envelope.clone_signature_occurrences())?);
    set.share_state_resource_address = envelope.data().directives().share_state_resource_address;
    let common_section = envelope.data().get_common_section();
    set.thread_id = Some(hex::encode(common_section.thread_id));

    let block_info = block.read_info().map_err(|e| anyhow::format_err!("{e}"))?;
    set.flags = Some(block_info.flags() as i64);
    set.gen_utime_ms_part = Some(block_info.gen_utime_ms_part() as i64);
    set.root_hash = Some(block_root.repr_hash().to_hex_string());

    let extra =
        block.read_extra().map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
    let mut out_msgs: Vec<String> = Vec::new();
    let mut in_msgs: Vec<String> = Vec::new();
    extra
        .read_out_msg_descr()
        .map_err(|e| anyhow::format_err!("Failed to rad block out msg descr: {e}"))?
        .iterate_with_keys(|id, _| {
            out_msgs.push(id.to_hex_string());
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("Failed to iterate out msg descr: {e}"))?;
    extra
        .read_in_msg_descr()
        .map_err(|e| anyhow::format_err!("Failed to read block in msg descr: {e}"))?
        .iterate_with_keys(|id, _| {
            in_msgs.push(id.to_hex_string());
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("{e}"))?;
    set.in_msgs = serde_json::to_string(&in_msgs).ok();
    set.out_msgs = serde_json::to_string(&out_msgs).ok();

    let mut total_tr_count = 0;
    let extra_account_blocks =
        extra.read_account_blocks().map_err(|e| anyhow::format_err!("{e}"))?;
    extra_account_blocks
        .iterate_objects(|account_block| {
            let tr_count = account_block.transaction_count()?;
            total_tr_count += tr_count;
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("{e}"))?;
    set.tr_count = Some(total_tr_count as i64);

    let data = bincode::serialize(&envelope.data())?;
    set.data = Some(data);

    Ok(set)
}

pub(crate) fn block_index(block: &Block) -> anyhow::Result<String> {
    let info =
        block.read_info().map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?;
    let gen_utime: u32 = info.gen_utime().into();
    let block_index = u64_to_string(gen_utime as u64);
    let placeholder = "00";
    let thread_prefix = u64_to_string(info.shard().shard_prefix_with_tag().reverse_bits());
    let thread_seq_no = u64_to_string(info.seq_no() as u64);

    Ok(block_index + placeholder + &thread_prefix + &thread_seq_no)
}

pub fn u64_to_string(value: u64) -> String {
    let mut string = format!("{:x}", value);
    string.insert_str(0, &format!("{:x}", string.len() - 1));
    string
}
