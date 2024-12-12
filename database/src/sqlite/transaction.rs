// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;
use tvm_block::AccStatusChange;
use tvm_block::AddSub;
use tvm_block::ComputeSkipReason;
use tvm_block::Deserializable;
use tvm_block::Grams;
use tvm_block::Message;
use tvm_block::TrActionPhase;
use tvm_block::TrBouncePhase;
use tvm_block::TrComputePhase;
use tvm_block::TrCreditPhase;
use tvm_block::TrStoragePhase;
use tvm_block::TransactionDescr;

use super::account::construct_address;
use super::account::serialize_account_status;
use super::message::get_msg_fees;
use crate::currency_collection::SignedCurrencyCollection;
use crate::helpers::u64_to_string;
use crate::serialization::TransactionSerializationSet;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct TransactionCredit {
    credit: Option<String>,
    due_fees_collected: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct TransactionStorage {
    storage_fees_collected: Option<String>,
    storage_fees_due: Option<String>,
    status_change: Option<u8>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct TransactionCompute {
    success: Option<bool>,
    msg_state_used: Option<bool>,
    account_activated: Option<bool>,
    gas_credit: Option<u32>,
    gas_fees: Option<String>,
    gas_used: Option<f64>,
    gas_limit: Option<f64>,
    mode: Option<i8>,
    exit_arg: Option<i32>,
    exit_code: Option<i32>,
    vm_steps: Option<u32>,
    vm_init_state_hash: Option<String>,
    vm_final_state_hash: Option<String>,
    compute_type: u8,
    skipped_reason: Option<u8>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct TransactionAction {
    success: Option<bool>,
    valid: Option<bool>,
    no_funds: Option<bool>,
    status_change: Option<u8>,
    result_arg: Option<i32>,
    result_code: Option<i32>,
    tot_actions: Option<i16>,
    spec_actions: Option<i16>,
    skipped_actions: Option<i16>,
    msgs_created: Option<i16>,
    action_list_hash: Option<String>,
    tot_msg_size_cells: Option<f64>,
    tot_msg_size_bits: Option<f64>,
    total_action_fees: Option<String>,
    total_fwd_fees: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct TransactionBounce {
    bounce_type: Option<i32>,
    fwd_fees: Option<String>,
    msg_fees: Option<String>,
    msg_size_bits: Option<f64>,
    msg_size_cells: Option<f64>,
    req_fwd_fees: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ArchTransaction {
    id: String,
    block_id: String,
    boc: Vec<u8>,
    bounce: Option<TransactionBounce>,
    status: u8,
    credit: Option<TransactionCredit>,
    storage: Option<TransactionStorage>,
    compute: Option<TransactionCompute>,
    action: Option<TransactionAction>,
    credit_first: bool,
    ext_in_msg_fee: Option<String>,
    aborted: bool,
    destroyed: bool,
    tr_type: u8,
    lt: String,
    prev_trans_hash: String,
    prev_trans_lt: String,
    proof: Option<Vec<u8>>,
    now: u32,
    outmsg_cnt: u16,
    orig_status: u8,
    end_status: u8,
    in_msg: String,
    out_msgs: Vec<String>,
    account_addr: String,
    workchain_id: i32,
    total_fees: String,
    balance_delta: String,
    old_hash: String,
    new_hash: String,
    pub chain_order: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FlatTransaction {
    pub id: String,
    pub block_id: String,
    pub boc: Vec<u8>,
    pub status: u8,
    pub credit: Option<String>,
    pub storage_fees_collected: Option<String>,
    pub storage_status_change: Option<u8>,
    pub compute_success: Option<bool>,
    pub compute_msg_state_used: Option<bool>,
    pub compute_account_activated: Option<bool>,
    pub compute_gas_fees: Option<String>,
    pub compute_gas_used: Option<f64>,
    pub compute_gas_limit: Option<f64>,
    pub compute_mode: Option<i8>,
    pub compute_exit_code: Option<i32>,
    pub compute_skipped_reason: Option<u8>,
    pub compute_vm_steps: Option<u32>,
    pub compute_vm_init_state_hash: Option<String>,
    pub compute_vm_final_state_hash: Option<String>,
    pub compute_type: u8,
    pub action_success: Option<bool>,
    pub action_valid: Option<bool>,
    pub action_no_funds: Option<bool>,
    pub action_status_change: Option<u8>,
    pub action_result_code: Option<i32>,
    pub action_tot_actions: Option<i16>,
    pub action_spec_actions: Option<i16>,
    pub action_skipped_actions: Option<i16>,
    pub action_msgs_created: Option<i16>,
    pub action_list_hash: Option<String>,
    pub action_tot_msg_size_cells: Option<f64>,
    pub action_tot_msg_size_bits: Option<f64>,
    pub credit_first: bool,
    pub aborted: bool,
    pub destroyed: bool,
    pub tr_type: u8,
    pub lt: String,
    pub prev_trans_hash: String,
    pub prev_trans_lt: String,
    pub now: u32,
    pub outmsg_cnt: u16,
    pub orig_status: u8,
    pub end_status: u8,
    pub in_msg: String,
    pub out_msgs: String,
    pub account_addr: String,
    pub workchain_id: i32,
    pub total_fees: String,
    pub balance_delta: String,
    pub old_hash: String,
    pub new_hash: String,
    pub chain_order: String,
}

impl From<ArchTransaction> for FlatTransaction {
    fn from(tr: ArchTransaction) -> Self {
        let credit = tr.credit.map(|TransactionCredit { credit, .. }| credit);
        let action = match tr.action {
            None => TransactionAction { ..Default::default() },
            Some(action) => action,
        };
        let compute = match tr.compute {
            None => TransactionCompute { ..Default::default() },
            Some(compute) => compute,
        };
        let storage = match tr.storage {
            None => TransactionStorage { ..Default::default() },
            Some(storage) => storage,
        };
        Self {
            id: tr.id,
            block_id: tr.block_id,
            boc: tr.boc,
            status: tr.status,
            credit: credit.flatten(),
            storage_fees_collected: storage.storage_fees_collected,
            storage_status_change: storage.status_change,
            compute_success: compute.success,
            compute_msg_state_used: compute.msg_state_used,
            compute_account_activated: compute.account_activated,
            compute_gas_fees: compute.gas_fees,
            compute_gas_used: compute.gas_used,
            compute_gas_limit: compute.gas_limit,
            compute_mode: compute.mode,
            compute_exit_code: compute.exit_code,
            compute_skipped_reason: compute.skipped_reason,
            compute_vm_steps: compute.vm_steps,
            compute_vm_init_state_hash: compute.vm_init_state_hash,
            compute_vm_final_state_hash: compute.vm_final_state_hash,
            compute_type: compute.compute_type,
            action_success: action.success,
            action_valid: action.valid,
            action_no_funds: action.no_funds,
            action_status_change: action.status_change,
            action_result_code: action.result_code,
            action_tot_actions: action.tot_actions,
            action_spec_actions: action.spec_actions,
            action_skipped_actions: action.skipped_actions,
            action_msgs_created: action.msgs_created,
            action_list_hash: action.action_list_hash,
            action_tot_msg_size_cells: action.tot_msg_size_cells,
            action_tot_msg_size_bits: action.tot_msg_size_bits,
            credit_first: tr.credit_first,
            aborted: tr.aborted,
            destroyed: tr.destroyed,
            tr_type: tr.tr_type,
            lt: tr.lt,
            prev_trans_hash: tr.prev_trans_hash,
            prev_trans_lt: tr.prev_trans_lt,
            now: tr.now,
            outmsg_cnt: tr.outmsg_cnt,
            orig_status: tr.orig_status,
            end_status: tr.end_status,
            in_msg: tr.in_msg,
            out_msgs: serde_json::to_string(&tr.out_msgs).unwrap_or("[]".to_string()),
            account_addr: tr.account_addr,
            workchain_id: tr.workchain_id,
            total_fees: tr.total_fees,
            balance_delta: tr.balance_delta,
            old_hash: tr.old_hash,
            new_hash: tr.new_hash,
            chain_order: tr.chain_order,
        }
    }
}

impl From<TransactionSerializationSet> for ArchTransaction {
    fn from(trx: TransactionSerializationSet) -> Self {
        let cc = trx.transaction.total_fees();
        let total_fees = format!("{:x}", cc.grams.as_u128());
        let mut arch_transaction = Self {
            id: trx.id.to_hex_string(),
            block_id: trx.block_id.unwrap().to_hex_string(),
            boc: trx.boc,
            end_status: serialize_account_status(&trx.transaction.end_status),
            lt: u64_to_string(trx.transaction.logical_time()),
            now: trx.transaction.now(),
            orig_status: serialize_account_status(&trx.transaction.orig_status),
            outmsg_cnt: trx.transaction.msg_count() as u16,
            prev_trans_hash: trx.transaction.prev_trans_hash().to_hex_string(),
            prev_trans_lt: u64_to_string(trx.transaction.prev_trans_lt()),
            proof: trx.proof,
            status: trx.status as u8,
            total_fees,
            ..Default::default()
        };

        if let Ok(state_update) = trx.transaction.read_state_update() {
            arch_transaction.old_hash = state_update.old_hash.to_hex_string();
            arch_transaction.new_hash = state_update.new_hash.to_hex_string();
        }

        let mut ext_in_msg_fee = None;

        match &trx.transaction.read_description() {
            Err(err) => tracing::error!("Failed to read transaction description: {err}"),
            Ok(tr_desc) => match tr_desc {
                TransactionDescr::Ordinary(tr) => {
                    let mut fees = trx.transaction.total_fees().grams;
                    let (storage, storage_fee) = serialize_storage_phase(tr.storage_ph.as_ref());
                    if let Some(fee) = storage_fee {
                        fees.sub(fee).expect("Failed to subtract storage fee");
                    }
                    arch_transaction.storage = storage;
                    arch_transaction.credit = serialize_credit_phase(tr.credit_ph.as_ref());
                    let (compute, compute_fee) = serialize_compute_phase(Some(&tr.compute_ph));
                    if let Some(fee) = compute_fee {
                        fees.sub(fee).expect("Failed to subtract compute fee");
                    }
                    arch_transaction.compute = compute;
                    let (action, action_fee) = serialize_action_phase(tr.action.as_ref());
                    if let Some(fee) = action_fee {
                        fees.sub(fee).expect("Failed to subtract action fee");
                    }
                    arch_transaction.action = action;
                    ext_in_msg_fee = Some(fees);
                    arch_transaction.bounce = serialize_bounce_phase(tr.bounce.as_ref());
                    arch_transaction.credit_first = tr.credit_first;
                    arch_transaction.aborted = tr.aborted;
                    arch_transaction.destroyed = tr.destroyed;
                    arch_transaction.tr_type = 0b0000;
                }
                _ => {
                    unimplemented!()
                }
            },
        }

        let mut balance_delta = SignedCurrencyCollection::new();
        let mut address_from_message = None;
        if let Some(msg) = &trx.transaction.in_msg {
            arch_transaction.in_msg = msg.hash().as_hex_string();

            let msg = msg.read_struct().expect("Failed to extract in msg from transaction");
            if let Some(value) = msg.get_value() {
                balance_delta.add(
                    &SignedCurrencyCollection::from_cc(value)
                        .expect("Failed to convert message value to SCC"),
                );
            }
            // IHR fee is added to account balance if IHR is not used or to total fees if
            // message delivered through IHR
            if let Some((ihr_fee, _)) = get_msg_fees(&msg) {
                balance_delta.grams += ihr_fee.as_u128();
            }
            address_from_message = msg.dst_ref().cloned();

            if msg.is_inbound_external() {
                arch_transaction.ext_in_msg_fee =
                    Some(ext_in_msg_fee.unwrap_or_default().as_u128().to_string());
            }
        }

        let mut out_ids = vec![];
        trx.transaction
            .out_msgs
            .iterate_slices(|slice| {
                if let Some(cell) = slice.reference_opt(0) {
                    out_ids.push(cell.repr_hash().as_hex_string());

                    let msg = Message::construct_from_cell(cell)?;
                    if let Some(value) = msg.get_value() {
                        balance_delta.sub(&SignedCurrencyCollection::from_cc(value)?);
                    }
                    if let Some((ihr_fee, fwd_fee)) = get_msg_fees(&msg) {
                        balance_delta.grams -= ihr_fee.as_u128();
                        balance_delta.grams -= fwd_fee.as_u128();
                    }
                    if address_from_message.is_none() {
                        address_from_message = msg.src_ref().cloned();
                    }
                }
                Ok(true)
            })
            .expect("Failed to iterate over outgoing messages");
        balance_delta
            .sub(&SignedCurrencyCollection::from_cc(trx.transaction.total_fees()).expect(""));
        // ??? bigint_to_string()
        arch_transaction.balance_delta = balance_delta.grams.to_string();

        arch_transaction.out_msgs = out_ids;
        let account_addr =
            construct_address(trx.workchain_id, trx.transaction.account_id().clone())
                .expect("Failed to construct account address (transaction owner)");
        arch_transaction.account_addr = account_addr.to_string();
        arch_transaction.workchain_id = trx.workchain_id;

        arch_transaction
    }
}

fn serialize_storage_phase(
    ph: Option<&TrStoragePhase>,
) -> (Option<TransactionStorage>, Option<&Grams>) {
    let mut storage = TransactionStorage { ..Default::default() };
    if let Some(ph) = ph {
        storage.storage_fees_collected = Some(ph.storage_fees_collected.as_u128().to_string());
        if let Some(grams) = &ph.storage_fees_due {
            storage.storage_fees_due = Some(grams.as_u128().to_string());
        }
        let status_change = match ph.status_change {
            AccStatusChange::Unchanged => 0,
            AccStatusChange::Frozen => 1,
            AccStatusChange::Deleted => 2,
        };
        storage.status_change = Some(status_change);
        (Some(storage), Some(&ph.storage_fees_collected))
    } else {
        (None, None)
    }
}

fn serialize_compute_phase(
    ph: Option<&TrComputePhase>,
) -> (Option<TransactionCompute>, Option<&Grams>) {
    let mut compute = TransactionCompute { ..Default::default() };
    let mut fees = None;
    compute.compute_type = match ph {
        Some(TrComputePhase::Skipped(ph)) => {
            let reason = match ph.reason {
                ComputeSkipReason::NoState => 0,
                ComputeSkipReason::BadState => 1,
                ComputeSkipReason::NoGas => 2,
                ComputeSkipReason::Suspended => 5,
            };
            compute.skipped_reason = reason.into();
            0
        }
        Some(TrComputePhase::Vm(ph)) => {
            compute.success = ph.success.into();
            compute.msg_state_used = ph.msg_state_used.into();
            compute.account_activated = ph.account_activated.into();
            compute.gas_fees = Some(ph.gas_fees.as_u128().to_string());
            fees = Some(&ph.gas_fees);
            compute.gas_used = Some(ph.gas_used.as_u64() as f64);
            compute.gas_limit = Some(ph.gas_limit.as_u64() as f64);
            if let Some(value) = ph.gas_credit.as_ref() {
                compute.gas_credit = value.as_u32().into();
            }
            compute.mode = ph.mode.into();
            compute.exit_code = ph.exit_code.into();
            compute.exit_arg = ph.exit_arg;
            compute.vm_steps = ph.vm_steps.into();
            compute.vm_init_state_hash = Some(ph.vm_init_state_hash.as_hex_string());
            compute.vm_final_state_hash = Some(ph.vm_final_state_hash.as_hex_string());
            1
        }
        None => return (None, None),
    };

    (Some(compute), fees)
}

fn serialize_credit_phase(ph: Option<&TrCreditPhase>) -> Option<TransactionCredit> {
    if let Some(ph) = ph {
        let mut credit = TransactionCredit { ..Default::default() };
        if let Some(grams) = &ph.due_fees_collected {
            credit.due_fees_collected = Some(grams.as_u128().to_string());
        }
        credit.credit = Some(ph.credit.grams.as_u128().to_string());
        Some(credit)
    } else {
        None
    }
}

fn serialize_action_phase(
    ph: Option<&TrActionPhase>,
) -> (Option<TransactionAction>, Option<&Grams>) {
    if let Some(ph) = ph {
        let mut action = TransactionAction { ..Default::default() };
        action.success = Some(ph.success);
        action.valid = Some(ph.valid);
        action.no_funds = Some(ph.no_funds);
        let status_change = match ph.status_change {
            AccStatusChange::Unchanged => 0,
            AccStatusChange::Frozen => 1,
            AccStatusChange::Deleted => 2,
        };
        action.status_change = Some(status_change);
        if let Some(grams) = ph.total_fwd_fees.as_ref() {
            action.total_fwd_fees = Some(grams.as_u128().to_string());
        }
        if let Some(grams) = ph.total_action_fees.as_ref() {
            action.total_action_fees = Some(grams.as_u128().to_string());
        }
        let fees = ph.total_action_fees.as_ref();
        action.result_code = Some(ph.result_code);
        action.result_arg = ph.result_arg;
        action.tot_actions = Some(ph.tot_actions);
        action.spec_actions = Some(ph.spec_actions);
        action.skipped_actions = Some(ph.skipped_actions);
        action.msgs_created = Some(ph.msgs_created);
        action.action_list_hash = Some(ph.action_list_hash.as_hex_string());
        action.tot_msg_size_cells = Some(ph.tot_msg_size.cells() as f64);
        action.tot_msg_size_bits = Some(ph.tot_msg_size.bits() as f64);
        (Some(action), fees)
    } else {
        (None, None)
    }
}

fn serialize_bounce_phase(ph: Option<&TrBouncePhase>) -> Option<TransactionBounce> {
    let mut bounce = TransactionBounce { ..Default::default() };
    let bounce_type = match ph {
        Some(TrBouncePhase::Negfunds) => 0,
        Some(TrBouncePhase::Nofunds(ph)) => {
            bounce.msg_size_cells = Some(ph.msg_size.cells() as f64);
            bounce.msg_size_bits = Some(ph.msg_size.bits() as f64);
            bounce.req_fwd_fees = Some(ph.req_fwd_fees.as_u128().to_string());
            1
        }
        Some(TrBouncePhase::Ok(ph)) => {
            bounce.msg_size_cells = Some(ph.msg_size.cells() as f64);
            bounce.msg_size_bits = Some(ph.msg_size.bits() as f64);
            bounce.msg_fees = Some(ph.msg_fees.as_u128().to_string());
            bounce.fwd_fees = Some(ph.fwd_fees.as_u128().to_string());
            2
        }
        None => return None,
    };
    bounce.bounce_type = Some(bounce_type);

    Some(bounce)
}

// pub fn bigint_to_string(value: &BigInt) -> String {
//     if Sign::Minus == value.sign() {
//         let bytes: Vec<u8> = value.to_bytes_be().1.iter().map(|byte| byte ^ 0xFF).collect();
//         let string = hex::encode(bytes).trim_start_matches('f').to_owned();
//         format!("-{:02x}{}", (string.len() - 1) ^ 0xFF, string)
//     } else {
//         format!("{:x}", value)
//     }
// }
