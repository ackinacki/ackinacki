// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize, Serialize, Clone, Debug)]
struct TransactionCredit {
    credit: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct TransactionStorage {
    storage_fees_collected: String,
    status_change: u8,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct TransactionCompute {
    success: Option<bool>,
    msg_state_used: Option<bool>,
    account_activated: Option<bool>,
    gas_fees: Option<String>,
    gas_used: Option<f64>,
    gas_limit: Option<f64>,
    mode: Option<i8>,
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
    result_code: Option<i32>,
    tot_actions: Option<i16>,
    spec_actions: Option<i16>,
    skipped_actions: Option<i16>,
    msgs_created: Option<i16>,
    action_list_hash: Option<String>,
    tot_msg_size_cells: Option<f64>,
    tot_msg_size_bits: Option<f64>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ArchTransaction {
    id: String,
    block_id: String,
    boc: String,
    status: u8,
    credit: Option<TransactionCredit>,
    storage: Option<TransactionStorage>,
    compute: Option<TransactionCompute>,
    action: Option<TransactionAction>,
    credit_first: bool,
    aborted: bool,
    destroyed: bool,
    tr_type: u8,
    lt: String,
    prev_trans_hash: String,
    prev_trans_lt: String,
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
    chain_order: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FlatTransaction {
    pub id: String,
    pub block_id: String,
    pub boc: String,
    pub status: u8,
    pub credit: Option<String>,
    pub storage_fees_collected: String,
    pub storage_status_change: u8,
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
        let credit = tr.credit.map(|TransactionCredit { credit }| credit);
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
            credit,
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
