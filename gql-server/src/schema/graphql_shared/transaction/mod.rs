// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::ComplexObject;
use async_graphql::Enum;
use async_graphql::SimpleObject;

use super::account::AccountStatusChangeEnum;
use super::account::AccountStatusEnum;
use super::message::Message;
use crate::helpers::format_big_int;
use crate::schema::db;
use crate::schema::graphql_shared::formats::BigIntFormat;
// use super::message::Message;

mod filter;
pub(crate) use filter::TransactionFilter;
pub mod resolver;
pub use resolver::TransactionLoader;

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub(crate) enum TransactionProcessingStatusEnum {
    Unknown,
    Preliminary,
    Proposed,
    Finalized,
    Refused,
}

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub(crate) enum TransactionTypeEnum {
    Ordinary,
    Storage,
    Tick,
    Tock,
    SplitPrepare,
    SplitInstall,
    MergePrepare,
    MergeInstall,
}

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub(crate) enum BounceTypeEnum {
    NegFunds,
    NoFunds,
    Ok,
}

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub(crate) enum ComputeTypeEnum {
    Skipped,
    Vm,
}

#[derive(Enum, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReasonEnum {
    NoState,
    BadState,
    NoGas,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
struct TransactionAction {
    action_list_hash: String,
    msgs_created: Option<i16>,
    /// The flag indicates absence of funds required to create an outbound
    /// message.
    no_funds: bool,
    result_arg: Option<i32>,
    result_code: i32,
    skipped_actions: i16,
    spec_actions: i16,
    status_change: u8,
    status_change_name: AccountStatusChangeEnum,
    success: bool,
    tot_actions: i16,
    #[graphql(name = "total_msg_size_bits")]
    tot_msg_size_bits: Option<f64>,
    #[graphql(name = "total_msg_size_cells")]
    tot_msg_size_cells: Option<f64>,
    #[graphql(skip)]
    total_fwd_fees: Option<String>,
    #[graphql(skip)]
    total_action_fees: Option<String>,
    valid: bool,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct TransactionBounce {
    bounce_type: Option<i32>,
    bounce_type_name: Option<BounceTypeEnum>,
    #[graphql(skip)]
    fwd_fees: Option<String>,
    #[graphql(skip)]
    msg_fees: Option<String>,
    msg_size_bits: Option<f64>,
    msg_size_cells: Option<f64>,
    #[graphql(skip)]
    req_fwd_fees: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
struct TransactionCompute {
    /// The flag reflects whether this has resulted in the activation of a
    /// previously frozen, uninitialized or non-existent account.
    account_activated: bool,
    compute_type: u8,
    compute_type_name: ComputeTypeEnum,
    exit_arg: Option<i32>,
    /// These parameter represents the status values returned by TVM; for a
    /// successful transaction, exit_code has to be 0 or 1.
    exit_code: Option<i32>,
    /// This parameter may be non-zero only for external inbound messages.
    /// It is the lesser of either the amount of gas that can be paid from the
    /// account balance or the maximum gas credit.
    gas_credit: Option<i32>,
    #[graphql(skip)]
    gas_fees: Option<String>,
    #[graphql(skip)]
    gas_limit: Option<String>,
    #[graphql(skip)]
    gas_used: Option<String>,
    mode: i8,
    /// This parameter reflects whether the state passed in the message has been
    /// used. If it is set, the account_activated flag is used (see below).
    msg_state_used: bool,
    // /// Reason for skipping the compute phase. According to the specification, the phase
    // /// can be skipped due to the absence of funds to buy gas, absence of state of an account
    // /// or a message, failure to provide a valid state in the message.
    skipped_reason: Option<i32>,
    // ? skipped_reason_name: SkipReasonEnum
    /// This flag is set if and only if exit_code is either 0 or 1.
    success: bool,
    /// This parameter is the representation hashes of the resulting state of
    /// TVM.
    vm_final_state_hash: String,
    /// This parameter is the representation hashes of the original state of
    /// TVM.
    vm_init_state_hash: String,
    /// the total number of steps performed by TVM (usually equal to two plus
    /// the number of instructions executed, including implicit RETs).
    vm_steps: u64,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
struct TransactionCredit {
    #[graphql(skip)]
    credit: Option<String>,
    #[graphql(visible = false)]
    dummy: Option<bool>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct TransactionSplitInfo {
    acc_split_depth: Option<i32>,
    cur_shard_pfx_len: Option<i32>,
    sibling_addr: Option<String>,
    this_addr: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
struct TransactionStorage {
    status_change: Option<u8>,
    status_change_name: Option<AccountStatusChangeEnum>,
    #[graphql(skip)]
    storage_fees_collected: Option<String>,
    #[graphql(skip)]
    storage_fees_due: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct Transaction {
    pub id: String,
    aborted: bool,
    // account: BlockchainAccount,
    account_addr: Option<String>,
    action: TransactionAction,
    #[graphql(skip)]
    balance_delta: Option<String>,
    // /// Account balance change after the transaction. Because fwd_fee is collected
    // /// by the validators of the receiving shard, total_fees value does not include
    // /// Sum(out_msg.fwd_fee[]), but includes in_msg.fwd_fee.
    // /// The formula is:
    // /// balance_delta = in_msg.value - total_fees - Sum(out_msg.value[]) -
    // Sum(out_msg.fwd_fee[]). ? balance_delta_other: [OtherCurrency]
    // ! block: Block,
    block_id: String,
    boc: String,
    bounce: Option<TransactionBounce>,
    /// Collection-unique field for pagination and sorting.
    /// This field is designed to retain logical order.
    pub chain_order: String,
    /// Code hash of the account before transaction execution.
    /// If an account was not activated before execution then this field
    /// contains code hash after account execution.
    code_hash: Option<String>,
    /// Code hash of the account before transaction execution. If an account was
    /// not activated before execution then this field contains code hash
    /// after account execution.
    // code_hash: String,
    compute: TransactionCompute,
    credit: Option<TransactionCredit>,
    credit_first: bool,
    destroyed: bool,
    /// The end state of an account after a transaction, 1 is returned to
    /// indicate a finalized transaction at an active account.
    /// - 0 – uninit
    /// - 1 – active
    /// - 2 – frozen
    /// - 3 – nonExist
    end_status: u8,
    end_status_name: AccountStatusEnum,
    #[graphql(skip)]
    ext_in_msg_fee: Option<String>,
    pub in_message: Option<Message>,
    pub in_msg: String,
    installed: Option<bool>,
    #[graphql(skip)]
    lt: Option<String>,
    /// seq_no of masterchain block which commited shard block containing the
    /// transaction.
    master_seq_no: Option<f64>,
    /// Merkle update field.
    new_hash: Option<String>,
    now: u64,
    now_string: String,
    /// Merkle update field.
    old_hash: Option<String>,
    /// The initial state of account. Note that in this case the query may
    /// return 0, if the account was not active before the transaction and 1
    /// if it was already active.
    /// - 0 – uninit
    /// - 1 – active
    /// - 2 – frozen
    /// - 3 – nonExist
    orig_status: u8,
    orig_status_name: AccountStatusEnum,
    pub out_messages: Option<Vec<Option<Message>>>,
    pub out_msgs: Vec<String>,
    /// The number of generated outbound messages (one of the common transaction
    /// parameters defined by the specification).
    outmsg_cnt: u16,
    prepare_transaction: Option<String>,
    prev_trans_hash: Option<String>,
    #[graphql(skip)]
    prev_trans_lt: Option<String>,
    proof: Option<String>,
    split_info: Option<TransactionSplitInfo>,
    /// Transaction processing status.
    /// - 0 – unknown
    /// - 1 – preliminary
    /// - 2 – proposed
    /// - 3 – finalized
    /// - 4 – refused
    status: u8,
    status_name: TransactionProcessingStatusEnum,
    storage: TransactionStorage,
    #[graphql(skip)]
    total_fees: Option<String>,
    /// Same as above, but reserved for non gram coins that may appear in the
    /// blockchain.
    // ! total_fees_other: [OtherCurrency]
    /// Transaction type according to the original blockchain specification,
    /// clause 4.2.4.
    /// - 0 – ordinary
    /// - 1 – storage
    /// - 2 – tick
    /// - 3 – tock
    /// - 4 – splitPrepare
    /// - 5 – splitInstall
    /// - 6 – mergePrepare
    /// - 7 – mergeInstall
    tr_type: u8,
    tr_type_name: TransactionTypeEnum,
    /// VM debug trace.
    // ! trace: [TransactionTrace]
    tt: Option<String>,
    /// Workchain id of the account address (account_addr field).
    workchain_id: i32,
}

impl From<i32> for BounceTypeEnum {
    fn from(val: i32) -> Self {
        match val {
            1 => BounceTypeEnum::NoFunds,
            2 => BounceTypeEnum::Ok,
            _ => BounceTypeEnum::NegFunds,
        }
    }
}

impl From<u8> for ComputeTypeEnum {
    fn from(val: u8) -> Self {
        match val {
            1 => ComputeTypeEnum::Vm,
            _ => ComputeTypeEnum::Skipped,
        }
    }
}

impl From<u8> for SkipReasonEnum {
    fn from(val: u8) -> Self {
        match val {
            1 => SkipReasonEnum::BadState,
            2 => SkipReasonEnum::NoGas,
            _ => SkipReasonEnum::NoState,
        }
    }
}

impl From<u8> for TransactionProcessingStatusEnum {
    fn from(val: u8) -> Self {
        match val {
            1 => TransactionProcessingStatusEnum::Preliminary,
            2 => TransactionProcessingStatusEnum::Proposed,
            3 => TransactionProcessingStatusEnum::Finalized,
            4 => TransactionProcessingStatusEnum::Refused,
            _ => TransactionProcessingStatusEnum::Unknown,
        }
    }
}

impl From<u8> for TransactionTypeEnum {
    fn from(val: u8) -> Self {
        match val {
            1 => TransactionTypeEnum::Storage,
            2 => TransactionTypeEnum::Tick,
            3 => TransactionTypeEnum::Tock,
            4 => TransactionTypeEnum::SplitPrepare,
            5 => TransactionTypeEnum::SplitInstall,
            6 => TransactionTypeEnum::MergePrepare,
            7 => TransactionTypeEnum::MergeInstall,
            _ => TransactionTypeEnum::Ordinary,
        }
    }
}

impl From<db::Transaction> for Transaction {
    fn from(trx: db::Transaction) -> Self {
        let boc = tvm_types::base64_encode(trx.boc);
        let action = TransactionAction {
            action_list_hash: trx.action_list_hash,
            msgs_created: Some(trx.action_msgs_created),
            no_funds: trx.action_no_funds,
            result_arg: None,
            result_code: trx.action_result_code,
            skipped_actions: trx.action_skipped_actions,
            spec_actions: trx.action_spec_actions,
            status_change: trx.action_status_change,
            status_change_name: trx.action_status_change.into(),
            success: trx.action_success,
            tot_actions: trx.action_tot_actions,
            tot_msg_size_bits: Some(trx.action_tot_msg_size_bits),
            tot_msg_size_cells: Some(trx.action_tot_msg_size_cells),
            total_action_fees: None,
            total_fwd_fees: None,
            valid: trx.action_valid,
        };
        let compute = TransactionCompute {
            account_activated: trx.compute_account_activated,
            compute_type: trx.compute_type,
            compute_type_name: trx.compute_type.into(),
            exit_arg: None,
            exit_code: Some(trx.compute_exit_code),
            gas_credit: None,
            gas_fees: Some(trx.compute_gas_fees),
            gas_limit: Some(trx.compute_gas_limit.to_string()),
            gas_used: Some(trx.compute_gas_used.to_string()),
            mode: trx.compute_mode,
            msg_state_used: trx.compute_msg_state_used,
            skipped_reason: None,
            success: trx.compute_success,
            vm_final_state_hash: trx.compute_vm_final_state_hash,
            vm_init_state_hash: trx.compute_vm_init_state_hash,
            vm_steps: trx.compute_vm_steps,
        };
        let credit =
            trx.credit.map(|credit| TransactionCredit { credit: Some(credit), dummy: None });
        let storage = TransactionStorage {
            status_change: trx.storage_status_change,
            status_change_name: Some(trx.storage_status_change.unwrap().into()),
            storage_fees_collected: trx.storage_fees_collected,
            storage_fees_due: None,
        };
        Self {
            id: trx.id,
            aborted: trx.aborted,
            account_addr: Some(trx.account_addr),
            action,
            balance_delta: Some(trx.balance_delta),
            block_id: trx.block_id,
            boc,
            bounce: None,
            chain_order: trx.chain_order,
            code_hash: None, // !!
            compute,
            credit,
            credit_first: trx.credit_first,
            destroyed: trx.destroyed,
            end_status: trx.end_status,
            end_status_name: trx.end_status.into(),
            ext_in_msg_fee: None, // !!
            in_message: None,
            in_msg: trx.in_msg,
            installed: None, // !!
            lt: Some(trx.lt),
            master_seq_no: None,
            new_hash: Some(trx.new_hash),
            now: trx.now,
            now_string: trx.now.to_string(),
            old_hash: Some(trx.old_hash),
            orig_status: trx.orig_status,
            orig_status_name: trx.orig_status.into(),
            out_messages: None,
            out_msgs: serde_json::from_str::<Vec<String>>(&trx.out_msgs).unwrap_or_default(),
            outmsg_cnt: trx.outmsg_cnt,
            prepare_transaction: None,
            prev_trans_hash: Some(trx.prev_trans_hash),
            prev_trans_lt: Some(trx.prev_trans_lt),
            proof: None,
            split_info: None,
            status: trx.status,
            status_name: trx.status.into(),
            storage,
            total_fees: Some(trx.total_fees),
            tr_type: trx.tr_type,
            tr_type_name: trx.tr_type.into(),
            tt: None,
            workchain_id: trx.workchain_id,
        }
    }
}

#[ComplexObject]
impl TransactionBounce {
    #[graphql(name = "fwd_fees")]
    async fn fwd_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.fwd_fees.clone(), format)
    }

    #[graphql(name = "msg_fees")]
    async fn msg_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.msg_fees.clone(), format)
    }

    #[graphql(name = "req_fwd_fees")]
    async fn req_fwd_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.req_fwd_fees.clone(), format)
    }
}

#[ComplexObject]
impl TransactionAction {
    #[graphql(name = "total_fwd_fees")]
    async fn total_fwd_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.total_fwd_fees.clone(), format)
    }

    #[graphql(name = "total_action_fees")]
    async fn total_action_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.total_action_fees.clone(), format)
    }
}

#[ComplexObject]
impl TransactionCompute {
    #[graphql(name = "gas_fees")]
    /// This parameter reflects the total gas fees collected by the validators
    /// for executing this transaction. It must be equal to the product of
    /// gas_used and gas_price from the current block header.
    async fn gas_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.gas_fees.clone(), format)
    }

    #[graphql(name = "gas_limit")]
    /// This parameter reflects the gas limit for this instance of TVM.
    /// It equals the lesser of either the Grams credited in the credit phase
    /// from the value of the inbound message divided by the current gas price,
    /// or the global per-transaction gas limit.
    async fn gas_limit(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.gas_limit.clone(), format)
    }

    #[graphql(name = "gas_used")]
    async fn gas_used(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.gas_used.clone(), format)
    }
}

#[ComplexObject]
impl TransactionCredit {
    #[graphql(name = "credit")]
    async fn credit(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.credit.clone(), format)
    }

    #[graphql(name = "due_fees_collected")]
    /// The sum of due_fees_collected and credit must equal the value of the
    /// message received, plus its ihr_fee if the message has not been
    /// received via Instant Hypercube Routing, IHR (otherwise the ihr_fee
    /// is awarded to the validators).
    async fn due_fees_collected(&self, _format: Option<BigIntFormat>) -> Option<String> {
        None
    }
}

#[ComplexObject]
impl TransactionStorage {
    #[graphql(name = "storage_fees_collected")]
    /// This field defines the amount of storage fees collected in grams.
    async fn storage_fees_collected(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.storage_fees_collected.clone(), format)
    }

    #[graphql(name = "storage_fees_due")]
    /// This field represents the amount of due fees in grams, it might be
    /// empty.
    async fn storage_fees_due(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.storage_fees_due.clone(), format)
    }
}
#[ComplexObject]
impl Transaction {
    // #[graphql(name = "account_addr")]
    // async fn account_addr(&self, format: Option<BigIntFormat>) -> Option<String>
    // {     format_big_int(self.account_addr.clone(), format)
    // }

    #[graphql(name = "balance_delta")]
    /// Account balance change after the transaction. Because fwd_fee is
    /// collected by the validators of the receiving shard, total_fees value
    /// does not include Sum(out_msg.fwd_fee[]), but includes
    /// in_msg.fwd_fee. The formula is:
    /// balance_delta = in_msg.value - total_fees - Sum(out_msg.value[]) -
    /// Sum(out_msg.fwd_fee[])
    async fn balance_delta(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.balance_delta.clone(), format)
    }

    #[graphql(name = "ext_in_msg_fee")]
    /// Fee for inbound external message import.
    async fn ext_in_msg_fee(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.ext_in_msg_fee.clone(), format)
    }

    #[graphql(name = "lt")]
    /// Logical time. A component of the TVM-based Blockchain that also plays an
    /// important role in message delivery is the logical time, usually
    /// denoted by Lt. It is a non-negative 64-bit integer, assigned to
    /// certain events. For more details, see [the TON blockchain specification](https://test.ton.org/tblkch.pdf).
    async fn lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.lt.clone(), format)
    }

    #[graphql(name = "prev_trans_lt")]
    async fn prev_trans_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.prev_trans_lt.clone(), format)
    }

    #[graphql(name = "total_fees")]
    /// Total amount of fees collected by the validators.
    /// Because fwd_fee is collected by the validators of the receiving shard,
    /// total_fees value does not include Sum(out_msg.fwd_fee[]), but includes
    /// in_msg.fwd_fee. The formula is:
    /// total_fees = in_msg.value - balance_delta - Sum(out_msg.value[]) -
    /// Sum(out_msg.fwd_fee[])
    async fn total_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.total_fees.clone(), format)
    }
}
