// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::ComplexObject;
use async_graphql::Enum;
use async_graphql::SimpleObject;

mod filter;
pub use filter::AccountFilter;

use super::currency::OtherCurrency;
use super::formats::BigIntFormat;
use crate::helpers::ecc_from_bytes;
use crate::helpers::format_big_int;
use crate::schema::db;

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub enum AccountStatusEnum {
    Uninit,
    Active,
    Frozen,
    NonExist,
}

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub(crate) enum AccountStatusChangeEnum {
    Unchanged,
    Frozen,
    Deleted,
}

/// # Account type
///
/// Recall that a smart contract and an account are the same thing in the
/// context of the TVM-based Blockchain, and that these terms can be used
/// interchangeably, at least as long as only small (or “usual”) smart contracts
/// are considered. A large smart-contract may employ several accounts lying in
/// different shardchains of the same workchain for load balancing purposes.
///
/// An account is identified by its full address and is completely described by
/// its state. In other words, there is nothing else in an account apart from
/// its address and state.
#[derive(SimpleObject, Clone)]
#[graphql(complex, rename_fields = "snake_case")]
pub(crate) struct Account {
    pub id: String,
    address: String,
    /// Returns the current status of the account.
    ///
    /// - 0 – uninit
    /// - 1 – active
    /// - 2 – frozen
    /// - 3 – nonExist
    acc_type: Option<u8>,
    acc_type_name: AccountStatusEnum,
    #[graphql(skip)]
    balance: Option<String>,
    balance_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    bits: Option<String>,
    /// Bag of cells with the account struct encoded as base64.
    boc: Option<String>,
    #[graphql(skip)]
    cells: Option<String>,
    /// If present, contains smart-contract code encoded with in base64.
    code: Option<String>,
    /// `code` field root hash.
    code_hash: String,
    /// If present, contains smart-contract data encoded with in base64.
    data: Option<String>,
    /// `data` field root hash.
    data_hash: String,
    dapp_id: Option<String>,
    #[graphql(skip)]
    due_payment: Option<String>,
    /// account 's initial code hash (when it was deployed).
    init_code_hash: String,
    /// Contains either the unixtime of the most recent storage payment
    /// collected (usually this is the unixtime of the most recent transaction),
    /// or the unixtime when the account was created (again, by a transaction).
    last_paid: Option<u64>,
    #[graphql(skip)]
    last_trans_lt: Option<String>,
    /// If present, contains library code used in smart-contract.
    library: Option<String>,
    /// `library` field root hash.
    library_hash: Option<String>,
    prev_code_hash: Option<String>,
    /// Merkle proof that account is a part of shard state it cut from as a bag
    /// of cells with Merkle proof struct encoded as base64.
    proof: Option<String>,
    #[graphql(skip)]
    public_cells: Option<String>,
    /// Is present and non-zero only in instances of large smart contracts.
    split_depth: Option<i32>,
    /// Contains the representation hash of an instance of `StateInit` when an
    /// account is frozen.
    state_hash: Option<String>,
    /// May be present only in the masterchain—and within the masterchain, only
    /// in some fundamental smart contracts required for the whole system to
    /// function.
    tick: Option<bool>,
    /// May be present only in the masterchain—and within the masterchain, only
    /// in some fundamental smart contracts required for the whole system to
    /// function.
    tock: Option<bool>,
    /// Workchain id of the account address (id field).
    workchain_id: i32,
}

impl From<u8> for AccountStatusEnum {
    fn from(val: u8) -> Self {
        match val {
            1 => AccountStatusEnum::Active,
            2 => AccountStatusEnum::Frozen,
            3 => AccountStatusEnum::NonExist,
            _ => AccountStatusEnum::Uninit,
        }
    }
}

impl From<u8> for AccountStatusChangeEnum {
    fn from(val: u8) -> Self {
        match val {
            1 => AccountStatusChangeEnum::Frozen,
            2 => AccountStatusChangeEnum::Deleted,
            _ => AccountStatusChangeEnum::Unchanged,
        }
    }
}

impl From<db::Account> for Account {
    fn from(acc: db::Account) -> Self {
        let balance_other = ecc_from_bytes(acc.balance_other).expect("Failed to decode ECC");
        let boc = acc.boc.map(tvm_types::base64_encode);
        let code = acc.code.map(tvm_types::base64_encode);
        let data = acc.data.map(tvm_types::base64_encode);
        Self {
            id: acc.id.clone(),
            address: acc.id,
            acc_type: Some(acc.acc_type),
            acc_type_name: acc.acc_type.into(),
            balance: Some(acc.balance),
            balance_other,
            bits: Some(acc.bits),
            boc,
            cells: Some(acc.cells),
            code,
            code_hash: acc.code_hash,
            data,
            data_hash: acc.data_hash,
            dapp_id: acc.dapp_id,
            due_payment: None,
            init_code_hash: acc.init_code_hash,
            last_paid: Some(acc.last_paid),
            last_trans_lt: Some(acc.last_trans_lt),
            library: None,
            library_hash: None,
            prev_code_hash: None,
            proof: None,
            public_cells: Some(acc.public_cells),
            split_depth: None,
            state_hash: None,
            tick: None,
            tock: None,
            workchain_id: acc.workchain_id,
        }
    }
}

#[ComplexObject]
impl Account {
    async fn balance(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.balance.clone(), format)
    }

    /// Contains sum of all the bits used by the cells of the account. Used in
    /// storage fee calculation.
    async fn bits(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.bits.clone(), format)
    }

    /// Contains number of the cells of the account. Used in storage fee
    /// calculation.
    async fn cells(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.cells.clone(), format)
    }

    #[graphql(name = "due_payment")]
    /// If present, accumulates the storage payments that could not be exacted
    /// from the balance of the account, represented by a strictly positive
    /// amount of nano tokens; it can be present only for uninitialized or
    /// frozen accounts that have a balance of zero Grams (but may have
    /// non-zero balances in non gram cryptocurrencies). When due_payment
    /// becomes larger than the value of a configurable parameter of the
    /// blockchain, the ac- count is destroyed altogether, and its balance,
    /// if any, is transferred to the zero account.
    async fn due_payment(&self, _format: Option<BigIntFormat>) -> Option<String> {
        self.due_payment.to_owned()
    }

    #[graphql(name = "last_trans_lt")]
    async fn last_trans_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.last_trans_lt.clone(), format)
    }

    #[graphql(name = "public_cells")]
    /// Contains the number of public cells of the account. Used in storage fee
    /// calculation.
    async fn public_cells(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.public_cells.clone(), format)
    }
}
