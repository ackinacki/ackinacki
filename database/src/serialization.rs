// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tvm_block::Account;
use tvm_block::Block;
use tvm_block::BlockId;
use tvm_block::BlockProcessingStatus;
use tvm_block::Message;
use tvm_block::MessageId;
use tvm_block::MessageProcessingStatus;
use tvm_block::Transaction;
use tvm_block::TransactionId;
use tvm_block::TransactionProcessingStatus;
use tvm_types::AccountId;
use tvm_types::UInt256;

#[derive(Debug, Default)]
pub struct AccountSerializationSet {
    pub account: Account,
    pub prev_code_hash: Option<UInt256>,
    pub boc: Vec<u8>,
    pub boc1: Option<Vec<u8>>,
    pub proof: Option<Vec<u8>>,
}

pub struct BlockSerializationSetFH {
    pub block: Block,
    pub id: BlockId,
    pub status: BlockProcessingStatus,
    pub boc: Vec<u8>,
    pub file_hash: Option<UInt256>,
}

#[derive(Default)]
pub struct DeletedAccountSerializationSet {
    pub account_id: AccountId,
    pub prev_code_hash: Option<UInt256>,
    pub workchain_id: i32,
}

#[derive(Default)]
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

#[derive(Default)]
pub struct TransactionSerializationSet {
    pub transaction: Transaction,
    pub id: TransactionId,
    pub status: TransactionProcessingStatus,
    pub block_id: Option<BlockId>,
    pub workchain_id: i32,
    pub boc: Vec<u8>,
    pub proof: Option<Vec<u8>>,
}
