// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::ComplexObject;
use async_graphql::Enum;
use async_graphql::SimpleObject;
use chrono::DateTime;
use chrono::Utc;

use crate::helpers::format_big_int;
use crate::helpers::ToBool;
use crate::helpers::ToFloat;
use crate::helpers::ToInt;
use crate::schema::db::message::InBlockMessage;
use crate::schema::db::{self};

pub mod filter;
pub use filter::BlockFilter;
pub mod resolver;
pub use resolver::BlockLoader;

use super::currency::OtherCurrency;
use super::formats::BigIntFormat;
use super::message::InMsg;
use super::message::OutMsg;

type Boolean = bool;
type Int = i32;
type Float = f64;

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum BlockProcessingStatusEnum {
    Unknown,
    Proposed,
    Finalized,
    Refused,
}

impl From<Option<i64>> for BlockProcessingStatusEnum {
    fn from(val: Option<i64>) -> Self {
        match val.unwrap_or(0) {
            1 => BlockProcessingStatusEnum::Proposed,
            2 => BlockProcessingStatusEnum::Finalized,
            3 => BlockProcessingStatusEnum::Refused,
            _ => BlockProcessingStatusEnum::Unknown,
        }
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
#[derive(Default)]
pub struct ExtBlkRef {
    #[graphql(skip)]
    end_lt: Option<String>,
    file_hash: Option<String>,
    root_hash: Option<String>,
    seq_no: Option<f64>,
}

#[ComplexObject]
impl ExtBlkRef {
    #[graphql(name = "end_lt")]
    async fn end_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.end_lt.clone(), format)
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
struct BlockMasterShardHashesDescr {
    gen_utime: Option<f64>,
    root_hash: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
struct BlockMasterShardHashes {
    workchain_id: Option<Int>,
    shard: Option<String>,
    descr: Option<BlockMasterShardHashesDescr>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
struct BlockMaster {
    shard_hashes: Option<BlockMasterShardHashes>,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct Directives {
    share_state_resource_address: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct BlockValueFlow {
    #[graphql(skip)]
    created: Option<String>,
    created_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    exported: Option<String>,
    exported_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    fees_collected: Option<String>,
    fees_collected_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    fees_imported: Option<String>,
    fees_imported_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    from_prev_blk: Option<String>,
    from_prev_blk_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    imported: Option<String>,
    imported_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    minted: Option<String>,
    minted_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    to_next_blk: Option<String>,
    to_next_blk_other: Option<Vec<OtherCurrency>>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct BlockAccountBlocksTransactions {
    #[graphql(skip)]
    lt: Option<String>,
    #[graphql(skip)]
    total_fees: Option<String>,
    total_fees_other: Option<Vec<OtherCurrency>>,
    transaction_id: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct BlockAccountBlocks {
    account_addr: Option<String>,
    new_hash: Option<String>,
    old_hash: Option<String>,
    tr_count: Option<i32>,
    transactions: Option<Vec<BlockAccountBlocksTransactions>>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct Block {
    pub id: String,
    account_blocks: Option<Vec<BlockAccountBlocks>>,
    after_merge: Option<Boolean>,
    after_split: Option<Boolean>,
    aggregated_signature: Vec<u8>,
    before_split: Option<Boolean>,
    /// Serialized bag of cells of this block encoded with base64.
    boc: Option<String>,
    /// Collection-unique field for pagination and sorting. This field is
    /// designed to retain logical order.
    pub chain_order: Option<String>,
    /// Public key of the collator who produced this block.
    created_by: Option<String>,
    directives: Directives,
    #[graphql(skip)]
    end_lt: Option<String>,
    /// Shard block file hash.
    file_hash: String,
    flags: Option<Int>,
    gen_catchain_seqno: Option<Float>,
    #[graphql(skip)]
    gen_software_capabilities: Option<String>,
    gen_software_version: Option<Float>,
    /// uint 32 generation time stamp.
    gen_utime: Option<Int>,
    gen_utime_string: String,
    gen_validator_list_hash_short: Option<Float>,
    /// uint32 global block ID.
    global_id: Option<Int>,
    pub hash: Option<String>,
    in_msg_descr: Option<Vec<Option<InMsg>>>,
    /// true if this block is a key block.
    key_block: Option<Boolean>,
    master: Option<BlockMaster>,
    master_ref: Option<ExtBlkRef>,
    /// seq_no of masterchain block which commited the block.
    master_seq_no: Option<u64>,
    /// Returns last known master block at the time of shard generation.
    min_ref_mc_seqno: Option<Float>,
    pub out_msg_descr: Option<Vec<Option<OutMsg>>>,
    #[graphql(skip)]
    pub out_msgs: Option<String>,
    // #[graphql(skip)]
    prev_alt_ref: Option<ExtBlkRef>,
    /// Returns a number of a previous key block.
    prev_key_block_seqno: Option<Float>,
    // #[graphql(skip)]
    prev_ref: Option<ExtBlkRef>,
    prev_vert_alt_ref: Option<ExtBlkRef>,
    /// External block reference for previous block in case of vertical blocks.
    prev_vert_ref: Option<ExtBlkRef>,
    rand_seed: String,
    seq_no: i64,
    shard: Option<String>,
    //-- signatures(timeout: Int, "**DEPRECATED**" when: BlockFilter): BlockSignatures,
    signature_occurrences: Vec<u8>,
    #[graphql(skip)]
    start_lt: Option<String>,
    //-- state_update: BlockStateUpdate,
    /// Returns block processing status:
    /// - 0 – unknown
    /// - 1 – proposed
    /// - 2 – finalized
    /// - 3 – refused
    status: u8,
    /// Returns block processing status name.
    status_name: BlockProcessingStatusEnum,
    thread_id: Option<String>,
    tr_count: Option<i32>,
    value_flow: Option<BlockValueFlow>,
    /// uin32 block version identifier.
    version: Option<Float>,
    vert_seq_no: Option<Float>,
    want_merge: Option<Boolean>,
    want_split: Option<Boolean>,
    /// int64 workchain identifier.
    workchain_id: Option<i64>,
}

impl From<db::Block> for Block {
    fn from(block: db::Block) -> Self {
        let boc = block.boc.map(tvm_types::base64_encode);
        let prev_alt_ref = if block.prev_alt_ref_root_hash.is_some() {
            Some(ExtBlkRef {
                end_lt: block.prev_alt_ref_end_lt,
                file_hash: block.prev_alt_ref_file_hash,
                root_hash: block.prev_alt_ref_root_hash,
                seq_no: block.prev_alt_ref_seq_no.to_float(),
            })
        } else {
            None
        };
        Self {
            id: block.id.clone(),
            account_blocks: None,
            after_merge: block.after_merge.to_bool(),
            after_split: block.after_split.to_bool(),
            aggregated_signature: block.aggregated_signature.unwrap_or_default(),
            before_split: block.before_split.to_bool(),
            boc,
            chain_order: block.chain_order,
            created_by: None,
            directives: Directives {
                share_state_resource_address: block.share_state_resource_address,
            },
            end_lt: None,
            file_hash: "".to_string(),
            flags: block.flags.to_int(),
            gen_catchain_seqno: block.gen_catchain_seqno.to_float(),
            gen_software_capabilities: block.gen_software_capabilities,
            gen_software_version: block.gen_software_version.to_float(),
            gen_utime: block.gen_utime.to_int(),
            gen_utime_string: {
                let dt: DateTime<Utc> =
                    DateTime::from_timestamp(block.gen_utime.unwrap(), 0).unwrap();
                dt.format("%Y-%m-%d %H:%M:%S.%z").to_string()
            },
            gen_validator_list_hash_short: block.gen_validator_list_hash_short.to_float(),
            global_id: block.global_id.to_int(),
            hash: Some(block.id),
            in_msg_descr: Some(vec![]),
            key_block: block.key_block.to_bool(),
            master: None,
            master_ref: None,
            master_seq_no: None,
            min_ref_mc_seqno: block.min_ref_mc_seqno.to_float(),
            out_msg_descr: None,
            out_msgs: block.out_msgs,
            prev_alt_ref,
            prev_key_block_seqno: block.prev_key_block_seqno.to_float(),
            prev_ref: Some(ExtBlkRef {
                end_lt: block.prev_ref_end_lt,
                file_hash: block.prev_ref_file_hash,
                root_hash: block.prev_ref_root_hash,
                seq_no: Some(block.prev_ref_seq_no.unwrap() as f64),
            }),
            prev_vert_ref: None,
            prev_vert_alt_ref: None,
            rand_seed: "".to_string(),
            seq_no: block.seq_no,
            shard: block.shard,
            signature_occurrences: block.signature_occurrences.unwrap_or_default(),
            start_lt: Some(block.start_lt.unwrap_or("".to_string())),
            status: block.status.unwrap_or(0) as u8,
            status_name: block.status.into(),
            thread_id: block.thread_id,
            tr_count: block.tr_count.to_int(),
            value_flow: None,
            version: block.version.to_float(),
            vert_seq_no: None,
            want_merge: block.want_merge.to_bool(),
            want_split: block.want_split.to_bool(),
            workchain_id: block.workchain_id,
        }
    }
}

#[ComplexObject]
impl Block {
    #[graphql(name = "end_lt")]
    /// Logical creation time automatically set by the block formation end.
    async fn end_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.end_lt.clone(), format)
    }

    #[graphql(name = "gen_software_capabilities")]
    async fn gen_software_capabilities(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.gen_software_capabilities.clone(), format)
    }

    // #[graphql(name = "prev_ref")]
    // /// External block reference for previous block.
    // async fn prev_ref(&self) -> Option<ExtBlkRef> {
    //     // let prev_block = db::Block::by_seq_no(pool, seq_no)
    //     // ExtBlkRef {

    //     // }
    //     Default::default()
    // }

    // #[graphql(name = "prev_alt_ref")]
    // /// External block reference for previous block in case of shard merge.
    // async fn prev_alt_ref(&self) -> Option<ExtBlkRef> {
    //     // let prev_block = db::Block::by_seq_no(pool, seq_no)
    //     // ExtBlkRef {

    //     // }
    //     Default::default()
    // }

    #[graphql(name = "start_lt")]
    /// Logical creation time automatically set by the block formation start.
    /// Logical time is a component of the Acki Nacki Blockchain that also plays
    /// an important role in message delivery is the logical time, usually
    /// denoted by Lt. It is a non-negative 64-bit integer, assigned to
    /// certain events. For more details, see the Acki Nacki blockchain
    /// specification.
    async fn start_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.start_lt.clone(), format)
    }
}

#[ComplexObject]
impl BlockValueFlow {
    async fn created(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.created.clone(), format)
    }

    async fn exported(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.exported.clone(), format)
    }

    #[graphql(name = "fees_collected")]
    async fn fees_collected(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.fees_collected.clone(), format)
    }

    #[graphql(name = "fees_imported")]
    async fn fees_imported(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.fees_imported.clone(), format)
    }

    #[graphql(name = "from_prev_blk")]
    async fn from_prev_blk(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.from_prev_blk.clone(), format)
    }

    async fn imported(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.imported.clone(), format)
    }

    async fn minted(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.minted.clone(), format)
    }

    #[graphql(name = "to_next_blk")]
    async fn to_next_blk(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.to_next_blk.clone(), format)
    }
}

#[ComplexObject]
impl BlockAccountBlocksTransactions {
    async fn lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.lt.clone(), format)
    }

    #[graphql(name = "total_fees")]
    async fn total_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.total_fees.clone(), format)
    }
}

impl Block {
    pub fn set_in_msg_descr(&mut self, in_msgs: Vec<InBlockMessage>) {
        self.in_msg_descr = Some(in_msgs.into_iter().map(|v| Some(v.into())).collect())
    }

    // pub fn set_out_msg_descr(&mut self, out_msgs: Vec<Message>) {
    //     self.out_msg_descr = Some(out_msgs.into_iter().map(|v|
    // Some(v.into())).collect()) }
}
