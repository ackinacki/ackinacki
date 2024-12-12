// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use rusqlite::Row;
use serde::Deserialize;
use serde::Serialize;
use serde_with::with_prefix;
use tvm_types::UInt256;

use crate::helpers::u64_to_string;
use crate::serialization::BlockSerializationSetFH;

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq)]
pub struct ExtBlkRef {
    pub seq_no: Option<i64>,
    pub end_lt: Option<String>,
    pub file_hash: Option<String>,
    pub root_hash: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct BlockValueFlow {
    created: Option<String>,
    exported: Option<String>,
    fees_collected: Option<String>,
    fees_imported: Option<String>,
    from_prev_blk: Option<String>,
    imported: Option<String>,
    minted: Option<String>,
    recovered: Option<String>,
    to_next_blk: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ArchBlock {
    pub id: String,
    pub status: u8,                                   // status INTEGER NOT NULL,
    pub seq_no: u32,                                  // seq_no INTEGER NOT NULL,
    pub parent: String,                               // parent TEXT NOT NULL,
    pub aggregated_signature: Option<Vec<u8>>,        // aggregated_signature BLOB,
    pub signature_occurrences: Option<Vec<u8>>,       // signature_occurrences BLOB,
    pub share_state_resource_address: Option<String>, // share_state_resource_address TEXT,
    pub global_id: Option<i64>,                       // global_id INTEGER,
    pub version: Option<i64>,                         // version INTEGER,
    pub after_merge: Option<bool>,                    // after_merge INTEGER,
    pub before_split: Option<bool>,                   // before_split INTEGER,
    pub after_split: Option<bool>,                    // after_split INTEGER,
    pub want_merge: Option<bool>,                     // want_merge INTEGER,
    pub want_split: Option<bool>,                     // want_split INTEGER,
    pub key_block: Option<bool>,                      // key_block INTEGER,
    pub flags: Option<i64>,                           // flags INTEGER,
    pub shard: Option<String>,                        // shard TEXT,
    pub workchain_id: Option<i64>,                    // workchain_id INTEGER,
    pub gen_utime: Option<i64>,                       // gen_utime INTEGER,
    pub gen_utime_ms_part: Option<i64>,               // gen_utime_ms_part INTEGER,
    pub start_lt: Option<String>,                     // start_lt TEXT,
    pub end_lt: Option<String>,                       // end_lt TEXT,
    pub gen_validator_list_hash_short: Option<i64>,   // gen_validator_list_hash_short INTEGER,
    pub gen_catchain_seqno: Option<i64>,              // gen_catchain_seqno INTEGER,
    pub min_ref_mc_seqno: Option<i64>,                // min_ref_mc_seqno INTEGER,
    pub prev_key_block_seqno: Option<i64>,            // prev_key_block_seqno INTEGER,
    pub gen_software_version: Option<i64>,            // gen_software_version INTEGER,
    pub gen_software_capabilities: Option<String>,    // gen_software_capabilities TEXT,
    pub boc: Option<Vec<u8>>,
    pub file_hash: Option<String>,
    pub root_hash: Option<String>,
    // #[serde(flatten, with = "prefix_prev_ref")]
    pub prev_ref: Option<ExtBlkRef>,
    // #[serde(flatten, with = "prefix_prev_alt_ref")]
    pub prev_alt_ref: Option<ExtBlkRef>,
    pub in_msgs: Option<String>,
    pub out_msgs: Option<String>,
    pub data: Option<Vec<u8>>,
    pub chain_order: Option<String>,
    pub tr_count: Option<i64>,
    pub value_flow: Option<BlockValueFlow>,
    pub thread_id: Option<String>,
}

with_prefix!(prefix_prev_ref "prev_ref_");
with_prefix!(prefix_prev_alt_ref "prev_alt_ref_");

impl From<&Row<'_>> for ArchBlock {
    fn from(val: &Row<'_>) -> Self {
        ArchBlock {
            id: val.get(1).unwrap(),
            status: val.get(2).unwrap(),
            seq_no: val.get(3).unwrap(),
            parent: val.get(4).unwrap(),
            aggregated_signature: val.get(5).ok(),
            signature_occurrences: val.get(6).ok(),
            share_state_resource_address: val.get(7).ok(),
            global_id: val.get(8).ok(),
            version: val.get(9).ok(),
            after_merge: val.get(10).ok(),
            before_split: val.get(11).ok(),
            after_split: val.get(12).ok(),
            want_split: val.get(13).ok(),
            want_merge: val.get(14).ok(),
            key_block: val.get(15).ok(),
            flags: val.get(16).ok(),
            workchain_id: val.get(17).ok(),
            gen_utime: val.get(18).ok(),
            gen_utime_ms_part: val.get(19).ok(),
            start_lt: val.get(20).ok(),
            end_lt: val.get(21).ok(),
            gen_validator_list_hash_short: val.get(22).ok(),
            gen_catchain_seqno: val.get(23).ok(),
            min_ref_mc_seqno: val.get(24).ok(),
            prev_key_block_seqno: val.get(25).ok(),
            gen_software_version: val.get(26).ok(),
            gen_software_capabilities: val.get(27).ok(),
            data: val.get(28).ok(),
            file_hash: val.get(29).ok(),
            root_hash: val.get(30).ok(),
            prev_alt_ref: Some(ExtBlkRef {
                seq_no: val.get(31).ok(),
                end_lt: val.get(32).ok(),
                file_hash: val.get(33).ok(),
                root_hash: val.get(34).ok(),
            }),
            prev_ref: Some(ExtBlkRef {
                seq_no: val.get(35).unwrap(),
                end_lt: val.get(36).ok(),
                file_hash: val.get(37).ok(),
                root_hash: val.get(38).ok(),
            }),
            in_msgs: val.get(39).ok(),
            out_msgs: val.get(40).ok(),
            shard: val.get(41).ok(),
            chain_order: val.get(42).ok(),
            tr_count: val.get(43).ok(),
            boc: val.get(44).ok(),
            thread_id: val.get(45).ok(),
            value_flow: None,
        }
    }
}

impl From<BlockSerializationSetFH> for ArchBlock {
    fn from(blk: BlockSerializationSetFH) -> Self {
        let file_hash = match blk.file_hash {
            Some(file_hash) => file_hash,
            None => UInt256::calc_file_hash(&blk.boc),
        };

        let block_info = blk.block.read_info().expect("Failed to read info from block");
        let prev_block_ref = block_info.read_prev_ref().expect("Failed to read prev ref");

        let mut arch_block = Self {
            id: blk.id.to_hex_string(),
            after_merge: block_info.after_merge().into(),
            after_split: block_info.after_split().into(),
            before_split: block_info.before_split().into(),
            boc: Some(blk.boc),
            end_lt: Some(block_info.end_lt().to_string()),
            file_hash: Some(file_hash.to_hex_string()),
            gen_catchain_seqno: Some(block_info.gen_catchain_seqno().into()),
            gen_utime: Some(block_info.gen_utime().as_u32().into()),
            gen_validator_list_hash_short: Some(block_info.gen_validator_list_hash_short().into()),
            global_id: Some(blk.block.global_id as i64),
            key_block: block_info.key_block().into(),
            min_ref_mc_seqno: Some(block_info.min_ref_mc_seqno().into()),
            prev_alt_ref: prev_block_ref.prev2().unwrap().map(serialize_block_ref),
            prev_key_block_seqno: Some(block_info.prev_key_block_seqno().into()),
            prev_ref: prev_block_ref.prev1().ok().map(serialize_block_ref),
            seq_no: block_info.seq_no(),
            shard: block_info.shard().shard_prefix_as_str_with_tag().into(),
            start_lt: Some(block_info.start_lt().to_string()),
            status: blk.status as u8,
            value_flow: blk.block.read_value_flow().ok().map(serialize_value_flow),
            version: Some(block_info.version() as i64),
            want_merge: block_info.want_merge().into(),
            want_split: block_info.want_split().into(),
            workchain_id: Some(block_info.shard().workchain_id().into()),
            ..Default::default()
        };

        if let Some(gs) = block_info.gen_software() {
            arch_block.gen_software_version = Some(gs.version.into());
            arch_block.gen_software_capabilities = Some(u64_to_string(gs.capabilities));
        }

        // let state_update = set.block.read_state_update()?;
        // serialize_id(&mut map, "old_hash", Some(&state_update.old_hash));
        // serialize_id(&mut map, "new_hash", Some(&state_update.new_hash));
        // map.insert("old_depth".to_string(), state_update.old_depth.into());
        // map.insert("new_depth".to_string(), state_update.new_depth.into());

        // let extra = blk.block.read_extra().expect("Failed to read block extra");
        // let mut msgs = vec![];
        // extra.read_in_msg_descr()?.iterate_objects(|ref msg| {
        //     msgs.push(serialize_in_msg(msg, mode)?);
        //     Ok(true)
        // })?;
        // map.insert("in_msg_descr".to_string(), msgs.into());

        // let mut msgs = vec![];
        // extra.read_out_msg_descr()?.iterate_objects(|ref msg| {
        //     msgs.push(serialize_out_msg(msg, mode)?);
        //     Ok(true)
        // })?;
        // map.insert("out_msg_descr".to_string(), msgs.into());

        // let mut total_tr_count = 0;
        // let mut account_blocks = Vec::new();
        // extra.read_account_blocks()?.iterate_objects(|account_block| {
        //     let workchain = block_info.shard().workchain_id();
        //     let address = construct_address(workchain, account_block.account_addr())?;
        //     let mut map = Map::new();
        //     serialize_field(&mut map, "account_addr", address.to_string());
        //     let mut transactions = Vec::new();
        //     account_block.transaction_iterate_full(|key, transaction_cell, cc| {
        //         let mut map = Map::new();
        //         serialize_lt(&mut map, "lt", &key, mode);
        //         serialize_id(&mut map, "transaction_id", Some(&transaction_cell.repr_hash()));
        //         serialize_cc(&mut map, "total_fees", &cc, mode)?;
        //         transactions.push(map);
        //         Ok(true)
        //     })?;
        //     serialize_field(&mut map, "transactions", transactions);
        //     let state_update = account_block.read_state_update()?;
        //     serialize_id(&mut map, "old_hash", Some(&state_update.old_hash));
        //     serialize_id(&mut map, "new_hash", Some(&state_update.new_hash));
        //     let tr_count = account_block.transaction_count()?;
        //     serialize_field(&mut map, "tr_count", tr_count);
        //     account_blocks.push(map);
        //    total_tr_count += tr_count;
        //     Ok(true)
        // }).expect("Failed to iterate over block transactions");
        // if !account_blocks.is_empty() {
        //     serialize_field(&mut map, "account_blocks", account_blocks);
        // }

        // serialize_field(&mut map, "tr_count", total_tr_count);
        arch_block
    }
}

fn serialize_block_ref(blk_ref: tvm_block::blocks::ExtBlkRef) -> ExtBlkRef {
    ExtBlkRef {
        end_lt: Some(u64_to_string(blk_ref.end_lt)),
        seq_no: Some(blk_ref.seq_no.into()),
        root_hash: Some(blk_ref.root_hash.to_hex_string()),
        file_hash: Some(blk_ref.file_hash.to_hex_string()),
    }
}

fn serialize_value_flow(value_flow: tvm_block::ValueFlow) -> BlockValueFlow {
    BlockValueFlow {
        created: Some(format!("{:x}", value_flow.created.grams.as_u128())),
        exported: Some(format!("{:x}", value_flow.exported.grams.as_u128())),
        fees_collected: Some(format!("{:x}", value_flow.fees_collected.grams.as_u128())),
        fees_imported: Some(format!("{:x}", value_flow.fees_imported.grams.as_u128())),
        from_prev_blk: Some(format!("{:x}", value_flow.from_prev_blk.grams.as_u128())),
        imported: Some(format!("{:x}", value_flow.imported.grams.as_u128())),
        minted: Some(format!("{:x}", value_flow.minted.grams.as_u128())),
        recovered: Some(format!("{:x}", value_flow.recovered.grams.as_u128())),
        to_next_blk: Some(format!("{:x}", value_flow.to_next_blk.grams.as_u128())),
    }
}

#[cfg(test)]
pub mod tests {
    use super::ArchBlock;

    #[test]
    fn test_block_deser() -> anyhow::Result<()> {
        let item = serde_json::json!({
            "id": "725523b902cc8c319cb3000ee6f54049c455e89bc8b8f251224cf9f8389ac48e",
            "status": 99,
            "seq_no": 1,
            "parent": "0000000000000000000000000000000000000000000000000000000000000000",
            "prev_ref": {
                "end_lt_dec": "4",
                "end_lt": "04",
                "seq_no": 2,
                "root_hash": "e833f25e76683f2cedee08d55b266a2b46018939fd6326b058b780527755f89b",
                "file_hash": "47a5b87b2c0851926703f5d6bbdc57d6c293c62aec6051086fa5382298858ab5"
            }
        });

        let block = serde_json::from_value::<ArchBlock>(item)?;

        assert_ne!(block.prev_ref, None);
        Ok(())
    }
}
