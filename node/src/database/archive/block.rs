// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use rusqlite::Row;
use serde::Deserialize;
use serde::Serialize;
use serde_with::with_prefix;

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq)]
pub struct ExtBlkRef {
    pub seq_no: Option<i64>,
    pub end_lt: Option<String>,
    pub file_hash: Option<String>,
    pub root_hash: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
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
    pub boc: Option<String>,
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
            want_merge: val.get(13).ok(),
            want_split: val.get(14).ok(),
            key_block: val.get(15).ok(),
            flags: val.get(16).ok(),
            shard: val.get(17).ok(),
            workchain_id: val.get(18).ok(),
            gen_utime: val.get(19).ok(),
            gen_utime_ms_part: val.get(20).ok(),
            start_lt: val.get(21).ok(),
            end_lt: val.get(22).ok(),
            gen_validator_list_hash_short: val.get(23).ok(),
            gen_catchain_seqno: val.get(24).ok(),
            min_ref_mc_seqno: val.get(25).ok(),
            prev_key_block_seqno: val.get(26).ok(),
            gen_software_version: val.get(27).ok(),
            gen_software_capabilities: val.get(28).ok(),
            boc: val.get(29).ok(),
            file_hash: val.get(30).ok(),
            root_hash: val.get(31).ok(),
            prev_ref: Some(ExtBlkRef {
                seq_no: val.get(32).unwrap(),
                end_lt: val.get(33).ok(),
                file_hash: val.get(34).ok(),
                root_hash: val.get(35).ok(),
            }),
            prev_alt_ref: Some(ExtBlkRef {
                seq_no: val.get(36).ok(),
                end_lt: val.get(37).ok(),
                file_hash: val.get(38).ok(),
                root_hash: val.get(39).ok(),
            }),
            in_msgs: val.get(40).ok(),
            out_msgs: val.get(41).ok(),
            data: val.get(42).ok(),
            chain_order: val.get(43).ok(),
            tr_count: val.get(44).ok(),
        }
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
