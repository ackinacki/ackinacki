use crate::config::REPAIR_BK_WALLETS_BLOCK_SEQ_NO;

pub fn get_engine_version(block_seq_no: u32) -> semver::Version {
    if block_seq_no <= REPAIR_BK_WALLETS_BLOCK_SEQ_NO {
        "1.0.0".parse().unwrap()
    } else {
        "1.0.1".parse().unwrap()
    }
}
