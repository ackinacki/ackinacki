use crate::config::UPDATE_MIRRORS_BLOCK_SEQ_NO;

pub fn get_engine_version(block_seq_no: u32) -> semver::Version {
    if block_seq_no <= UPDATE_MIRRORS_BLOCK_SEQ_NO {
        "1.0.2".parse().unwrap()
    } else {
        "1.0.3".parse().unwrap()
    }
}
