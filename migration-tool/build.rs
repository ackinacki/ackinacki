// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use cargo_emit::rerun_if_env_changed;
use cargo_emit::rustc_env;
use merkle_hash::bytes_to_hex;
use merkle_hash::Algorithm;
use merkle_hash::MerkleTree;

fn main() {
    let tree = MerkleTree::builder("migrations")
        .algorithm(Algorithm::Blake3)
        .hash_names(true)
        .build()
        .expect("Could not calculate a hash of the migrations dir");
    let migrations_dir_hash = bytes_to_hex(&tree.root.item.hash);
    rustc_env!("MIGRATIONS_DIR_HASH", "{}", migrations_dir_hash);
    rerun_if_env_changed!("MIGRATIONS_DIR_HASH");
}
