// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::cmp::Ordering;

use sha2::Digest;
use sha2::Sha256;

pub(crate) type Sha256Hash = [u8; 32];

pub fn compare_hashes(lhs: &Sha256Hash, rhs: &Sha256Hash) -> Ordering {
    for i in 0..32 {
        match lhs[i].cmp(&rhs[i]) {
            Ordering::Less => {
                return Ordering::Less;
            }
            Ordering::Greater => {
                return Ordering::Greater;
            }
            _ => {}
        }
    }
    Ordering::Equal
}

pub(crate) fn debug_hash(hash: &Sha256Hash) -> String {
    let mut result = String::new();
    for byte in hash {
        result = format!("{}{:02X}", result, byte);
    }
    result
}

pub fn calculate_hash(data: &[u8]) -> anyhow::Result<Sha256Hash> {
    #[cfg(feature = "timing")]
    let start = std::time::Instant::now();
    let mut hasher = Sha256::new();
    hasher.update(data);
    let res = hasher.finalize().into();
    #[cfg(feature = "timing")]
    tracing::trace!("Calculating block hash time: {}", start.elapsed().as_millis());
    Ok(res)
}
