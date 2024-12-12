// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tvm_types::Cell;

// TODO: finalize light block structure with headers
#[allow(dead_code)]
struct LiteBlock {
    cell: Cell,
    headers: Vec<u8>,
    thread_id: u32,
    thread_count: u32,
    seq_no: u64, // ???
}
