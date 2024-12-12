// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub fn u64_to_string(value: u64) -> String {
    let mut string = format!("{:x}", value);
    string.insert_str(0, &format!("{:x}", string.len() - 1));
    string
}
