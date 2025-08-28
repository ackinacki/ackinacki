// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub fn read_file(file_path: &str) -> anyhow::Result<Vec<u8>> {
    let file = std::fs::File::open(file_path);
    if let Err(e) = file {
        anyhow::bail!("Failed to read {file_path}: {e}");
    }
    let mut buffer = Vec::new();
    std::io::Read::read_to_end(&mut file?, &mut buffer)?;
    Ok(buffer)
}
