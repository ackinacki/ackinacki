//! Test-only wire-manifest inspector, independent of the node/account model.
//! Outputs content addresses, not a proof of primary metadata or partition data.
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

fn inspect(reader: &mut impl Read) -> anyhow::Result<serde_json::Value> {
    let mut magic = [0; 7];
    reader.read_exact(&mut magic)?;
    anyhow::ensure!(&magic == b"SNAP2V2", "expected SNAP2V2 primary");
    let mut lists = Vec::new();
    for kind in 0..3 {
        let mut length = [0; 8];
        reader.read_exact(&mut length)?;
        let length = u64::from_le_bytes(length);
        anyhow::ensure!(length <= 65_536, "manifest root limit exceeded");
        let mut ids = Vec::new();
        for _ in 0..length {
            let mut root = [0; 32];
            reader.read_exact(&mut root)?;
            let mut hash = blake3::Hasher::new();
            if kind == 0 {
                hash.update(b"ACCTPV1");
            } else {
                hash.update(b"MSGSPV1");
                hash.update(&[kind - 1]);
            }
            hash.update(&root);
            ids.push(hash.finalize().to_hex().to_string());
        }
        lists.push(ids);
    }
    Ok(serde_json::json!({"accounts": lists[0], "normal": lists[1], "high": lists[2]}))
}

fn main() -> anyhow::Result<()> {
    let path =
        std::env::args_os().nth(1).ok_or_else(|| anyhow::anyhow!("expected primary path"))?;
    let mut file = std::fs::File::open(path)?;
    let mut envelope = [0; 4];
    file.read_exact(&mut envelope)?;
    let result = if &envelope == b"ZSTD" {
        let mut decoder = zstd::Decoder::new(file)?;
        decoder.window_log_max(21)?;
        inspect(&mut decoder)?
    } else {
        file.seek(SeekFrom::Start(0))?;
        inspect(&mut file)?
    };
    serde_json::to_writer(std::io::stdout(), &result)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_bind_kind_and_root() -> anyhow::Result<()> {
        let mut bytes = b"SNAP2V2".to_vec();
        for _ in 0..3 {
            bytes.extend(1u64.to_le_bytes());
            bytes.extend([42; 32]);
        }
        let lists = inspect(&mut bytes.as_slice())?;
        assert_ne!(lists["accounts"], lists["normal"]);
        assert_ne!(lists["normal"], lists["high"]);
        let mut expected = b"ACCTPV1".to_vec();
        expected.extend([42; 32]);
        assert_eq!(lists["accounts"][0], blake3::hash(&expected).to_hex().to_string());
        for end in 0..bytes.len() {
            assert!(inspect(&mut &bytes[..end]).is_err());
        }
        let mut oversized = b"SNAP2V2".to_vec();
        oversized.extend(65_537u64.to_le_bytes());
        assert!(inspect(&mut oversized.as_slice()).is_err());
        Ok(())
    }
}
