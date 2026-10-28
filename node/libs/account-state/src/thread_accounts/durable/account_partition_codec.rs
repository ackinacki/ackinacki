//! ACCTPV1 payloads: marker followed by canonical account records, without a map.
//! Compression is an independent outer envelope. Publication is not enabled here.
use std::io::Read;
use std::io::Write;

use multi_map::MapKeyPath;
use multi_map::MultiMapAug;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use node_types::ThreadAccountsHash;

use super::account_validation::validate_account;
use super::account_validation::with_validated_accounts;
use super::repository::write_snapshot_account_entry;
use super::repository::DurableThreadAccountsRepository;
#[cfg(test)]
use super::AccountCapacity;
#[cfg(test)]
use super::AccountInfo;
use super::PartitionReadLimits;
use super::ThreadAccountMap;
use crate::ThreadAccount;

pub const ACCOUNT_PARTITION_MAGIC: &[u8; 7] = b"ACCTPV1";

/// Write an uncompressed partition. Hold the whole-thread block pin until
/// this call completes. Errors leave partial output, which must be discarded.
pub fn write_account_partition_checked<W, C>(
    repository: &DurableThreadAccountsRepository,
    map: &ThreadAccountMap,
    writer: &mut W,
    limits: PartitionReadLimits,
    should_cancel: &C,
) -> anyhow::Result<()>
where
    W: Write,
    C: Fn() -> bool + Sync,
{
    anyhow::ensure!(!map.is_empty(), "empty account partition");
    // Thread scopes are L1 prefixes. L2 maps retain the complete account
    // key space; sparse partition contents do not change their root scope.
    for (_, dapp) in map.dapps.iter() {
        anyhow::ensure!(
            dapp.0.root_path == MapKeyPath::default(),
            "account partition requires full-scope L2 maps"
        );
    }
    anyhow::ensure!(!should_cancel(), "account partition write cancelled");
    writer.write_all(ACCOUNT_PARTITION_MAGIC)?;
    repository.read_partition_canonical_checked(map, limits, should_cancel, |routing, canonical| {
        write_snapshot_account_entry(writer, &routing, canonical)
    })
}

/// Decode one bounded uncompressed payload, rebuilding its sparse map and
/// checking the manifest root. `scope` is the thread's L1 scope from primary
/// state, not a second copy in each blob. The reader must end at the blob EOF.
///
/// Bodies are delivered incrementally to a STAGED sink. No body may become
/// visible as imported state until this function and the whole snapshot pass
/// verification. The callback owns the recanonicalized account so storage can move its bytes
/// without copying. On any error the caller must abort the staged import.
pub fn read_account_partition_checked<R, C, F>(
    reader: &mut R,
    scope: MapKeyPath,
    expected_root: ThreadAccountsHash,
    should_cancel: &C,
    mut on_account: F,
) -> anyhow::Result<ThreadAccountMap>
where
    R: Read,
    C: Fn() -> bool,
    F: FnMut(AccountRouting, ThreadAccount) -> anyhow::Result<()>,
{
    let started = std::time::Instant::now();
    anyhow::ensure!(scope.len <= 256, "invalid account partition scope");
    let mut magic = [0; 7];
    reader.read_exact(&mut magic)?;
    anyhow::ensure!(&magic == ACCOUNT_PARTITION_MAGIC, "invalid account partition marker");
    let mut map =
        ThreadAccountMap { dapps: MultiMapAug { root_path: scope, ..MultiMapAug::new() } };
    let mut previous = None;
    let mut count = 0u64;
    let mut canonical_payload_bytes = 0u64;
    // Framing, untrusted lengths and wire order are checked before submitting
    // owned bodies to the shared import pool. Decoded batches retain wire order
    // so the staged sink and partition boundary checks keep their contract.
    let mut done = false;
    let mut raw = std::iter::from_fn(|| {
        if done {
            return None;
        }
        let result = (|| -> anyhow::Result<Option<_>> {
            anyhow::ensure!(!should_cancel(), "account partition read cancelled");
            // Probe only the first byte: an EOF inside a record is truncation,
            // never a successful end of the stream.
            let mut dapp = [0; 32];
            match reader.read_exact(&mut dapp[..1]) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(error) => return Err(error.into()),
            }
            reader.read_exact(&mut dapp[1..])?;
            let mut account_id = [0; 32];
            reader.read_exact(&mut account_id)?;
            let key = (dapp, account_id);
            anyhow::ensure!(
                previous.is_none_or(|previous| previous < key),
                "account partition records must be strictly ordered and unique"
            );
            anyhow::ensure!(
                multi_map::ops::prefix_bits_match(&scope.prefix.0, &dapp, scope.len),
                "account partition key outside thread scope"
            );
            let mut length = [0; 8];
            reader.read_exact(&mut length)?;
            let length = u64::from_le_bytes(length);
            // Do not preallocate an untrusted declared length. Read at most that
            // length and check EOF before decoding or handing anything to storage.
            let mut bytes = Vec::new();
            (&mut *reader).take(length).read_to_end(&mut bytes)?;
            anyhow::ensure!(bytes.len() as u64 == length, "truncated account body");
            let routing =
                AccountRouting::new(DAppIdentifier::new(dapp), AccountIdentifier::new(account_id));
            previous = Some(key);
            count += 1;
            Ok(Some((routing, bytes)))
        })();
        match result {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => {
                done = true;
                None
            }
            Err(error) => {
                done = true;
                Some(Err(error))
            }
        }
    });
    with_validated_accounts(&mut raw, true, validate_account, |validated| {
        for entry in validated {
            anyhow::ensure!(!should_cancel(), "account partition read cancelled");
            let (routing, bytes, info, capacity) = entry?;
            let canonical_size = bytes.len() as u64;
            canonical_payload_bytes = canonical_payload_bytes
                .checked_add(canonical_size)
                .ok_or_else(|| anyhow::anyhow!("canonical partition capacity overflow"))?;
            map = map.update_single(&routing, Some((info, capacity)));
            // Already recanonicalized by the worker; do not decode or encode
            // again just to transfer ownership of the canonical allocation.
            on_account(routing, ThreadAccount::from_canonical_bytes(bytes))?;
        }
        Ok(())
    })?;
    anyhow::ensure!(count > 0, "empty account partition");
    anyhow::ensure!(map.hash() == expected_root, "account partition root mismatch");
    tracing::info!(
        accounts = count,
        canonical_bytes = canonical_payload_bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "Account snapshot partition imported"
    );
    Ok(map)
}

#[cfg(test)]
#[path = "account_partition_codec_tests.rs"]
mod tests;
