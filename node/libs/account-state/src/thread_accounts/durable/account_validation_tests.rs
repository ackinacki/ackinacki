use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use node_types::AccountIdentifier;
use node_types::DAppIdentifier;

use super::*;

fn raw(index: usize) -> RawAccount {
    (
        AccountRouting::new(DAppIdentifier::new([0; 32]), AccountIdentifier::new([0; 32])),
        index.to_le_bytes().to_vec(),
    )
}

fn index(entry: RawAccount) -> anyhow::Result<usize> {
    Ok(usize::from_le_bytes(entry.1.try_into().unwrap()))
}

#[test]
fn ordered_validation_keeps_wire_order_across_batches() -> anyhow::Result<()> {
    let count = VALIDATION_BATCH_RECORDS * 5 + 1;
    let mut entries = (0..count).map(|i| Ok(raw(i)));
    let output = with_validated_accounts(
        &mut entries,
        true,
        |entry| {
            let i = index(entry)?;
            if i == 0 {
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(i)
        },
        |validated| validated.collect::<anyhow::Result<Vec<_>>>(),
    )?;
    assert_eq!(output, (0..count).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn legacy_validation_can_deliver_a_later_batch_first() -> anyhow::Result<()> {
    if super::super::repository::snapshot_written_hash_worker_count() < 2 {
        return Ok(());
    }
    let count = VALIDATION_BATCH_RECORDS * 3;
    let mut entries = (0..count).map(|i| Ok(raw(i)));
    let (release, gate) = crossbeam_channel::bounded(1);
    let mut output = with_validated_accounts(
        &mut entries,
        false,
        move |entry| {
            let i = index(entry)?;
            if i == 0 {
                gate.recv_timeout(Duration::from_secs(5))?;
            }
            Ok(i)
        },
        |validated| {
            let first = validated.next().unwrap()?;
            assert_ne!(first, 0, "the serial sink must not wait for the slow first batch");
            release.send(())?;
            let mut values = vec![first];
            for value in validated {
                values.push(value?);
            }
            Ok(values)
        },
    )?;
    output.sort_unstable();
    assert_eq!(output, (0..count).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn validation_bounds_read_ahead_and_stops_on_sink_error() {
    let window = super::super::repository::snapshot_written_hash_worker_count()
        + super::super::repository::snapshot_written_hash_queue_batches();
    let read = AtomicUsize::new(0);
    let mut entries = (0..VALIDATION_BATCH_RECORDS * (window + 5)).map(|i| {
        read.fetch_add(1, Ordering::Relaxed);
        Ok(raw(i))
    });
    let result =
        with_validated_accounts(&mut entries, true, index, |validated| -> anyhow::Result<()> {
            assert_eq!(validated.next().unwrap()?, 0);
            anyhow::bail!("cancelled sink")
        });
    assert!(result.unwrap_err().to_string().contains("cancelled sink"));
    assert_eq!(read.load(Ordering::Relaxed), window * VALIDATION_BATCH_RECORDS);
}

#[test]
fn validation_rejects_an_incompletely_consumed_sink() {
    let mut entries = (0..VALIDATION_BATCH_RECORDS * 2).map(|i| Ok(raw(i)));
    let result = with_validated_accounts(&mut entries, true, index, |validated| {
        validated.next().unwrap()?;
        Ok(())
    });
    assert!(result.unwrap_err().to_string().contains("did not consume all bodies"));
}

#[test]
fn validation_propagates_input_and_worker_errors() {
    for ordered in [false, true] {
        let mut input = vec![Err(anyhow::anyhow!("broken framing"))].into_iter();
        let result = with_validated_accounts(&mut input, ordered, index, |validated| {
            validated.collect::<anyhow::Result<Vec<_>>>()
        });
        assert!(result.unwrap_err().to_string().contains("broken framing"));
        let mut input = (0..VALIDATION_BATCH_RECORDS * 2).map(|i| Ok(raw(i)));
        let result = with_validated_accounts(
            &mut input,
            ordered,
            move |entry| {
                let i = index(entry)?;
                if i == VALIDATION_BATCH_RECORDS {
                    anyhow::bail!("invalid body");
                }
                Ok(i)
            },
            |validated| validated.collect::<anyhow::Result<Vec<_>>>(),
        );
        let message = result.unwrap_err().to_string();
        assert!(message.contains("invalid body"));
    }
}
