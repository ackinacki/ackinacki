use std::fs::File;
use std::io::Read;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::complete_import;
use crate::repository::repository_impl::snapshot_consumption::stage_partitioned_snapshot;
use crate::repository::repository_impl::snapshot_consumption::tests::limits;
use crate::repository::repository_impl::snapshot_consumption::tests::Target;
use crate::repository::repository_impl::snapshot_consumption::StagedPartitionedSnapshot;
use crate::repository::repository_impl::snapshot_publication::publish_partitioned_snapshot;
use crate::repository::repository_impl::snapshot_publication::tests::Fixture;

fn stage<'a>(
    source: &Fixture,
    target: &'a Target,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>> {
    let directory = tempfile::tempdir()?;
    let manifest = publish_partitioned_snapshot(
        source.publication(source.pin()?, directory.path()),
        &|| false,
        |_| Ok(()),
    )?;
    let mut primary =
        File::open(directory.path().join(hex::encode(source.state.block_id.as_array())))?;
    stage_partitioned_snapshot(
        &mut primary,
        &target.repository,
        (source.state.thread_id, source.state.block_id),
        limits(),
        |id| {
            Ok(Box::new(File::open(
                directory.path().join(".snapshot-resources/blobs").join(hex::encode(id)),
            )?) as Box<dyn Read>)
        },
        |primary| {
            anyhow::ensure!(primary.manifest == manifest, "unexpected fixture manifest");
            Ok(())
        },
        &|| false,
    )
}

#[test]
fn snapshot_import_handoff_observes_cancel_before_next_monitor_poll() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let target = Target::new()?;
    let staged = stage(&source, &target)?;
    assert!(target.count("A") > 0 && target.count("B") > 0);
    target.assert_active_unchanged();

    // Deterministically model the missed-poll window: staging is successful,
    // the caller has cancelled, but the actors still see watch=false. No sleep
    // or scheduler assumptions are needed to exercise the final handoff check.
    let cancelled = AtomicBool::new(false);
    let (_sender, actor_cancellation) = tokio::sync::watch::channel(false);
    cancelled.store(true, Ordering::Release);
    assert!(!*actor_cancellation.borrow());
    let result = complete_import(Ok(staged), &|| cancelled.load(Ordering::Acquire));
    assert!(result
        .err()
        .expect("cancelled handoff must fail")
        .to_string()
        .contains("snapshot import cancelled"));
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_import_handoff_keeps_uncancelled_staging_private() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let target = Target::new()?;
    let staged = complete_import(Ok(stage(&source, &target)?), &|| false)?;
    assert!(target.count("A") > 0 && target.count("B") > 0);
    target.assert_active_unchanged();
    drop(staged);
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_import_handoff_preserves_original_failure() {
    let result = complete_import(Err(anyhow::anyhow!("original import failure")), &|| {
        panic!("failed imports must keep their original error")
    });
    assert_eq!(result.err().unwrap().to_string(), "original import failure");
}
