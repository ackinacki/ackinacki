use super::*;
use crate::repository::repository_impl::snapshot_consumption::tests::limits;
use crate::repository::repository_impl::snapshot_consumption::tests::Target;
use crate::repository::repository_impl::snapshot_http_import::SnapshotFetchLimits;

fn actor(directory: &std::path::Path) -> anyhow::Result<ThreadSnapshotDownloader> {
    let target = Target::new()?;
    let (cancellation, _) = watch::channel(false);
    let mut actor = ThreadSnapshotDownloader::new(
        &SnapshotHttpImport {
            repository: &target.repository,
            cache_directory: directory,
            expected: (ThreadIdentifier::default(), BlockIdentifier::default()),
            primary_sources: &[],
            known_sources: &[],
            fallback_sources: &[],
            download_limits: BlobDownloadLimits {
                rounds: 2,
                connect_timeout: Duration::from_secs(1),
                request_timeout: Duration::from_secs(2),
                retry_delay: Duration::ZERO,
                deadline: Instant::now() + Duration::from_secs(10),
            },
            import_limits: limits(),
            fetch_limits: SnapshotFetchLimits::default(),
        },
        None,
        Vec::new(),
        cancellation,
    );
    actor.state.add([1; 32], Some(0), vec![Url::parse("http://source.test/share/one")?])?;
    actor.state.add([2; 32], Some(1), vec![Url::parse("http://source.test/share/two")?])?;
    Ok(actor)
}

fn active(
    actor: &mut ThreadSnapshotDownloader,
    slot: usize,
    id: [u8; 32],
    expected: Option<&str>,
) -> watch::Receiver<bool> {
    let source = actor.state.partitions[&id].sources[0].clone();
    let part = actor.state.partitions.get_mut(&id).unwrap();
    part.active_task = Some(slot as u64);
    part.attempts.insert(source.clone(), 1);
    let (cancel, cancellation) = watch::channel(false);
    actor.state.slots[slot] = Some(ActiveTask {
        task_id: slot as u64,
        blob_id: id,
        source: Some(source),
        previous_chunks: 0,
        had_map: false,
        expected_etag: expected.map(str::to_owned),
        cancel,
    });
    cancellation
}

fn response(error: TransferError) -> PartitionDownloadResponse {
    PartitionDownloadResponse {
        task_id: 0,
        state: None,
        result: Err(error),
        bytes_received: 128,
        elapsed: Duration::from_millis(1),
        source_etag: None,
    }
}

/// Exercise the actor's real command/event lifecycle, withholding importer
/// acknowledgements until after the transport deadline. Cached blobs still go
/// through complete chunk verification before their readiness events are sent.
async fn downloaded_files_survive_deadline(cancel_afterwards: bool) -> anyhow::Result<()> {
    use crate::repository::repository_impl::snapshot_publication::publish_partitioned_snapshot;
    use crate::repository::repository_impl::snapshot_publication::tests::Fixture;
    use crate::repository::repository_impl::snapshot_schedule::manifest_partition_count;

    let source = Fixture::new()?;
    let published = tempfile::tempdir()?;
    let manifest = publish_partitioned_snapshot(
        source.publication(source.pin()?, published.path()),
        &|| false,
        |_| Ok(()),
    )?;
    let primary_id = *source.state.block_id.as_array();
    let mut ids = vec![primary_id];
    ids.extend(
        (0..manifest_partition_count(&manifest))
            .map(|index| manifest.partition_blob_id(index).unwrap()),
    );
    let cache = tempfile::tempdir()?;
    for id in &ids {
        let name = hex::encode(id);
        let path = if *id == primary_id {
            published.path().join(&name)
        } else {
            published.path().join(".snapshot-resources/blobs").join(&name)
        };
        std::fs::copy(path, cache.path().join(name))?;
    }
    let mut actor = actor(cache.path())?;
    actor.state = SnapshotDownloadState::new(4);
    actor.primary_id = primary_id;
    actor.thread_id = source.state.thread_id;
    actor.limits.deadline = Instant::now() + Duration::from_secs(5);
    let deadline = actor.limits.deadline;
    let cancellation = actor.cancel_import.clone();
    let (commands, receiver) = mpsc::channel(1);
    let (events, mut readiness) = mpsc::unbounded_channel();
    let (reply, started) = oneshot::channel();
    commands
        .send(ThreadSnapshotDownloaderCommand::Start {
            thread_id: source.state.thread_id,
            block_id: source.state.block_id,
            known_sources: Vec::new(),
            fallback_sources: Vec::new(),
            events,
            reply,
        })
        .await?;
    let running = tokio::spawn(actor.run(receiver));
    started.await?;
    let mut paths = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in &ids {
            match readiness
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("readiness stream stopped"))?
            {
                ThreadSnapshotDownloaderEvent::PrimaryReady(primary) => {
                    assert_eq!(primary.manifest, manifest);
                }
                ThreadSnapshotDownloaderEvent::AccountsReady { path, .. }
                | ThreadSnapshotDownloaderEvent::NormalMessagesReady { path, .. }
                | ThreadSnapshotDownloaderEvent::HighPriorityMessagesReady { path, .. } => {
                    paths.push(path)
                }
            }
        }
        anyhow::Ok(())
    })
    .await??;
    tokio::time::sleep_until((deadline + Duration::from_millis(100)).into()).await;
    assert!(!*cancellation.borrow(), "a completed download must not cancel a slow importer");
    assert!(!running.is_finished(), "the actor must keep processing importer commands");
    assert!(paths.iter().all(|path| path.exists()), "ready files must remain leased");
    if cancel_afterwards {
        // Completion disables only the timer, not caller-driven cancellation.
        cancellation.send(true)?;
    } else {
        for blob_id in ids {
            let (reply, processed) = oneshot::channel();
            commands
                .send(ThreadSnapshotDownloaderCommand::BlobProcessed {
                    blob_id,
                    accepted: true,
                    reply,
                })
                .await?;
            tokio::time::timeout(Duration::from_secs(1), processed).await??.result?;
        }
        let (reply, stopped) = oneshot::channel();
        commands.send(ThreadSnapshotDownloaderCommand::Stop { reply }).await?;
        tokio::time::timeout(Duration::from_secs(1), stopped).await??;
    }
    tokio::time::timeout(Duration::from_secs(1), running).await???;
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_deadline_does_not_cancel_downloaded_files_waiting_for_import(
) -> anyhow::Result<()> {
    tokio::try_join!(
        downloaded_files_survive_deadline(false),
        downloaded_files_survive_deadline(true)
    )?;
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_unpublished_blob_preserves_error_budget_and_source_rating(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut actor = actor(directory.path())?;
    actor.limits.retry_delay = Duration::from_secs(2);
    let _cancel = active(&mut actor, 0, [1; 32], None);
    let source = actor.state.slots[0].as_ref().unwrap().source.clone().unwrap();
    actor.finish_attempt(response(TransferError::NotPublished)).await?;
    let part = &actor.state.partitions[&[1; 32]];
    assert_eq!(part.attempts[&source], 0);
    let (misses, until) = part.not_published[&source];
    assert_eq!(misses, 1);
    assert!(until > Instant::now());
    assert!(until <= Instant::now() + Duration::from_secs(2));
    let provider = &actor.state.sources[&source_base(&source)?];
    assert_eq!(provider.penalty, 0);
    assert_eq!(provider.failures, 0);
    assert!(provider.banned.is_none());
    assert!(actor.state.slots[0].is_none());
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_network_failure_lowers_rating_without_banning() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut actor = actor(directory.path())?;
    let _cancel = active(&mut actor, 0, [1; 32], None);
    actor.finish_attempt(response(TransferError::Network(anyhow::anyhow!("disconnected")))).await?;
    let source = &actor.state.sources[&Url::parse("http://source.test/share/")?];
    assert_eq!(source.penalty, 1);
    assert_eq!(source.failures, 1);
    assert!(source.banned.is_none());
    assert!(actor.state.slots[0].is_none());
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_bad_chunk_bans_source_across_partitions_and_cancels_siblings(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut actor = actor(directory.path())?;
    let _first = active(&mut actor, 0, [1; 32], None);
    let second = active(&mut actor, 1, [2; 32], None);
    actor.finish_attempt(response(TransferError::Invalid(anyhow::anyhow!("bad chunk")))).await?;
    assert!(actor.state.sources[&Url::parse("http://source.test/share/")?].banned.is_some());
    assert!(*second.borrow());
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_other_source_etag_is_a_representation_not_a_ban() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let mut actor = actor(directory.path())?;
    let _cancel = active(&mut actor, 0, [1; 32], Some("old-representation"));
    actor.finish_attempt(response(TransferError::EtagChanged("new-representation".into()))).await?;
    assert!(actor.state.sources[&Url::parse("http://source.test/share/")?].banned.is_none());
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_switches_etag_after_disconnect_before_first_chunk(
) -> anyhow::Result<()> {
    switch_etag_after_disconnect(false).await
}

#[tokio::test]
async fn snapshot_downloader_discards_alternate_etag_to_fit_cache() -> anyhow::Result<()> {
    switch_etag_after_disconnect(true).await
}

async fn switch_etag_after_disconnect(tight_cache: bool) -> anyhow::Result<()> {
    use std::collections::HashMap;
    use std::io::Write;

    use rand::RngCore;
    use rand::SeedableRng;
    use snapshot_storage::chunks;

    use crate::repository::repository_impl::snapshot_http_import::tests::Server;

    // Two valid wire representations of the SAME logical payload. Changing
    // the zstd checksum flag changes their chunk maps/ETags, not their contents.
    let mut payload = vec![57; 128 * 1024];
    if tight_cache {
        // Incompressible input leaves a real verified chunk in the failed
        // representation. Reclaiming it must remove its file as well as bytes
        // reserved for the not-yet-downloaded tail.
        payload.resize(chunks::CHUNK_BYTES * 3 + chunks::CHUNK_BYTES / 4, 0);
        rand::rngs::StdRng::seed_from_u64(42).fill_bytes(&mut payload);
    }
    let encode = |checksum| -> anyhow::Result<Vec<u8>> {
        let mut writer = chunks::ChunkWriter::new(Vec::new())?;
        let mut encoder = zstd::Encoder::new(&mut writer, 3)?;
        encoder.include_checksum(checksum)?;
        encoder.write_all(&payload)?;
        encoder.finish()?;
        Ok(writer.finish()?.0)
    };
    let interrupted_body = encode(false)?;
    let healthy_body = encode(true)?;
    let interrupted_map = chunks::read_footer(&mut std::io::Cursor::new(&interrupted_body))?;
    let healthy_map = chunks::read_footer(&mut std::io::Cursor::new(&healthy_body))?;
    assert_ne!(interrupted_map.etag(), healthy_map.etag());
    let budget = interrupted_body.len().max(healthy_body.len()) as u64;
    assert!(interrupted_body.len() as u64 + healthy_body.len() as u64 > budget);
    // The ordinary case disconnects before any complete chunk; the tight-cache
    // case disconnects after the first verified chunk.
    assert_eq!(interrupted_map.offset - 4 < chunks::CHUNK_BYTES as u64, !tight_cache);

    let manifest =
        ThreadSnapshotManifest { account_partition_roots: vec![[1; 32]], ..Default::default() };
    let id = manifest.partition_blob_id(0).unwrap();
    let name = hex::encode(id);
    let interrupted = Server::new(
        HashMap::from([(name.clone(), interrupted_body)]),
        name.clone(),
        None,
        None,
        Some(name.clone()),
    );
    let healthy = Server::new(
        HashMap::from([(name.clone(), healthy_body.clone())]),
        name.clone(),
        None,
        None,
        None,
    );
    let directory = tempfile::tempdir()?;
    // Either representation fits alone, but retaining both exceeds the budget.
    // Keep the manager alive so the worker reopens this exact cache configuration.
    let _cache =
        crate::services::blob_sync::external_fileshares_based::download_cache::DownloadCache::open(
            directory.path(),
            tight_cache.then_some(budget),
        )?;
    let mut actor = actor(directory.path())?;
    actor.state = SnapshotDownloadState::new(1);
    actor.state.manifest = manifest;
    actor.primary_ready = true;
    actor.fallbacks = vec![interrupted.base.clone(), healthy.base.clone()];
    let mut bk = crate::block_keeper_system::BlockKeeperSet::new();
    bk.insert(0, crate::block_keeper_system::BlockKeeperData::default());
    actor.source_plan = Some(SnapshotSourcePlan::new(&actor.state.manifest, &bk, [])?);
    actor.state.add(id, Some(0), vec![interrupted.base.join(&name)?, healthy.base.join(&name)?])?;
    // Discovering a different representation must not consume the healthy
    // provider's sole attempt, otherwise the next scheduling pass would fail.
    actor.limits.rounds = 1;
    let (events, mut ready) = mpsc::unbounded_channel();
    actor.events = Some(events);
    let worker = PartitionDownloader::new(directory.path(), actor.limits)?;
    let (commands, mut requests) = mpsc::channel(1);
    let (responses, mut replies) = mpsc::channel(1);

    // Drive the real scheduler and HTTP worker through their actual channels:
    // With a tight cache, the coordinator must discard the old representation
    // on Capacity and retry the healthy provider without spending its sole attempt.
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut partial_path = None;
        for step in 0..if tight_cache { 5 } else { 4 } {
            actor.schedule(std::slice::from_ref(&commands), &responses).await?;
            worker.run(requests.recv().await.expect("scheduled download")).await;
            let response = replies.recv().await.expect("terminal worker response");
            match step {
                0 => assert!(matches!(response.result, Err(TransferError::CacheMiss))),
                1 => {
                    assert!(matches!(response.result, Err(TransferError::Network(_))));
                    let state = response.state.as_ref().unwrap();
                    assert_eq!(state.verified_chunks, usize::from(tight_cache));
                    assert!(state.has_verified_map());
                    assert_eq!(state.etag, interrupted_map.etag());
                    partial_path = Some(state.path());
                }
                2 => assert!(matches!(response.result, Err(TransferError::EtagChanged(_)))),
                3 if tight_cache => assert!(
                    matches!(response.result, Err(TransferError::Capacity)),
                    "{:?}",
                    response.result
                ),
                3 | 4 => assert!(response.result.is_ok(), "{:?}", response.result),
                _ => unreachable!(),
            }
            actor.finish_attempt(response).await?;
            if step == 3 && tight_cache {
                assert!(
                    !partial_path.as_ref().unwrap().exists(),
                    "old private representation must be removed"
                );
                assert!(actor.state.partitions[&id].etags[&interrupted_map.etag()]
                    .download
                    .is_none());
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        }
        anyhow::Ok(())
    })
    .await??;

    let ThreadSnapshotDownloaderEvent::AccountsReady { root, path } = ready.try_recv()? else {
        panic!("expected a verified account partition");
    };
    assert_eq!(root, [1; 32]);
    assert_eq!(std::fs::read(&path)?, healthy_body);
    chunks::verify_file(&mut std::fs::File::open(&path)?, &|| false)?;
    assert_eq!(zstd::decode_all(&healthy_body[4..healthy_map.offset as usize])?, payload);
    assert!(actor.state.sources[&healthy.base].banned.is_none());
    assert_eq!(actor.state.sources[&healthy.base].etags[&id], healthy_map.etag());
    let path = actor.source_plan.as_ref().unwrap().partition_path(&actor.primary_id, 0);
    assert_eq!(actor.state.partitions[&id].attempts[&healthy.base.join(&path)?], 1);
    assert!(healthy.raw_requests().iter().any(|request| request.starts_with("HEAD")));
    actor.process_blob(id, true).await?;
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_retries_unadmitted_blob_without_starving_its_worker(
) -> anyhow::Result<()> {
    use std::collections::HashMap;
    use std::io::Write;

    use snapshot_storage::chunks;

    use crate::repository::repository_impl::snapshot_http_import::tests::Server;

    // More partitions than worker slots: after the first 404 the scheduler
    // must NOT give its free slot to a fifth task waiting behind that retry.
    let manifest = ThreadSnapshotManifest {
        account_partition_roots: (1..=6).map(|i| [i; 32]).collect(),
        ..Default::default()
    };
    let ids: Vec<_> = (0..6).map(|i| manifest.partition_blob_id(i).unwrap()).collect();
    let mut writer = chunks::ChunkWriter::new(Vec::new())?;
    writer.write_all(b"transport-only fixture")?;
    let body = writer.finish()?.0;
    let missing = Server::new(HashMap::new(), String::new(), None, None, None);
    let good = Server::new(
        ids.iter().map(|id| (hex::encode(id), body.clone())).collect(),
        String::new(),
        None,
        None,
        None,
    );
    let directory = tempfile::tempdir()?;
    let mut actor = actor(directory.path())?;
    actor.state = SnapshotDownloadState::new(4);
    actor.state.manifest = manifest;
    actor.primary_ready = true;
    actor.fallbacks = vec![missing.base.clone(), good.base.clone()];
    actor.limits.retry_delay = Duration::from_millis(10);
    // Both endpoints initially return 404. Even the eventually healthy one
    // misses more often than the two-attempt transport-error budget permits.
    good.defer_publication(&hex::encode(ids[0]), 4);
    let mut bk = crate::block_keeper_system::BlockKeeperSet::new();
    bk.insert(0, crate::block_keeper_system::BlockKeeperData::default());
    actor.source_plan = Some(SnapshotSourcePlan::new(&actor.state.manifest, &bk, [])?);
    for id in &ids {
        actor.state.add(
            *id,
            Some(actor.state.order.len()),
            vec![missing.base.join(&hex::encode(id))?, good.base.join(&hex::encode(id))?],
        )?;
    }
    let (events, mut ready) = mpsc::unbounded_channel();
    actor.events = Some(events);
    let (responses, mut replies) = mpsc::channel(4);
    let mut workers = JoinSet::new();
    let mut commands = Vec::new();
    for _ in 0..4 {
        let worker = PartitionDownloader::new(directory.path(), actor.limits)?;
        let (sender, mut receiver) = mpsc::channel(1);
        workers.spawn(async move {
            while let Some(command) = receiver.recv().await {
                worker.run(command).await;
            }
        });
        commands.push(sender);
    }
    let mut saw_404 = false;
    let mut saw_cooldown = false;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while actor.state.partitions.values().any(|part| !part.ready) {
            actor.schedule(&commands, &responses).await?;
            tokio::select! {
                response = replies.recv() => {
                    let response = response.expect("worker response");
                    let first_404 = matches!(&response.result, Err(TransferError::NotPublished));
                    actor.finish_attempt(response).await?;
                    saw_404 |= first_404;
                    let head = &actor.state.partitions[&ids[0]];
                    if head.not_published.len() == 2 && head.has_pending_admission()
                        && head.not_published.values().all(|(_, until)| Instant::now() < *until)
                    {
                        saw_cooldown = true;
                        actor.schedule(&commands, &responses).await?;
                        anyhow::ensure!(actor.state.slots.iter().any(Option::is_none),
                            "retry lost its worker to successors waiting on its admission");
                        anyhow::ensure!(actor.state.partitions[&ids[4]].active_task.is_none(),
                            "fifth partition must not occupy the retry's worker");
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        anyhow::Ok(())
    })
    .await;
    for task in actor.state.slots.iter().flatten() {
        let _ = task.cancel.send(true);
    }
    drop(commands);
    while let Some(joined) = workers.join_next().await {
        joined?;
    }
    result??;
    assert!(saw_404);
    assert!(saw_cooldown);
    let path = actor.source_plan.as_ref().unwrap().partition_path(&actor.primary_id, 0);
    assert_eq!(actor.state.partitions[&ids[0]].not_published[&good.base.join(&path)?].0, 4);
    assert_eq!(actor.state.partitions[&ids[0]].attempts[&good.base.join(&path)?], 1);
    assert!(actor.state.sources.values().all(|source| source.banned.is_none()));
    assert!(actor.state.sources.values().all(|source| source.penalty == 0 && source.failures == 0));
    for _ in &ids {
        let ThreadSnapshotDownloaderEvent::AccountsReady { path, .. } = ready.try_recv()? else {
            panic!("expected verified accounts");
        };
        assert_eq!(std::fs::read(path)?, body);
    }
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_same_source_changing_its_etag_is_banned() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut actor = actor(directory.path())?;
    let provider = Url::parse("http://source.test/share/")?;
    actor
        .state
        .sources
        .get_mut(&provider)
        .unwrap()
        .etags
        .insert([1; 32], "old-representation".into());
    let _cancel = active(&mut actor, 0, [1; 32], Some("old-representation"));
    actor.finish_attempt(response(TransferError::EtagChanged("new-representation".into()))).await?;
    assert!(actor.state.sources[&provider].banned.is_some());
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_known_etag_survives_eviction_of_its_partial_file() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let mut actor = actor(directory.path())?;
    let provider = Url::parse("http://source.test/share/")?;
    actor.state.sources.get_mut(&provider).unwrap().etags.insert([1; 32], "known-new".into());
    // The provider's own representation was evicted; the task tried another
    // saved representation. Returning known-new is NOT changing its own ETag.
    let _cancel = active(&mut actor, 0, [1; 32], Some("other-old"));
    actor.finish_attempt(response(TransferError::EtagChanged("known-new".into()))).await?;
    assert!(actor.state.sources[&provider].banned.is_none());
    Ok(())
}

#[tokio::test]
async fn snapshot_downloader_unknown_and_duplicate_cache_handoffs_are_errors() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let mut actor = actor(directory.path())?;
    assert!(actor.process_blob([9; 32], true).await.is_err());
    assert!(actor.process_blob([1; 32], true).await.is_err());
    actor.state.partitions.get_mut(&[1; 32]).unwrap().ready = true;
    actor.process_blob([1; 32], true).await?;
    assert!(actor.process_blob([1; 32], true).await.is_err());
    Ok(())
}
