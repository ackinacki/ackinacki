// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use telemetry_utils::mpsc::instrumented_channel;
use tokio::time::Instant;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::helper::block_flow_trace;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::associated_types::SynchronizationResult;
use crate::node::network_message::Command;
use crate::node::services::sync::StateSyncService;
use crate::node::services::sync::SyncSnapshotAnchor;
use crate::node::services::sync::SyncSnapshotLoaded;
use crate::node::services::sync::SyncSnapshotRequest;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::BlockHeight;
use crate::types::BlockIndex;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;

#[derive(Clone, Copy)]
enum InitialStateMarker {
    Height(BlockHeight),
}

fn is_loaded_snapshot_current(active: &SyncSnapshotRequest, loaded: &SyncSnapshotLoaded) -> bool {
    active.address == loaded.address
}

#[derive(Clone, Copy)]
struct BridgeCheckResult {
    can_bridge: bool,
    checked_blocks: usize,
}

fn has_parent_chain_between<F>(
    from_newer_block_id: BlockIdentifier,
    to_older_block_id: BlockIdentifier,
    max_steps: usize,
    mut parent_of: F,
) -> BridgeCheckResult
where
    F: FnMut(BlockIdentifier) -> Option<BlockIdentifier>,
{
    let mut current = from_newer_block_id;
    let mut visited = HashSet::new();
    let mut checked_blocks = 0usize;
    while current != to_older_block_id {
        if checked_blocks >= max_steps || !visited.insert(current) {
            return BridgeCheckResult { can_bridge: false, checked_blocks };
        }
        let Some(parent) = parent_of(current) else {
            return BridgeCheckResult { can_bridge: false, checked_blocks };
        };
        current = parent;
        checked_blocks += 1;
    }
    BridgeCheckResult { can_bridge: true, checked_blocks }
}

fn loaded_anchor_can_be_used_before_active(
    loaded: SyncSnapshotAnchor,
    active: SyncSnapshotAnchor,
) -> bool {
    match (loaded, active) {
        (SyncSnapshotAnchor::Height(loaded), SyncSnapshotAnchor::Height(active)) => {
            loaded.signed_distance_to(&active).map(|distance| distance >= 0).unwrap_or(false)
        }
    }
}

fn should_accept_sync_snapshot_target(
    current: Option<&SyncSnapshotRequest>,
    new: &SyncSnapshotRequest,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    if current.address == new.address {
        return false;
    }
    match (current.anchor, new.anchor) {
        (SyncSnapshotAnchor::Height(current), SyncSnapshotAnchor::Height(new)) => {
            current.signed_distance_to(&new).map(|distance| distance > 0).unwrap_or(false)
        }
    }
}

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    fn finish_synchronization_from_loaded_snapshot(
        &mut self,
        loaded_snapshot: SyncSnapshotLoaded,
    ) -> anyhow::Result<SynchronizationResult<(NetworkMessage, SocketAddr)>> {
        let resource_address = loaded_snapshot.address;
        let Some(synced_block_id) = resource_address.get(&self.thread_id).cloned() else {
            anyhow::bail!("Thread to block is missing in the resources map");
        };
        let Some(synced_block_seq_no) =
            self.block_state_repository.get(&synced_block_id)?.guarded(|e| *e.block_seq_no())
        else {
            anyhow::bail!("Synced block seq_no is missing for {synced_block_id:?}");
        };
        self.unprocessed_blocks_cache.retain(|e| {
            e.guarded(|e| *e.block_seq_no())
                .map(|seq_no| seq_no >= synced_block_seq_no)
                // IMPORTANT: KEEP BLOCKS THAT ARE INPROGRESS! OTHERWISE IT MAY DROP A BLOCK THAT WILL BE NEEDED LATER
                .unwrap_or(true)
        });
        tracing::trace!("Consensus received sync: {synced_block_seq_no} {synced_block_id:?}");
        Ok(SynchronizationResult::Ok)
    }

    fn can_finish_sync_from_outdated_loaded_snapshot(
        &self,
        loaded: &SyncSnapshotLoaded,
        active: &SyncSnapshotRequest,
    ) -> anyhow::Result<BridgeCheckResult> {
        let Some(loaded_block_id) = loaded.address.get(&self.thread_id).cloned() else {
            return Ok(BridgeCheckResult { can_bridge: false, checked_blocks: 0 });
        };
        let Some(active_block_id) = active.address.get(&self.thread_id).cloned() else {
            return Ok(BridgeCheckResult { can_bridge: false, checked_blocks: 0 });
        };
        if loaded_block_id == active_block_id {
            return Ok(BridgeCheckResult { can_bridge: true, checked_blocks: 0 });
        }
        if !loaded_anchor_can_be_used_before_active(loaded.anchor, active.anchor) {
            return Ok(BridgeCheckResult { can_bridge: false, checked_blocks: 0 });
        }
        self.has_local_chain_between(active_block_id, loaded_block_id)
    }

    fn has_local_chain_between(
        &self,
        from_newer_block_id: BlockIdentifier,
        to_older_block_id: BlockIdentifier,
    ) -> anyhow::Result<BridgeCheckResult> {
        let maybe_depth_bound = {
            let from = self.block_state_repository.get(&from_newer_block_id)?;
            let to = self.block_state_repository.get(&to_older_block_id)?;
            let by_height = from
                .guarded(|e| *e.block_height())
                .zip(to.guarded(|e| *e.block_height()))
                .and_then(|(from_h, to_h)| from_h.signed_distance_to(&to_h))
                .and_then(|distance| usize::try_from(distance).ok());
            let by_seq_no = from
                .guarded(|e| *e.block_seq_no())
                .zip(to.guarded(|e| *e.block_seq_no()))
                .and_then(|(from_s, to_s)| {
                    if from_s >= to_s {
                        usize::try_from((from_s - to_s) as u64).ok()
                    } else {
                        None
                    }
                });
            by_height.or(by_seq_no).map(|distance| distance.saturating_add(1))
        };
        let safe_bound = maybe_depth_bound.unwrap_or(10_000).min(100_000);

        let bridge = has_parent_chain_between(
            from_newer_block_id,
            to_older_block_id,
            safe_bound,
            |current| {
                self.block_state_repository.get(&current).ok().and_then(|state| {
                    let parent = state.guarded(|inner| *inner.parent_block_identifier());
                    parent?;
                    // Require block data to be present locally, not just an empty lazily created state.
                    let is_available = self
                        .unprocessed_blocks_cache
                        .get_block_by_id(&current)
                        .is_some()
                        || self.repository.get_finalized_block(&current).ok().flatten().is_some();
                    if !is_available {
                        return None;
                    }
                    parent
                })
            },
        );
        Ok(bridge)
    }

    pub(crate) fn execute_synchronizing(
        &mut self,
    ) -> anyhow::Result<SynchronizationResult<(NetworkMessage, SocketAddr)>> {
        tracing::trace!(target: "monit", "Start synchronization");
        let (synchronization_tx, synchronization_rx) = instrumented_channel(
            self.metrics.clone(),
            crate::helper::metrics::STATE_LOAD_RESULT_CHANNEL,
        );
        let mut initial_state: Option<(BlockIdentifier, InitialStateMarker)> = None;
        let mut active_sync_snapshot: Option<SyncSnapshotRequest> = None;
        let mut last_node_join_message_time = Instant::now();
        // Broadcast NodeJoining only the first start of the node, do not broadcast it on split
        if self.thread_id == ThreadIdentifier::default() {
            self.broadcast_node_joining()?;
        }
        let mut block_request_was_sent = false;
        let mut recieved_sync_from = None;

        loop {
            if *SHUTDOWN_FLAG.get().unwrap_or(&false) {
                let (blocks_queue, _) = self.unprocessed_blocks_cache.clone_queue();
                tracing::trace!("Stop execution: {}", blocks_queue.blocks().len());
                self.repository.dump_unfinalized_blocks(blocks_queue);
                tracing::trace!("Stop execution");
                return Ok(SynchronizationResult::Interrupted);
            }
            // Periodic NodeJoining re-broadcast. NOT gated on whether a
            // load thread is currently running — the new
            // `add_load_state_task` semantics (bounded LIFO of
            // candidates) make it safe to keep advertising for state
            // even while a download is in progress. New sync-finalized
            // responses just push into the candidate queue (with dedup
            // and capacity-bounded eviction); the worker iterates
            // newest-first on each pass.
            if last_node_join_message_time.elapsed() > self.global_config.node_joining_timeout {
                self.broadcast_node_joining()?;
                last_node_join_message_time = Instant::now();
                // We may already have a downloaded state buffered; the
                // worker will report it on success. Resetting the
                // buffered initial-state hints here would discard a
                // pending result, so keep them.
            }
            if let Some(active_snapshot) = active_sync_snapshot.clone() {
                match synchronization_rx.try_recv() {
                    Ok(Ok(loaded_snapshot)) => {
                        if !is_loaded_snapshot_current(&active_snapshot, &loaded_snapshot) {
                            let loaded_block_id =
                                loaded_snapshot.address.get(&self.thread_id).cloned();
                            let active_block_id =
                                active_snapshot.address.get(&self.thread_id).cloned();
                            let bridge = self.can_finish_sync_from_outdated_loaded_snapshot(
                                &loaded_snapshot,
                                &active_snapshot,
                            )?;
                            if bridge.can_bridge {
                                tracing::info!(
                                    target: "monit",
                                    "Downloaded state is older than active snapshot, but local chain bridges the gap; finishing sync from loaded snapshot: loaded_anchor_kind={}, active_anchor_kind={}, loaded_block_id={loaded_block_id:?}, active_block_id={active_block_id:?}, checked_blocks={}",
                                    loaded_snapshot.anchor.kind(),
                                    active_snapshot.anchor.kind(),
                                    bridge.checked_blocks,
                                );
                                self.state_sync_service.clear_load_state_tasks();
                                return self
                                    .finish_synchronization_from_loaded_snapshot(loaded_snapshot);
                            }
                            tracing::trace!(
                                target: "monit",
                                "Downloaded state is already outdated and local chain does not bridge the gap; waiting for active snapshot: loaded_anchor_kind={}, active_anchor_kind={}, loaded_block_id={loaded_block_id:?}, active_block_id={active_block_id:?}, checked_blocks={}",
                                loaded_snapshot.anchor.kind(),
                                active_snapshot.anchor.kind(),
                                bridge.checked_blocks,
                            );
                            continue;
                        }
                        return self.finish_synchronization_from_loaded_snapshot(loaded_snapshot);
                    }
                    Ok(Err(e)) => {
                        active_sync_snapshot = None;
                        // Note: State download failed.
                        // Nothing can be done at this moment. Should be investigated.
                        tracing::error!("Synchronization error: {}", e);
                    }
                    Err(TryRecvError::Disconnected) => {
                        // TODO: we have to fix channel and move it entirely so this event may
                        // happen. Otherwise it is possible in case of the sync service failure to
                        // stay in this loop forever.
                        anyhow::bail!(
                            "Something went wrong. Sync Service failed to load the state."
                        );
                    }
                    Err(TryRecvError::Empty) => {}
                };
            }
            tracing::debug!("[synchronizing]({:?}) waiting for a network message", self.thread_id);
            match self.network_rx.recv_timeout(Duration::from_millis(
                self.global_config.time_to_produce_block_millis,
            )) {
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::error!("Disconnect signal received (synchronization)");
                    return Ok(SynchronizationResult::Interrupted);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Ok((msg, reply_to)) => match msg {
                    NetworkMessage::InnerCommand(Command::TryStartSynchronization) => {
                        continue;
                    }
                    NetworkMessage::AuthoritySwitchProtocol(_) => {
                        // todo!("monitor authority switch accepts");
                        // Authority switch is processed in execute_normal
                        continue;
                    }
                    NetworkMessage::Candidate(ref net_block)
                    | NetworkMessage::ResentCandidate((ref net_block, _)) => {
                        tracing::debug!("[synchronizing] Incoming candidate block");
                        tracing::debug!("[synchronizing] Incoming block candidate: {}", net_block,);
                        block_flow_trace(
                            "received candidate [synchronizing]",
                            &net_block.identifier,
                            &self.config.local.node_id,
                            [],
                        );
                        let resend_node_id =
                            if let NetworkMessage::ResentCandidate((_, node_id)) = &msg {
                                Some(node_id.clone())
                            } else {
                                None
                            };
                        let mut envelope =
                            self.on_incoming_candidate_block(net_block, resend_node_id)?;

                        if let Some((block_id, initial_state_marker)) = initial_state {
                            match initial_state_marker {
                                InitialStateMarker::Height(initial_block_height) => {
                                    tracing::debug!(
                                        "[synchronizing] initial_state block: {}, block_height: {:?}",
                                        block_id.clone(),
                                        initial_block_height,
                                    );
                                    let candidate_block_height = if let Some(envelope) =
                                        envelope.as_ref()
                                    {
                                        *envelope.data().common_section().block_height()
                                    } else {
                                        let Ok(block_envelope) = net_block.get_envelope() else {
                                            tracing::trace!("Failed to deserialize block");
                                            continue;
                                        };
                                        let candidate_block_height =
                                            *block_envelope.data().common_section().block_height();
                                        if block_id == net_block.identifier {
                                            envelope = Some(block_envelope);
                                        }
                                        candidate_block_height
                                    };
                                    if initial_block_height
                                        .signed_distance_to(&candidate_block_height)
                                        .map(|distance| distance < 0)
                                        .unwrap_or(false)
                                    {
                                        continue;
                                    }
                                }
                            }
                            // We have received finalized block from stopped BP
                            // It can be older than our last finalized, so process here
                            if block_id == net_block.identifier {
                                let Ok(block_envelope) = net_block.get_envelope() else {
                                    tracing::trace!("Failed to deserialize block");
                                    continue;
                                };
                                if envelope.is_none() {
                                    envelope = Some(block_envelope);
                                }
                                let envelope = envelope.as_ref().unwrap();
                                if self.check_block_signature(envelope) != Some(true) {
                                    continue;
                                }
                                if active_sync_snapshot.is_none() {
                                    // Here we can have such situation:
                                    // nodes were working well, but suddenly BP lost network,
                                    // or all nodes were turned off. When nodes rise up they can
                                    // have block finalized newer than BP. In that case they will not
                                    // send their attestation to BP again and BP will not finalize block.
                                    // So we set finalized block force with possible downgrade.
                                    // But this should be fixed with attestation resending mechanism.
                                    // self.repository.store_block(candidate_block.clone())?;
                                    // self.finalize_synced_block(candidate_block)?;
                                    // return match self.take_next_unprocessed_block(block_id, seq_no)? {
                                    //     Some(next_block) => {
                                    //         tracing::trace!(
                                    //             "Next unprocessed block after sync: {:?} {:?}",
                                    //             next_block.data().seq_no(),
                                    //             next_block.data().identifier()
                                    //         );
                                    //         Ok(SynchronizationResult::Forward(
                                    //             NetworkMessage::Candidate(next_block),
                                    //         ))
                                    //     }
                                    //     None => {
                                    tracing::trace!(
                                        "Next unprocessed block after sync was not found"
                                    );
                                    return Ok(SynchronizationResult::Ok);
                                    //     }
                                    // };
                                } else {
                                    // Otherwise wait for state
                                    // TODO: fix adding to unprocessed cache
                                    // self.add_unprocessed_block(candidate_block.clone())?;
                                }
                            }
                        }
                        if let Some(seq_no) = recieved_sync_from.as_ref() {
                            if &net_block.seq_no < seq_no {
                                continue;
                            }
                        }

                        let Ok(block_envelope) = net_block.get_envelope() else {
                            tracing::trace!("Failed to deserialize block");
                            continue;
                        };
                        if envelope.is_none() {
                            envelope = Some(block_envelope);
                        }
                        let envelope = envelope.as_ref().unwrap();
                        // if self.is_candidate_block_can_be_applied(candidate_block)
                        //         .unwrap_or(false)
                        let parent_id = envelope.data().parent();
                        if self
                            .block_state_repository
                            .get(&parent_id)?
                            .guarded(|e| e.is_block_already_applied())
                        {
                            // If we have received block that can be applied end synchronization
                            tracing::trace!("[synchronizing] Candidate block can be applied");
                            // let parent_id = candidate_block.data().parent();
                            // if parent_id == BlockIdentifier::default() {
                            return Ok(SynchronizationResult::Forward((msg, reply_to)));
                            // }
                            // if let Some(block) = self.repository.get_block(&parent_id)? {
                            //     if self.is_candidate_block_signed_by_this_node(&block)? {
                            //         return Ok(SynchronizationResult::Forward(msg));
                            //     }
                            // }
                            // self.add_unprocessed_block(candidate_block.clone())?;
                        } else if !block_request_was_sent {
                            tracing::trace!("[synchronizing] Candidate block can't be applied check for block request");
                            let (_last_block_id, last_block_seq_no) = {
                                // if self.is_spawned_from_node_sync {
                                (BlockIdentifier::default(), BlockSeqNo::default())
                                // } else {
                                // self.find_thread_last_block_id_this_node_can_continue(
                                //      &self.thread_id
                                //  )?
                                // }
                            };
                            let first_missed_block_seq_no = next_seq_no(last_block_seq_no);
                            tracing::trace!("[synchronizing] first_missed_block_seq_no: {first_missed_block_seq_no:?}");
                            let incoming_block_seq_no = net_block.seq_no;
                            let seq_no_diff = incoming_block_seq_no - first_missed_block_seq_no;
                            if seq_no_diff < self.global_config.need_synchronization_block_diff
                                && !block_request_was_sent
                            {
                                block_request_was_sent = true;
                                self.send_block_request(
                                    net_block.producer_id.clone(),
                                    first_missed_block_seq_no,
                                    incoming_block_seq_no,
                                    None,
                                )?;
                            }
                        }

                        if *envelope.data().directives().share_state_resources() {
                            let last_finalized_block_seq_no = self
                                .repository
                                .select_thread_last_finalized_block(&self.thread_id)?
                                .map(|(_, seq_no)| seq_no)
                                .unwrap_or_default();
                            if last_finalized_block_seq_no >= net_block.seq_no {
                                tracing::debug!("[synchronizing] Received block with shared state seq_no({:?} is less or equal to local last finalized seq_no ({last_finalized_block_seq_no:?}), skip it", net_block.seq_no);
                                continue;
                            }

                            let block_id = envelope.data().identifier();
                            let thread_id = *envelope.data().common_section().thread_id();
                            let block_height = *envelope.data().common_section().block_height();
                            let resource_address = BTreeMap::from([(thread_id, block_id)]);
                            let request = SyncSnapshotRequest {
                                address: resource_address.clone(),
                                anchor: SyncSnapshotAnchor::Height(block_height),
                            };
                            if should_accept_sync_snapshot_target(
                                active_sync_snapshot.as_ref(),
                                &request,
                            ) {
                                last_node_join_message_time = Instant::now();
                                active_sync_snapshot = Some(request);
                                initial_state = Some((
                                    net_block.identifier,
                                    InitialStateMarker::Height(block_height),
                                ));

                                self.state_sync_service.add_load_state_task_with_height(
                                    resource_address,
                                    block_height,
                                    self.repository.clone(),
                                    synchronization_tx.clone(),
                                )?;
                            }
                        }
                    }
                    NetworkMessage::Ack(_ack) => {
                        tracing::debug!("[synchronizing] Received Ack block");
                    }
                    NetworkMessage::Nack(_nack) => {
                        // It seems we have joined in the middle of some big sht happening.
                        // Well. since there's no state we can't join any commitee, ignoring.
                        tracing::debug!("[synchronizing] Received Nack block");
                    }
                    NetworkMessage::NodeJoining(_) => {
                        tracing::debug!("[synchronizing] Received NodeJoining");
                    }
                    NetworkMessage::NodeJoiningWithLastFinalized(_) => {
                        tracing::debug!("[synchronizing] Received NodeJoiningWithLastFinalized");
                    }
                    NetworkMessage::ExternalMessage(_) => {
                        tracing::debug!("[synchronizing] Received ExternalMessage");
                    }
                    NetworkMessage::BlockAttestation(_) => {
                        tracing::debug!("[synchronizing] Received BlockAttestation");
                    }
                    NetworkMessage::BlockRequest { .. } => {
                        tracing::debug!("[synchronizing] Received BlockRequest");
                    }
                    NetworkMessage::SyncFrom((seq_no_from, _)) => {
                        tracing::debug!("[synchronizing] Received SyncFrom: {:?}", seq_no_from);
                        // Clear previous waited
                        active_sync_snapshot = None;
                        initial_state = None;
                        recieved_sync_from = Some(seq_no_from);
                    }
                    NetworkMessage::SyncFinalized((sync_finalized, _thread_identifier)) => {
                        tracing::debug!(
                            "[synchronizing] Received legacy SyncFinalized, ignoring: {:?} {:?} {:?}",
                            sync_finalized.data().block_seq_no(),
                            sync_finalized.data().block_identifier(),
                            sync_finalized.data().thread_refs()
                        );
                    }
                    NetworkMessage::SyncFinalizedWithHeight((
                        sync_finalized,
                        _thread_identifier,
                    )) => {
                        let duration_since_last_finalization =
                            self.shared_services.duration_since_last_finalization();
                        if duration_since_last_finalization
                            < self.global_config.time_to_enable_sync_finalized
                        {
                            continue;
                        }
                        let identifier = *sync_finalized.data().block_identifier();
                        let block_height = *sync_finalized.data().block_height();
                        let address = sync_finalized.data().thread_refs().clone();
                        tracing::debug!(
                            "[synchronizing] Received SyncFinalizedWithHeight: {:?} {:?} {:?}",
                            block_height,
                            identifier,
                            address
                        );
                        if address.get(&self.thread_id) != Some(&identifier) {
                            tracing::trace!("Incoming SyncFinalizedWithHeight is broken, skip it");
                            continue;
                        }
                        let Some((last_finalized_block_id, _last_finalized_block_seq_no)) =
                            self.repository.select_thread_last_finalized_block(&self.thread_id)?
                        else {
                            tracing::trace!(
                                "Last finalized block is missing, skip SyncFinalizedWithHeight"
                            );
                            continue;
                        };
                        let Some(last_finalized_block_height) = self
                            .block_state_repository
                            .get(&last_finalized_block_id)?
                            .guarded(|e| *e.block_height())
                        else {
                            tracing::trace!(
                                "Last finalized block height is missing, skip SyncFinalized"
                            );
                            continue;
                        };
                        let is_newer = last_finalized_block_height
                            .signed_distance_to(&block_height)
                            .map(|distance| distance > 0)
                            .unwrap_or(false);
                        if !is_newer {
                            tracing::debug!(
                                "[synchronizing] Received SyncFinalizedWithHeight height({block_height:?}) is stale relative to local last finalized height ({last_finalized_block_height:?}), skip it"
                            );
                            continue;
                        }
                        let request = SyncSnapshotRequest {
                            address: address.clone(),
                            anchor: SyncSnapshotAnchor::Height(block_height),
                        };
                        if should_accept_sync_snapshot_target(
                            active_sync_snapshot.as_ref(),
                            &request,
                        ) {
                            tracing::trace!(
                                "[synchronizing] start loading shared state anchor_kind=height"
                            );
                            active_sync_snapshot = Some(request);
                            last_node_join_message_time = Instant::now();
                            initial_state =
                                Some((identifier, InitialStateMarker::Height(block_height)));
                            self.state_sync_service.add_load_state_task_with_height(
                                address,
                                block_height,
                                self.repository.clone(),
                                synchronization_tx.clone(),
                            )?;
                        }
                    }
                },
            }
        }
    }

    pub(crate) fn share_finalized_state(
        &mut self,
        last_finalized_seq_no: BlockSeqNo,
        last_finalized_block_id: BlockIdentifier,
    ) -> anyhow::Result<()> {
        if last_finalized_seq_no != BlockSeqNo::default() {
            let Some(last_finalized_block_height) = self
                .block_state_repository
                .get(&last_finalized_block_id)?
                .guarded(|e| *e.block_height())
            else {
                anyhow::bail!("Missing block height for last finalized block");
            };
            tracing::trace!(
                "share finalized state: id: {:?}; seq_no: {:?}",
                last_finalized_block_id,
                last_finalized_seq_no
            );
            // let finalized_block = self
            //     .repository
            //     .get_finalized_block(&last_finalized_block_id)?
            //     .ok_or(anyhow::format_err!("missing last finalized block"))?;
            // let finalized_state = self
            //     .repository
            //     .get_optimistic_state(
            //         &last_finalized_block_id,
            //         finalized_block.data().common_section().thread_id(),
            //         None,
            //     )?
            //     .ok_or(anyhow::format_err!("Failed to load finalized state"))?;
            // let share_state_hint = finalized_state.get_share_stare_refs();
            let share_state_hint = HashMap::from_iter([(self.thread_id, last_finalized_block_id)]);
            for (thread_id, block_id) in &share_state_hint {
                let Some(block_seq_no) =
                    self.block_state_repository.get(block_id)?.guarded(|e| *e.block_seq_no())
                else {
                    anyhow::bail!("Failed to get block seq no: {:?}", block_id);
                };
                let block_index = BlockIndex::new(block_seq_no, *block_id);
                let mut cursors = vec![*block_id];
                let mut finalizing_block = None;
                while finalizing_block.is_none() {
                    anyhow::ensure!(
                        !cursors.is_empty(),
                        "Failed to find finalizing block for {:?}",
                        block_id
                    );
                    let cursor = cursors.pop().unwrap();
                    self.block_state_repository.get(&cursor)?.guarded(|e| {
                        if e.finalizes_blocks()
                            .as_ref()
                            .map(|blocks| blocks.contains(&block_index))
                            .unwrap_or(false)
                        {
                            finalizing_block = Some(cursor);
                        }
                        cursors.extend(e.known_children(thread_id).cloned().unwrap_or_default());
                    });
                    if finalizing_block.is_some() {
                        break;
                    }
                }

                let anchor =
                    self.repository.build_snapshot_anchor_for_block(block_id, thread_id)?;
                self.state_sync_service.save_state_for_sharing(
                    block_id,
                    thread_id,
                    anchor,
                    None,
                    finalizing_block.unwrap(),
                )?;
            }
            self.last_synced_state =
                Some((last_finalized_block_id, last_finalized_seq_no, last_finalized_block_height));
            self.broadcast_sync_finalized(
                last_finalized_block_id,
                last_finalized_seq_no,
                last_finalized_block_height,
                share_state_hint,
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::has_parent_chain_between;
    use super::loaded_anchor_can_be_used_before_active;
    use super::SyncSnapshotAnchor;
    use crate::types::BlockHeight;

    fn block_id(seed: u8) -> node_types::BlockIdentifier {
        node_types::BlockIdentifier::new([seed; 32])
    }

    #[test]
    fn loaded_anchor_order_height() {
        let thread_id = node_types::ThreadIdentifier::default();
        let h10 = BlockHeight::builder().thread_identifier(thread_id).height(10).build();
        let h15 = BlockHeight::builder().thread_identifier(thread_id).height(15).build();
        assert!(loaded_anchor_can_be_used_before_active(
            SyncSnapshotAnchor::Height(h10),
            SyncSnapshotAnchor::Height(h15)
        ));
        assert!(!loaded_anchor_can_be_used_before_active(
            SyncSnapshotAnchor::Height(h15),
            SyncSnapshotAnchor::Height(h10)
        ));
    }

    #[test]
    fn has_parent_chain_between_accepts_linear_chain() {
        let b1 = block_id(1);
        let b2 = block_id(2);
        let b3 = block_id(3);
        let graph = HashMap::from([(b3, b2), (b2, b1)]);
        let result = has_parent_chain_between(b3, b1, 32, |id| graph.get(&id).cloned());
        assert!(result.can_bridge);
        assert_eq!(result.checked_blocks, 2);
    }

    #[test]
    fn has_parent_chain_between_rejects_fork_and_missing_parent() {
        let b1 = block_id(1);
        let b2 = block_id(2);
        let b3 = block_id(3);
        let bx = block_id(9);
        let graph = HashMap::from([(b3, b2), (b2, bx)]);
        let result = has_parent_chain_between(b3, b1, 32, |id| graph.get(&id).cloned());
        assert!(!result.can_bridge);
    }
}
