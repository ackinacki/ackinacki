use std::collections::HashSet;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;

use network::channel::NetDirectSender;
use parking_lot::Mutex;

use super::associated_types::NodeAssociatedTypes;
use super::block_state::repository::BlockStateRepository;
use super::shared_services::SharedServices;
use super::NetworkMessage;
use super::NodeIdentifier;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::config::Config;
use crate::helper::block_flow_trace;
use crate::helper::metrics::BlockProductionMetrics;
use crate::node::block_state::repository::BlockState;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;

pub struct BlockRequestParams {
    pub start: BlockSeqNo,
    pub end: BlockSeqNo,
    pub node_id: NodeIdentifier,
    pub at_least_n_blocks: Option<usize>,
    pub last_state_sync_executed: Arc<Mutex<std::time::Instant>>,
    pub is_state_sync_requested: Arc<Mutex<Option<BlockSeqNo>>>,
    pub thread_id: ThreadIdentifier,
}

pub struct BlockRequestService {
    config: Config,
    repository: RepositoryImpl,
    shared_services: SharedServices,
    block_state_repository: BlockStateRepository,
    network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
    rx: Receiver<BlockRequestParams>,
    metrics: Option<BlockProductionMetrics>,
    unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
}

impl BlockRequestService {
    pub fn start(
        config: Config,
        shared_services: SharedServices,
        repository: RepositoryImpl,
        block_state_repository: BlockStateRepository,
        network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
        metrics: Option<BlockProductionMetrics>,
        unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
    ) -> anyhow::Result<(Sender<BlockRequestParams>, JoinHandle<anyhow::Result<()>>)> {
        let (tx, rx) = std::sync::mpsc::channel::<BlockRequestParams>();

        let service = BlockRequestService {
            config,
            shared_services,
            repository,
            block_state_repository,
            network_direct_tx,
            rx,
            metrics,
            unprocessed_blocks_cache,
        };

        let handle: JoinHandle<anyhow::Result<()>> = std::thread::Builder::new()
            .name("BlockRequestService".into())
            .spawn_critical(move || {
                for request in service.rx.iter() {
                    service.on_incoming_block_request(request)?
                }
                Err(anyhow::anyhow!("Sender has disconnected"))
            })?;

        Ok((tx, handle))
    }

    fn on_incoming_block_request(&self, params: BlockRequestParams) -> anyhow::Result<()> {
        let BlockRequestParams {
            start,
            end,
            node_id,
            at_least_n_blocks,
            last_state_sync_executed,
            is_state_sync_requested,
            thread_id,
        } = params;

        match self.inner(start, end, node_id.clone(), &thread_id, at_least_n_blocks) {
            Ok(()) => {}
            Err(e) => {
                tracing::info!(
                    "Request from {node_id} for blocks range [{:?},{:?}) failed with: {e:?}",
                    start,
                    end
                );

                let elapsed = last_state_sync_executed.guarded(|e| e.elapsed());

                if elapsed > self.config.global.min_time_between_state_publish_directives {
                    {
                        let mut guard = last_state_sync_executed.lock();
                        *guard = std::time::Instant::now();
                    }

                    // Otherwise share state in one of the next blocks if we have not marked one block yet
                    if let Some(block_seq_no) = is_state_sync_requested.guarded(|e| *e) {
                        // Note: error handling logic was changed here
                        if self.send_sync_from(node_id, &thread_id, block_seq_no).is_err() {
                            tracing::error!("Reciever has gone"); // TODO: Add error metric here?
                        }
                    } else {
                        // Note: error handling logic was changed here
                        if let Some((_, mut block_seq_no_with_sync)) = self
                            .repository
                            .select_thread_last_finalized_block(&thread_id)
                            .expect("Must be known here")
                        {
                            for _i in 0..self.config.global.sync_gap {
                                block_seq_no_with_sync = next_seq_no(block_seq_no_with_sync);
                            }
                            tracing::trace!(
                                "Mark next block to share state: {block_seq_no_with_sync:?}"
                            );

                            // Note: error handling logic was changed here
                            if self
                                .send_sync_from(node_id, &thread_id, block_seq_no_with_sync)
                                .is_err()
                            {
                                // This error will terminate the `block_request_service` process
                                anyhow::bail!(
                                    "Failed to execute send_sync_from, reciever had gone"
                                );
                            } else {
                                is_state_sync_requested
                                    .guarded_mut(|e| *e = Some(block_seq_no_with_sync));
                            }
                        } else {
                            tracing::error!("Thread {} not found", thread_id);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn inner(
        &self,
        from_inclusive: BlockSeqNo,
        to_exclusive: BlockSeqNo,
        node_id: NodeIdentifier,
        thread_id: &ThreadIdentifier,
        at_least_n_blocks: Option<usize>,
    ) -> anyhow::Result<()> {
        tracing::trace!(
            "on_incoming_block_request from {node_id}: {:?} - {:?}",
            from_inclusive,
            to_exclusive
        );

        self.shared_services.throttle(&node_id)?;

        let mut at_least_n_blocks = at_least_n_blocks.unwrap_or_default();
        let (last_finalized_block_id, last_finalized_block_seq_no) = self
            .repository
            .select_thread_last_finalized_block(thread_id)?
            .expect("Must be known here");
        #[allow(clippy::mutable_key_type)]
        let unprocessed_blocks_cache = self.unprocessed_blocks_cache.clone_queue();
        if last_finalized_block_seq_no > from_inclusive {
            let count = self.resend_finalized(
                thread_id,
                node_id.clone(),
                last_finalized_block_id.clone(),
                from_inclusive,
            )?;
            at_least_n_blocks = at_least_n_blocks.saturating_sub(count);
        }
        #[allow(clippy::mutable_key_type)]
        let mut demanded = HashSet::<BlockState>::new();
        if at_least_n_blocks > 0 {
            let mut generation = vec![last_finalized_block_id];
            for _ in 0..at_least_n_blocks {
                let mut next_generation = vec![];
                for block_id in generation.iter() {
                    let block = self.block_state_repository.get(block_id)?;
                    if block.guarded(|e| e.is_invalidated()) {
                        // Skip.
                        continue;
                    }
                    block.guarded(|e| e.known_children(thread_id).cloned()).inspect(
                        |descendants| {
                            next_generation.extend(descendants.iter().cloned());
                        },
                    );
                }
                for block_id in next_generation.iter() {
                    let block = self.block_state_repository.get(block_id)?;
                    if block.guarded(|e| !e.is_invalidated()) {
                        demanded.insert(block);
                    }
                }
                generation = next_generation;
            }
        }
        let cached = unprocessed_blocks_cache.into_iter().filter_map(|(_, (e, _))| {
            if demanded.contains(&e) {
                return Some(e.block_identifier().clone());
            }
            let (block_id, Some(seq_no)) =
                e.guarded(|x| (x.block_identifier().clone(), *x.block_seq_no()))
            else {
                return None;
            };
            if seq_no >= from_inclusive && seq_no < to_exclusive {
                Some(block_id)
            } else {
                None
            }
        });
        let cached = cached
            .map(|e| {
                let Some(block) = self.repository.get_finalized_block(&e)? else {
                    anyhow::bail!("too far into the history (None block)");
                };
                Ok(block)
            })
            .collect::<Result<Vec<Arc<<Self as NodeAssociatedTypes>::CandidateBlock>>, _>>()?;
        self.resend(thread_id, node_id.clone(), cached)?;
        Ok(())
    }

    fn send_sync_from(
        &self,
        node_id: NodeIdentifier,
        thread_id: &ThreadIdentifier,
        from_seq_no: BlockSeqNo,
    ) -> anyhow::Result<()> {
        tracing::info!("sending syncFrom to node {}: {:?}", node_id, from_seq_no,);
        self.network_direct_tx
            .send((node_id, NetworkMessage::SyncFrom((from_seq_no, *thread_id))))?;
        Ok(())
    }

    // Returns number of blocks sent
    fn resend_finalized(
        &self,
        thread_id: &ThreadIdentifier,
        destination_node_id: NodeIdentifier,
        tail: BlockIdentifier,
        cutoff: BlockSeqNo,
    ) -> anyhow::Result<usize> {
        const MAX_TO_RESEND: usize = 200;
        let mut cache = vec![];
        let mut cursor = tail;
        loop {
            if cache.len() > MAX_TO_RESEND {
                anyhow::bail!("too far into the history (MAX_TO_RESEND)");
            }
            let (Some(block_seq_no), Some(parent)) = self
                .block_state_repository
                .get(&cursor)?
                .guarded(|e| (*e.block_seq_no(), e.parent_block_identifier().clone()))
            else {
                anyhow::bail!("too far into the history (None state)");
            };
            let Some(block) = self.repository.get_finalized_block(&cursor)? else {
                anyhow::bail!("too far into the history (None block)");
            };
            cache.push(block);
            if cutoff > block_seq_no {
                return self.resend(thread_id, destination_node_id, cache);
            }
            cursor = parent;
        }
    }

    // Returns number of blocks sent
    fn resend(
        &self,
        thread_id: &ThreadIdentifier,
        destination: NodeIdentifier,
        blocks: Vec<Arc<<Self as NodeAssociatedTypes>::CandidateBlock>>,
    ) -> anyhow::Result<usize> {
        self.metrics.as_ref().inspect(|m| m.report_resend(thread_id));
        let n = blocks.len();
        for block in blocks.into_iter() {
            self.send_candidate_block(block.as_ref().clone(), destination.clone())?;
        }
        Ok(n)
    }

    pub(crate) fn send_candidate_block(
        &self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
        node_id: NodeIdentifier,
    ) -> anyhow::Result<()> {
        tracing::info!("sending block to node {node_id}:{}", candidate_block.data());
        block_flow_trace(
            "direct sending candidate",
            &candidate_block.data().identifier(),
            &self.config.local.node_id,
            [("to", &node_id.to_string())],
        );
        self.network_direct_tx.send((node_id, NetworkMessage::candidate(&candidate_block)?))?;
        Ok(())
    }
}
