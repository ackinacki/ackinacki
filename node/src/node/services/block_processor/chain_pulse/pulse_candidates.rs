use std::time::Duration;
use std::time::Instant;

use network::channel::NetBroadcastSender;
use typed_builder::TypedBuilder;

use crate::node::associated_types::NodeIdentifier;
use crate::node::unprocessed_blocks_collection::UnfinalizedBlocksSnapshot;
use crate::node::NetworkMessage;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

#[derive(TypedBuilder)]
pub struct PulseCandidateBlocks {
    node_id: NodeIdentifier,
    thread_identifier: ThreadIdentifier,

    broadcast_tx: NetBroadcastSender<NetworkMessage>,

    #[builder(setter(skip), default=None)]
    last_finalized_block: Option<BlockSeqNo>,

    #[builder(setter(skip), default_code = "Instant::now()")]
    last_pulse: Instant,

    #[builder(setter(skip), default_code = "Instant::now()")]
    last_broadcast_timestamp: Instant,

    resend_timeout: Duration,
    resend_extra_timeout_per_candidate: Duration,

    trigger_by_finalization_stopped_timer: Duration,
    trigger_by_no_finalized_since_start_timer: Duration,
}

impl PulseCandidateBlocks {
    pub fn pulse(&mut self, last_finalized_block: BlockSeqNo) -> anyhow::Result<()> {
        if let Some(ref prev) = self.last_finalized_block {
            if &last_finalized_block == prev {
                return Ok(());
            }
            anyhow::ensure!(last_finalized_block > prev);
        }
        self.last_finalized_block = Some(last_finalized_block);
        self.last_pulse = Instant::now();
        // drop timers and ranges
        self.last_broadcast_timestamp = Instant::now();
        Ok(())
    }

    #[allow(clippy::mutable_key_type)]
    pub fn evaluate(
        &mut self,
        candidates: &UnfinalizedBlocksSnapshot,
        blocks_repository: &RepositoryImpl,
    ) -> anyhow::Result<()> {
        let is_triggering = {
            // Dump simple implementation. One of two conditions:
            // - This node was finalizing blocks and had stopped (short timeout)
            // - Was not able to receive new state and not actively syncronizing (large timeout)
            let short_timeout_trigger = if self.last_finalized_block.is_some() {
                self.last_pulse.elapsed() > self.trigger_by_finalization_stopped_timer
            } else {
                false
            };
            let large_timeout_trigger =
                { self.last_pulse.elapsed() > self.trigger_by_no_finalized_since_start_timer };
            short_timeout_trigger || large_timeout_trigger
        };
        if !is_triggering || self.last_broadcast_timestamp.elapsed() < self.resend_timeout {
            return Ok(());
        }
        // Do broadcast now.
        // TODO: fix naive implementation. It sends all messages now.
        // should select only needed.
        // setting last_broadcast_timestamp twice. Once before messages sent
        // and at the end with an extra buffer (offset by the number of messages).
        self.last_broadcast_timestamp = Instant::now();
        let (finalized_block_id, _) = blocks_repository
            .select_thread_last_finalized_block(&self.thread_identifier)?
            .expect("Must be known here");
        let Some(finalized_block) = blocks_repository.get_block(&finalized_block_id)? else {
            tracing::error!("Failed to load finalized block");
            return Ok(());
        };
        let mut sent_cnt = 1;
        tracing::info!("rebroadcasting block: {}", finalized_block,);
        let message = NetworkMessage::resent_candidate(&finalized_block, self.node_id.clone())?;
        let _ = self.broadcast_tx.send(message);
        for (x, block) in candidates.values() {
            x.guarded(|e| {
                if e.is_stored() && !e.is_invalidated() {
                    sent_cnt += 1;
                    tracing::info!("rebroadcasting block: {}", block,);
                    if let Ok(message) =
                        NetworkMessage::resent_candidate(block, self.node_id.clone())
                    {
                        let _ = self.broadcast_tx.send(message);
                    }
                }
            })
        }
        self.last_broadcast_timestamp =
            Instant::now() + self.resend_extra_timeout_per_candidate.saturating_mul(sent_cnt);
        //
        Ok(())
    }
}
