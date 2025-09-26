use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use derive_getters::Getters;
use parking_lot::Mutex;
use typed_builder::TypedBuilder;

use super::action_lock::Authority;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use crate::types::BlockHeight;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;

#[derive(TypedBuilder, Clone, Getters)]
pub struct Deadline {
    block_height: Option<BlockHeight>,
    timestamp: Instant,
}

pub struct ChainPulseMonitor {
    _handler: JoinHandle<()>,
    monitor: Sender<ChainPulseEvent>,
    stalled_threads: Arc<Mutex<HashSet<ThreadIdentifier>>>,
}

impl AllowGuardedMut for HashSet<ThreadIdentifier> {}

impl ChainPulseMonitor {
    pub fn monitor(&mut self) -> Sender<ChainPulseEvent> {
        self.monitor.clone()
    }

    pub fn stalled_threads(&self) -> Arc<Mutex<HashSet<ThreadIdentifier>>> {
        self.stalled_threads.clone()
    }
}

fn move_deadline(
    deadlines: &mut HashMap<ThreadIdentifier, Deadline>,
    thread_identifier: &ThreadIdentifier,
    next_deadline: Deadline,
) {
    deadlines
        .entry(*thread_identifier)
        .and_modify(|e| {
            if (next_deadline.block_height() != e.block_height()
                && next_deadline.block_height().is_some())
                || (next_deadline.timestamp() > e.timestamp())
            {
                *e = next_deadline.clone();
            }
        })
        .or_insert(next_deadline);
}

pub fn bind(authority: Arc<Mutex<Authority>>) -> ChainPulseMonitor {
    let (tx, rx) = std::sync::mpsc::channel();
    let stalled_threads = Arc::new(Mutex::new(HashSet::new()));
    let stalled_threads_clone = stalled_threads.clone();
    let chain_pulse_binding = std::thread::Builder::new()
        .name("ChainPulse to Authority binding".to_string())
        .spawn_critical(move || {
            let mut deadlines = HashMap::<ThreadIdentifier, Deadline>::new();
            let mut block_collections = HashMap::new();
            loop {
                if SHUTDOWN_FLAG.get() == Some(&true) {
                    return Ok(());
                }
                let next_deadline =
                    deadlines.values().fold(None, |min: Option<Deadline>, value| {
                        if let Some(deadline) = min.clone() {
                            if deadline.timestamp() > value.timestamp() {
                                Some(value.clone())
                            } else {
                                min
                            }
                        } else {
                            Some(value.clone())
                        }
                    });
                let next_event = {
                    if let Some(deadline) = next_deadline {
                        let now = Instant::now();
                        if *deadline.timestamp() > now {
                            match rx.recv_timeout(*deadline.timestamp() - now) {
                                Ok(e) => Some(e),
                                Err(RecvTimeoutError::Timeout) => None,
                                Err(RecvTimeoutError::Disconnected) => {
                                    unreachable!();
                                }
                            }
                        } else {
                            None
                        }
                    } else {
                        match rx.recv() {
                            Ok(e) => Some(e),
                            Err(_e) => unreachable!(),
                        }
                    }
                };
                let now = Instant::now();
                match next_event {
                    None => {
                        let mut stalled = HashSet::new();
                        for (thread, deadline) in deadlines.iter_mut() {
                            if now >= *deadline.timestamp() {
                                stalled.insert(*thread);
                                // TODO:
                                // It requires to send chain events on new block candidates,
                                // so this monitor would know that BP has not stalled.
                            }
                        }
                        stalled_threads_clone.guarded_mut(|e| *e = stalled.clone());
                        for thread_id in stalled.iter() {
                            authority
                                .guarded_mut(|e| e.get_thread_authority(thread_id))
                                .guarded_mut(|e| {
                                    let result = e.on_block_producer_stalled();
                                    tracing::trace!("on_block_producer_stalled result: {result:?}");
                                    if let Some(next_round) = result.next_round().clone() {
                                        let blocks = block_collections
                                            .get(thread_id)
                                            .cloned()
                                            .expect("Unexpected stalled thread");
                                        e.on_next_round_incoming_request(next_round, None, blocks);
                                    }
                                    deadlines.insert(
                                        *thread_id,
                                        Deadline::builder()
                                            .timestamp(*result.next_deadline())
                                            .block_height(*result.block_height())
                                            .build(),
                                    );
                                })
                        }
                    }
                    Some(ChainPulseEvent::BlockFinalized(e)) => {
                        stalled_threads_clone.guarded_mut(|set| set.remove(e.thread_identifier()));
                        // TODO: config
                        move_deadline(
                            &mut deadlines,
                            e.thread_identifier(),
                            Deadline::builder()
                                .timestamp(now + 2 * Duration::from_millis(330))
                                .block_height(*e.block_height())
                                .build(),
                        );
                    }
                    Some(ChainPulseEvent::BlockPrefinalized(e)) => {
                        stalled_threads_clone.guarded_mut(|set| set.remove(e.thread_identifier()));
                        // TODO: config
                        move_deadline(
                            &mut deadlines,
                            e.thread_identifier(),
                            Deadline::builder()
                                .timestamp(now + 2 * Duration::from_millis(330))
                                .block_height(*e.block_height())
                                .build(),
                        );
                    }
                    Some(ChainPulseEvent::StartThread { thread_id, block_candidates }) => {
                        stalled_threads_clone.guarded_mut(|set| set.remove(&thread_id));
                        block_collections.insert(thread_id, block_candidates);
                        move_deadline(
                            &mut deadlines,
                            &thread_id,
                            Deadline::builder()
                                .timestamp(now + 2 * Duration::from_millis(330))
                                .block_height(None)
                                .build(),
                        );
                    }
                    Some(ChainPulseEvent::BlockApplied(e)) => {
                        stalled_threads_clone.guarded_mut(|set| set.remove(e.thread_identifier()));
                        // TODO: config
                        move_deadline(
                            &mut deadlines,
                            e.thread_identifier(),
                            Deadline::builder()
                                .timestamp(now + 2 * Duration::from_millis(330))
                                .block_height(*e.block_height())
                                .build(),
                        );
                    }
                }
            }
        })
        .expect("Failed to create thread");
    ChainPulseMonitor { _handler: chain_pulse_binding, monitor: tx, stalled_threads }
}
