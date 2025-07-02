use std::collections::HashMap;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;

use super::action_lock::Authority;
use crate::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;

pub struct ChainPulseMonitor {
    _handler: JoinHandle<()>,
    monitor: Sender<ChainPulseEvent>,
}

impl ChainPulseMonitor {
    pub fn monitor(&mut self) -> Sender<ChainPulseEvent> {
        self.monitor.clone()
    }
}

fn move_deadline(
    deadlines: &mut HashMap<ThreadIdentifier, Instant>,
    thread_identifier: &ThreadIdentifier,
    next_deadline: Instant,
) {
    deadlines
        .entry(*thread_identifier)
        .and_modify(|e| {
            if next_deadline > *e {
                *e = next_deadline;
            }
        })
        .or_insert(next_deadline);
}

pub fn bind(authority: Arc<Mutex<Authority>>) -> ChainPulseMonitor {
    let (tx, rx) = std::sync::mpsc::channel();
    let chain_pulse_binding = std::thread::Builder::new()
        .name("ChainPulse to Authority binding".to_string())
        .spawn_critical(move || {
            let mut deadlines = HashMap::<ThreadIdentifier, Instant>::new();
            let mut block_collections = HashMap::new();
            loop {
                let next_deadline = deadlines.values().fold(None, |min, value| {
                    if let Some(deadline) = min {
                        if deadline > value {
                            Some(value)
                        } else {
                            min
                        }
                    } else {
                        Some(value)
                    }
                });
                let next_event = {
                    if let Some(deadline) = next_deadline {
                        let now = Instant::now();
                        if *deadline > now {
                            match rx.recv_timeout(*deadline - now) {
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
                        let mut stalled = vec![];
                        for (thread, deadline) in deadlines.iter_mut() {
                            if now >= *deadline {
                                stalled.push(*thread);
                                // TODO:
                                // It requires to send chain events on new block candidates,
                                // so this monitor would know that BP has not stalled.
                            }
                        }
                        authority.guarded_mut(|e| {
                            for thread in stalled.iter() {
                                let result = e.on_block_producer_stalled(thread);
                                if let Some(next_round) = result.next_round().clone() {
                                    let blocks = block_collections
                                        .get(thread)
                                        .cloned()
                                        .expect("Unexpected stalled thread");
                                    e.on_next_round_incoming_request(next_round, blocks);
                                }
                                deadlines.insert(*thread, *result.next_deadline());
                            }
                        });
                    }
                    Some(ChainPulseEvent::BlockFinalized(e)) => {
                        // TODO: config
                        move_deadline(
                            &mut deadlines,
                            e.thread_identifier(),
                            now + Duration::from_secs(3),
                        );
                    }
                    Some(ChainPulseEvent::BlockPrefinalized(e)) => {
                        // TODO: config
                        move_deadline(
                            &mut deadlines,
                            e.thread_identifier(),
                            now + Duration::from_secs(3),
                        );
                    }
                    Some(ChainPulseEvent::StartThread { thread_id, block_candidates }) => {
                        block_collections.insert(thread_id, block_candidates);
                        move_deadline(&mut deadlines, &thread_id, now + Duration::from_millis(330));
                    }
                }
            }
        })
        .expect("Failed to create thread");
    ChainPulseMonitor { _handler: chain_pulse_binding, monitor: tx }
}
