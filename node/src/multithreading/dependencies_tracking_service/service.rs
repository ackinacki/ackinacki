// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use anyhow::bail;

use super::context::DependenciesTrackingServiceContext;
use super::context::Event as DependenciesTrackingEvent;
use crate::bls::GoshBLS;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

// Note: why I don't like this solution.
// It requires to update all optimistic states for all blocks.
// This means single finalized block would require an update in 6 * 100 states.
// Not good at all. One thing is obvious - it must be stored in a separate object
// rather than together in the optimistic state.
// There's also a problem with the state durability. The node may require full
// state resync in case this state fails to be saved on disk.
// Is there alternatives? Not that I can think of right now. Might come up with
// something later. Yet, this solution should work ok'ish for now

pub type Event = DependenciesTrackingEvent;

#[derive(Clone)]
pub enum Command {
    Stop,
    BlockFinalized(AckiNackiBlock<GoshBLS>),
    BlockInvalidated(AckiNackiBlock<GoshBLS>),
    AddCandidateBlock(AckiNackiBlock<GoshBLS>),
}

pub struct DependenciesTrackingService {
    worker: JoinHandle<anyhow::Result<()>>,
    context: Arc<Mutex<DependenciesTrackingServiceContext>>,
}

impl DependenciesTrackingService {
    pub fn start(control: Receiver<Command>) -> Self {
        let service = DependenciesTrackingServiceContext::new();
        let service = Arc::new(Mutex::new(service));
        let service_clone = service.clone();
        let worker = std::thread::Builder::new()
            .name("Dependencies tracking service".to_string())
            .spawn(move || Self::execute(service_clone, control))
            .expect("Failed to start DependenciesTrackingService worker");
        Self { context: service, worker }
    }

    pub fn join(self) -> anyhow::Result<()> {
        match self.worker.join() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => {
                let msg = match e.downcast_ref::<&'static str>() {
                    Some(msg) => *msg,
                    None => match e.downcast_ref::<String>() {
                        Some(msg) => &msg[..],
                        None => "Unknown error type",
                    },
                }
                .to_owned();
                bail!(msg)
            }
        }
    }

    pub fn subscribe(&mut self, subscriber: Sender<Event>) {
        if let Ok(ref mut service) = self.context.lock() {
            service.subscribe(subscriber);
        } else {
            todo!();
        }
    }

    pub fn can_reference(
        &self,
        parent: BlockIdentifier,
        thread: ThreadIdentifier,
        references: Vec<BlockIdentifier>,
    ) -> bool {
        if let Ok(ref mut service) = self.context.lock() {
            service.can_reference(parent, thread, references)
        } else {
            todo!();
        }
    }

    fn execute(
        context: Arc<Mutex<DependenciesTrackingServiceContext>>,
        control: Receiver<Command>,
    ) -> anyhow::Result<()> {
        use Command::*;
        loop {
            match control.recv() {
                Err(RecvError) | Ok(Stop) => break,
                Ok(e) => {
                    if let Ok(ref mut service) = context.lock() {
                        match e {
                            Stop => break,
                            BlockFinalized(block) => service.on_block_finalized(&block)?,
                            BlockInvalidated(block) => service.on_block_invalidated(&block)?,
                            AddCandidateBlock(block) => service.append(&block)?,
                        };
                    } else {
                        todo!();
                    }
                }
            };
        }
        Ok(())
    }
}
