use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use crate::helper::metrics::BlockProductionMetrics;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::storage::MessageDurableStorage;
use crate::types::AccountAddress;

#[derive(Clone)]
pub struct MessageDBWriterService {
    sender: std::sync::mpsc::Sender<
        HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    >,
    handler: Arc<JoinHandle<anyhow::Result<()>>>,
}

impl MessageDBWriterService {
    pub fn new(
        message_db: MessageDurableStorage,
        metrics: Option<BlockProductionMetrics>,
    ) -> anyhow::Result<Self> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let handler = std::thread::Builder::new().name("MessageDBWriterService".to_owned()).spawn(
            move || loop {
                match receiver.recv() {
                    Ok(messages) => {
                        let moment = Instant::now();

                        let mut handles = vec![];
                        for (address, messages_vec) in messages {
                            let messages_vec: Vec<(MessageIdentifier, Arc<WrappedMessage>)> =
                                messages_vec;
                            let message_db = message_db.clone();
                            let handle = std::thread::Builder::new()
                                .name("MessageDBWriterService_thread_inner".to_owned())
                                .spawn(move || {
                                    // We will start a new std::thread for every address.
                                    // TBD: control number of threads
                                    let one_address_messages =
                                        HashMap::from([(address, messages_vec)]);
                                    message_db.write_messages(one_address_messages)?;
                                    anyhow::Ok(())
                                });
                            handles.push(handle);
                        }

                        for handle in handles {
                            match handle {
                                Ok(handle) => {
                                    match handle.join() {
                                        Ok(Ok(())) => {} // Success
                                        Ok(Err(err)) => {
                                            // Write errors are tolerated; metrics track their count, so just logging is enough.
                                            tracing::error!("write_messages: {err}");
                                        }
                                        Err(panic) => {
                                            tracing::error!("Thread panicked: {panic:?}");
                                            break;
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::error!("Failed to spawn thread: {err:?}");
                                    break;
                                }
                            }
                        }

                        metrics.as_ref().inspect(|m| {
                            m.report_aerospike_messages_write_busy(
                                moment.elapsed().as_millis() as u64
                            );
                        });
                    }
                    _ => {
                        tracing::error!("MessageDBWriterService receiver thread terminated");
                    }
                }
            },
        )?;
        Ok(Self { sender, handler: Arc::new(handler) })
    }

    pub fn write(
        &self,
        messages: HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!self.handler.is_finished(), "Message storage writer should not stop");
        self.sender.send(messages)?;
        Ok(())
    }
}
