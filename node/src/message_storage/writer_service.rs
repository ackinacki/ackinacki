use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::message_storage::MessageDurableStorage;
use crate::types::AccountAddress;

#[derive(Clone)]
pub struct MessageDBWriterService {
    sender: std::sync::mpsc::Sender<
        HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    >,
    handler: Arc<JoinHandle<anyhow::Result<()>>>,
}

impl MessageDBWriterService {
    pub fn new(message_db: MessageDurableStorage) -> anyhow::Result<Self> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let handler = std::thread::Builder::new().name("MessageDBWriterService".to_owned()).spawn(
            move || loop {
                match receiver.recv() {
                    Ok(messages) => {
                        let messages: HashMap<
                            AccountAddress,
                            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
                        > = messages;
                        tracing::trace!(
                            "MessageDBWriterService received messages: {}",
                            messages.len()
                        );
                        message_db.write_messages(messages)?;
                        // for (addr, messages) in messages {
                        //     let dst_str = addr.0.to_hex_string();
                        //     for (message_id, message) in messages {
                        //         let message_blob = bincode::serialize(&message)?;
                        //         let message_hash_str = message_id.inner().hash.to_hex_string();
                        //         message_db.write_message(
                        //             &dst_str,
                        //             &message_blob,
                        //             &message_hash_str,
                        //         )?;
                        //     }
                        // }
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
