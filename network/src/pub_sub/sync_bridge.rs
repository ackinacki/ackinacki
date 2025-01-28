use std::sync::Arc;

use crate::pub_sub::IncomingMessage;
use crate::pub_sub::MessageDelivery;
use crate::pub_sub::OutgoingMessage;

// Read outgoing message from app's sync channel,
// serialize it and send to the transport's async channel
pub fn outgoing_bridge<T>(
) -> (std::sync::mpsc::Sender<T>, tokio::sync::broadcast::Sender<OutgoingMessage>)
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    let (outgoing_tx, outgoing_rx) = std::sync::mpsc::channel::<T>();
    let (outgoing_transport_tx, _) = tokio::sync::broadcast::channel::<OutgoingMessage>(1000);
    let sender = outgoing_transport_tx.clone();
    tokio::task::spawn_blocking(move || loop {
        match outgoing_rx.recv() {
            Ok(message) => {
                if let Ok(data) = bincode::serialize(&message).inspect_err(|err| {
                    tracing::error!("Error serializing outgoing message: {}", err);
                }) {
                    match sender.send(OutgoingMessage {
                        data: Arc::new(data),
                        delivery: MessageDelivery::Broadcast,
                    }) {
                        Ok(receiver_count) => {
                            tracing::trace!(
                                "Forwarded broadcast message to {} sender(s)",
                                receiver_count
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                "There are no broadcast senders. Message was not forwarded."
                            );
                        }
                    }
                }
            }
            Err(_) => {
                tracing::info!("Finished forwarding broadcast messages");
                break;
            }
        }
    });
    (outgoing_tx, outgoing_transport_tx)
}

// Read incoming message from the transport's tokio channel,
// deserialize it and send to the app's std channel
pub fn incoming_bridge<T>(
    incoming_tx: std::sync::mpsc::Sender<T>,
) -> tokio::sync::mpsc::Sender<IncomingMessage>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    let (transport_tx, mut transport_rx) = tokio::sync::mpsc::channel::<IncomingMessage>(1000);
    tokio::task::spawn(async move {
        loop {
            match transport_rx.recv().await {
                Some(message) => {
                    tracing::info!("Received incoming message");
                    if let Ok(data) = bincode::deserialize(&message.data).inspect_err(|err| {
                        tracing::error!("Error deserializing incoming message: {}", err);
                    }) {
                        if incoming_tx.send(data).is_err() {
                            tracing::info!("Finished receiving incoming messages");
                            break;
                        }
                    }
                }
                None => {
                    tracing::info!("Finished receiving incoming messages");
                    break;
                }
            }
        }
    });
    transport_tx
}
