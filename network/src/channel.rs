use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;
use std::time::Instant;

use serde::Serialize;

use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::MessageDelivery;
use crate::pub_sub::connection::OutgoingMessage;
use crate::DeliveryPhase;
use crate::SendMode;

#[derive(Clone)]
pub struct NetDirectSender<PeerId, Message> {
    inner: tokio::sync::mpsc::UnboundedSender<(PeerId, NetMessage, Instant)>,
    metrics: Option<NetMetrics>,
    self_peer_id: PeerId,
    _message_type: PhantomData<Message>,
}

pub enum NetSendError<Message> {
    Disconnected(Message),
    EncodeFailed(Message, anyhow::Error),
}

impl<T> NetSendError<T> {
    pub fn message(&self) -> String {
        match self {
            NetSendError::Disconnected(_) => "Sending on a closed channel".to_string(),
            NetSendError::EncodeFailed(_, err) => format!("Encoding failed: {err}"),
        }
    }
}

impl<T> Debug for NetSendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl<Message> std::fmt::Display for NetSendError<Message> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl<Message> std::error::Error for NetSendError<Message> {}

fn encode_outgoing<Message: Debug + Serialize>(
    message: Message,
) -> Result<(Message, NetMessage, usize), NetSendError<Message>> {
    match NetMessage::encode(&message) {
        Ok((wrapped, uncompressed_size)) => Ok((message, wrapped, uncompressed_size)),
        Err(err) => {
            tracing::error!("Error serializing outgoing {message:#?}: {}", err);
            Err(NetSendError::EncodeFailed(message, err))
        }
    }
}

impl<PeerId, Message> NetDirectSender<PeerId, Message>
where
    Message: Debug + serde::Serialize + Send + Sync + Clone + 'static,
    PeerId: PartialEq + ToString + Clone,
{
    pub(crate) fn new(
        inner: tokio::sync::mpsc::UnboundedSender<(PeerId, NetMessage, Instant)>,
        metrics: Option<NetMetrics>,
        self_peer_id: PeerId,
    ) -> Self {
        Self { inner, metrics, self_peer_id, _message_type: PhantomData }
    }

    pub fn send(&self, args: (PeerId, Message)) -> Result<(), NetSendError<Message>> {
        let (peer_id, outgoing) = args;
        if peer_id == self.self_peer_id {
            tracing::trace!("Skip message to self");
            return Ok(());
        }

        let (original, net_message, orig_size) = encode_outgoing(outgoing)?;

        let id = net_message.id.clone();
        let label = net_message.label.clone();

        self.metrics.as_ref().inspect(|metrics| {
            metrics.report_message_size(orig_size, net_message.data.len(), &label);
            metrics.start_delivery_phase(
                DeliveryPhase::OutgoingBuffer,
                1,
                &net_message.label,
                SendMode::Direct,
            );
        });

        tracing::debug!(
            peer_id = peer_id.to_string(),
            msg_id = net_message.id,
            msg_type = label,
            broadcast = false,
            "Message delivery: started"
        );
        match self.inner.send((peer_id.clone(), net_message, Instant::now())) {
            Ok(()) => Ok(()),
            Err(_) => {
                tracing::error!(
                    peer_id = peer_id.to_string(),
                    msg_id = id,
                    msg_type = label,
                    broadcast = false,
                    "Message delivery: forwarding to peer dispatcher failed"
                );
                self.metrics.as_ref().inspect(|x| {
                    x.finish_delivery_phase(
                        DeliveryPhase::OutgoingBuffer,
                        1,
                        &label,
                        SendMode::Direct,
                        Duration::from_millis(0),
                    );
                });
                Err(NetSendError::Disconnected(original))
            }
        }
    }
}

#[derive(Clone)]
pub struct NetBroadcastSender<Message> {
    inner: tokio::sync::broadcast::Sender<OutgoingMessage>,
    metrics: Option<NetMetrics>,
    _message_type: PhantomData<Message>,
}

impl<Message> NetBroadcastSender<Message>
where
    Message: Debug + serde::Serialize + Send + Sync + Clone + 'static,
{
    pub(crate) fn new(
        inner: tokio::sync::broadcast::Sender<OutgoingMessage>,
        metrics: Option<NetMetrics>,
    ) -> Self {
        Self { inner, metrics, _message_type: PhantomData }
    }

    pub fn send(&self, message: Message) -> Result<(), NetSendError<Message>> {
        let (_, net_message, orig_size) = encode_outgoing(message)?;
        let label = net_message.label.clone();

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.report_message_size(orig_size, net_message.data.len(), &label);
        }

        tracing::debug!(
            msg_id = net_message.id,
            msg_type = label,
            broadcast = true,
            "Message delivery: started"
        );

        let label = net_message.label.clone();
        match self.inner.send(OutgoingMessage {
            message: net_message,
            delivery: MessageDelivery::Broadcast,
            duration_before_transfer: Instant::now(),
        }) {
            Ok(receiver_count) => {
                if receiver_count > 0 {
                    self.metrics.as_ref().inspect(|x| {
                        x.start_delivery_phase(
                            DeliveryPhase::OutgoingBuffer,
                            receiver_count,
                            &label,
                            SendMode::Broadcast,
                        )
                    });
                    tracing::trace!(
                        receiver_count = receiver_count,
                        "Forwarded broadcast {}",
                        label
                    );
                }
            }
            Err(_) => {
                tracing::warn!("There are no broadcast senders. {} was not forwarded.", label);
            }
        }
        Ok(())
    }
}
