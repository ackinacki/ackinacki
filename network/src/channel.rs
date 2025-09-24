use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

use serde::Serialize;

use crate::direct_sender::DirectReceiver;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::MessageDelivery;
use crate::pub_sub::connection::OutgoingMessage;
use crate::DeliveryPhase;
use crate::SendMode;

#[derive(Clone)]
pub struct NetDirectSender<PeerId, Message>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    inner: tokio::sync::mpsc::UnboundedSender<(DirectReceiver<PeerId>, NetMessage, Instant)>,
    metrics: Option<NetMetrics>,
    self_peer_id: PeerId,
    self_addr: SocketAddr,
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
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        inner: tokio::sync::mpsc::UnboundedSender<(DirectReceiver<PeerId>, NetMessage, Instant)>,
        metrics: Option<NetMetrics>,
        self_peer_id: PeerId,
        self_addr: SocketAddr,
    ) -> Self {
        Self { inner, metrics, self_peer_id, self_addr, _message_type: PhantomData }
    }

    pub fn send(
        &self,
        args: (DirectReceiver<PeerId>, Message),
    ) -> Result<(), NetSendError<Message>> {
        let (receiver, outgoing) = args;
        if receiver == DirectReceiver::Peer(self.self_peer_id.clone())
            || receiver == DirectReceiver::Addr(self.self_addr)
        {
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
            peer_id = receiver.to_string(),
            msg_id = net_message.id,
            msg_type = label,
            broadcast = false,
            "Message delivery: started"
        );

        let message_aprox_size = NetMessage::transfer_size(&net_message);

        match self.inner.send((receiver.clone(), net_message, Instant::now())) {
            Ok(()) => {
                self.metrics.as_ref().inspect(|metrics| {
                    metrics.report_sent_to_outgoing_buffer_bytes(
                        message_aprox_size,
                        &label,
                        SendMode::Direct,
                    );
                });
                Ok(())
            }
            Err(_) => {
                tracing::error!(
                    peer_id = receiver.to_string(),
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

    pub fn send(&self, message: Message) -> Result<usize, NetSendError<Message>> {
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
        let message_aprox_size = NetMessage::transfer_size(&net_message);

        let received_count = match self.inner.send(OutgoingMessage {
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
                        );

                        x.report_sent_to_outgoing_buffer_bytes(
                            message_aprox_size * receiver_count as u64,
                            &label,
                            SendMode::Broadcast,
                        );
                    });
                    tracing::trace!(
                        receiver_count = receiver_count,
                        "Forwarded broadcast {}",
                        label
                    );
                }
                receiver_count
            }
            Err(_) => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.report_warn("no_brd_senders");
                }
                tracing::warn!("There are no broadcast senders. {} was not forwarded.", label);
                0
            }
        };
        Ok(received_count)
    }
}
