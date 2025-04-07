use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;
use std::time::Instant;

use serde::Serialize;

use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::connection::MessageDelivery;
use crate::pub_sub::connection::OutgoingMessage;
use crate::DeliveryPhase;
use crate::SendMode;

#[derive(Clone)]
pub struct NetDirectSender<PeerId, Message> {
    inner: tokio::sync::mpsc::UnboundedSender<(PeerId, NetMessage, Instant)>,
    metrics: Option<NetMetrics>,
    self_id: PeerId,
    _message_type: PhantomData<Message>,
}

pub enum NetSendError<Message> {
    Disconnected(Message),
    EncodeFailed(Message, anyhow::Error),
}

impl<T> NetSendError<T> {
    pub fn message(&self) -> &str {
        match self {
            NetSendError::Disconnected(_) => "sending on a closed channel",
            NetSendError::EncodeFailed(_, _) => "encoding failed",
        }
    }
}

impl<T> Debug for NetSendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl<Message> std::fmt::Display for NetSendError<Message> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl<Message> std::error::Error for NetSendError<Message> {
    #[allow(deprecated)]
    fn description(&self) -> &str {
        self.message()
    }
}

fn encode_outgoing<Message: Debug + Serialize>(
    message: Message,
) -> Result<(Message, NetMessage), NetSendError<Message>> {
    match NetMessage::encode(&message) {
        Ok(wrapped) => Ok((message, wrapped)),
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
        self_id: PeerId,
    ) -> Self {
        Self { inner, metrics, self_id, _message_type: PhantomData }
    }

    pub fn send(&self, args: (PeerId, Message)) -> Result<(), NetSendError<Message>> {
        let (peer_id, outgoing) = args;
        if peer_id == self.self_id {
            tracing::trace!("Skip message to self");
            return Ok(());
        }

        let (original, net_message) = encode_outgoing(outgoing)?;
        self.metrics.as_ref().inspect(|x| {
            x.start_delivery_phase(
                DeliveryPhase::OutgoingBuffer,
                1,
                &net_message.label,
                SendMode::Direct,
            );
        });
        let id = net_message.id.clone();
        let label = net_message.label.clone();
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
        let (_, net_message) = encode_outgoing(message)?;
        let label = net_message.label.clone();
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

pub enum NetRecvError {
    Disconnected,
    DecodingFailed(anyhow::Error),
}

impl From<RecvError> for NetRecvError {
    fn from(_: RecvError) -> Self {
        NetRecvError::Disconnected
    }
}

pub enum NetTryRecvError {
    Empty,
    Disconnected,
    DecodingFailed(anyhow::Error),
}

impl NetRecvError {
    fn message(&self) -> &str {
        match self {
            Self::Disconnected => "disconnected",
            Self::DecodingFailed(_) => "decoding failed",
        }
    }
}

impl From<TryRecvError> for NetTryRecvError {
    fn from(e: TryRecvError) -> Self {
        match e {
            TryRecvError::Empty => NetTryRecvError::Empty,
            TryRecvError::Disconnected => NetTryRecvError::Disconnected,
        }
    }
}

impl Debug for NetRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::fmt::Display for NetRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for NetRecvError {
    #[allow(deprecated)]
    fn description(&self) -> &str {
        self.message()
    }
}

pub struct NetReceiver<Message> {
    inner: std::sync::mpsc::Receiver<IncomingMessage>,
    metrics: Option<NetMetrics>,
    _message_type: PhantomData<Message>,
}

impl<Message> NetReceiver<Message>
where
    Message: Debug + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
{
    pub(crate) fn new(
        inner: std::sync::mpsc::Receiver<IncomingMessage>,
        metrics: Option<NetMetrics>,
    ) -> Self {
        Self { inner, metrics, _message_type: PhantomData }
    }

    pub fn recv(&self) -> Result<Message, NetRecvError> {
        self.inner
            .recv()
            .map_err(From::from)
            .and_then(|incoming| self.decode_incoming(incoming, NetRecvError::DecodingFailed))
    }

    pub fn try_recv(&self) -> Result<Message, NetTryRecvError> {
        self.inner
            .try_recv()
            .map_err(From::from)
            .and_then(|incoming| self.decode_incoming(incoming, NetTryRecvError::DecodingFailed))
    }

    fn decode_incoming<E>(
        &self,
        incoming: IncomingMessage,
        e: impl FnOnce(anyhow::Error) -> E,
    ) -> Result<Message, E> {
        tracing::debug!(
            peer_id = incoming.peer.peer_id,
            msg_id = incoming.message.id,
            msg_type = incoming.message.label,
            broadcast = incoming.peer.self_is_subscriber,
            "Message delivery: decode"
        );
        let message = match incoming.message.decode() {
            Ok(message) => message,
            Err(reason) => return Err(e(reason)),
        };
        let _ = self.metrics.as_ref().inspect(|m| {
            m.finish_delivery_phase(
                DeliveryPhase::IncomingBuffer,
                1,
                &incoming.message.label,
                incoming.peer.peer_send_mode(),
                incoming.duration_after_transfer.elapsed(),
            );
        });
        match incoming.message.delivery_duration_ms() {
            Ok(delivery_duration_ms) => {
                tracing::info!(
                    "Received incoming {}, peer {}, duration {}",
                    incoming.message.label,
                    incoming.peer.peer_info(),
                    delivery_duration_ms
                );
                let _ = self.metrics.as_ref().inspect(|m| {
                    m.report_incoming_message_delivery_duration(
                        delivery_duration_ms,
                        &incoming.message.label,
                    );
                });
            }
            Err(reason) => {
                tracing::error!("{}", reason);
                tracing::info!(
                    "Received incoming {}, peer {}, duration N/A",
                    incoming.message.label,
                    incoming.peer.peer_info(),
                );
            }
        }
        tracing::debug!(
            peer_id = incoming.peer.peer_id,
            msg_id = incoming.message.id,
            msg_type = incoming.message.label,
            broadcast = incoming.peer.self_is_subscriber,
            "Message delivery: finished"
        );
        Ok(message)
    }
}
