use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use libc::c_void;
use parking_lot::Mutex;
use parking_lot::RwLock;
use thiserror::Error;
use tracing::trace;

use super::connection::ConnectionError;
use crate::msquic::msquic_async::stream::StartError;

/// A stream represents a bidirectional or unidirectional stream.
#[derive(Debug)]
pub struct SendStream(Arc<StreamInstance>);

impl SendStream {
    pub(crate) fn open(msquic_conn: &msquic::Connection) -> Result<Self, StartError> {
        let flags = msquic::StreamOpenFlags::UNIDIRECTIONAL;
        let inner = Arc::new(StreamInner::new(StreamSendState::Closed));
        let inner_in_ev = inner.clone();
        let msquic_stream = msquic::Stream::open(msquic_conn, flags, move |stream_ref, ev| {
            inner_in_ev.callback_handler_impl(stream_ref, ev)
        })
        .map_err(StartError::OtherError)?;
        let stream_handle = unsafe { msquic_stream.as_raw() };
        trace!("Stream(Inner: {:p}, HQUIC: {:p}) Open by local", inner, stream_handle);
        Ok(Self(Arc::new(StreamInstance { inner, msquic_stream })))
    }

    pub(crate) fn debug_id(&self) -> String {
        format!("Stream(Inner: {:p}, id: {:?})", self.0.inner, self.id())
    }

    pub(crate) fn poll_start(&mut self, cx: &mut Context) -> Poll<Result<(), StartError>> {
        let mut exclusive = self.0.inner.exclusive.lock();
        trace!("Stream(Inner: {:p}) poll_start state={:?}", self.0.inner, exclusive.state);
        match exclusive.state {
            StreamState::Open => {
                let res = self
                    .0
                    .msquic_stream
                    .start(
                        msquic::StreamStartFlags::SHUTDOWN_ON_FAIL
                            | msquic::StreamStartFlags::INDICATE_PEER_ACCEPT,
                    )
                    .map_err(StartError::OtherError);
                trace!("Stream(Inner: {:p}) poll_start start={:?}", self.0.inner, res);
                res?;
                exclusive.state = StreamState::Start;
                exclusive.send_state = StreamSendState::Start;
            }
            StreamState::Start => {}
            _ => {
                return if let Some(start_status) = &exclusive.start_status {
                    if start_status.is_ok() {
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(match start_status.try_as_status_code().unwrap() {
                        msquic::StatusCode::QUIC_STATUS_STREAM_LIMIT_REACHED => {
                            StartError::LimitReached
                        }
                        msquic::StatusCode::QUIC_STATUS_ABORTED
                        | msquic::StatusCode::QUIC_STATUS_INVALID_STATE => {
                            StartError::ConnectionLost(
                                exclusive.conn_error.as_ref().expect("conn_error").clone(),
                            )
                        }
                        _ => StartError::OtherError(start_status.clone()),
                    }))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
        exclusive.start_waiters.push(cx.waker().clone());
        Poll::Pending
    }

    /// Returns the stream ID.
    pub fn id(&self) -> Option<u64> {
        self.0.id()
    }

    /// Poll to finish writing to the stream.
    pub fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), WriteError>> {
        self.0.poll_finish_write(cx)
    }

    /// Poll to abort writing to the stream.
    pub fn poll_abort(
        &mut self,
        cx: &mut Context<'_>,
        error_code: u64,
    ) -> Poll<Result<(), WriteError>> {
        self.0.poll_abort(cx, error_code)
    }

    /// Abort writing to the stream.
    pub fn abort(&mut self, error_code: u64) -> Result<(), WriteError> {
        self.0.abort(error_code)
    }

    pub async fn send(&mut self, buf: Vec<u8>) -> Result<(), WriteError> {
        self.0.send(buf).await
    }
}

#[derive(Debug)]
pub(crate) struct StreamInstance {
    inner: Arc<StreamInner>,
    msquic_stream: msquic::Stream,
}

impl StreamInstance {
    pub(crate) fn id(&self) -> Option<u64> {
        let id = { *self.inner.shared.id.read() };
        if id.is_some() {
            id
        } else {
            let res = unsafe {
                msquic::Api::get_param_auto::<u64>(
                    self.msquic_stream.as_raw(),
                    msquic::PARAM_STREAM_ID,
                )
            };
            if let Ok(id) = res {
                self.inner.shared.id.write().replace(id);
                Some(id)
            } else {
                None
            }
        }
    }

    fn check_sending(&self, exclusive: &mut StreamInnerExclusive) -> Result<(), msquic::Status> {
        while let Some(buffer) = exclusive.sending_queue.pop_back() {
            let (msquic_buffer, msquic_buffer_count) = buffer.get_buffers();
            let buffers = unsafe { std::slice::from_raw_parts(msquic_buffer, msquic_buffer_count) };
            let context = buffer.into_raw();
            match unsafe { self.msquic_stream.send(buffers, msquic::SendFlags::NONE, context) } {
                Ok(()) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn poll_send_data(&self, cx: &mut Context<'_>, seq_no: usize) -> Poll<Result<(), WriteError>> {
        let mut exclusive = self.inner.exclusive.lock();
        match exclusive.send_state {
            StreamSendState::Closed => {
                return Poll::Ready(Err(WriteError::Closed));
            }
            StreamSendState::Start => {
                exclusive.start_waiters.push(cx.waker().clone());
                return Poll::Pending;
            }
            StreamSendState::StartComplete => {}
            StreamSendState::Shutdown => {
                return Poll::Ready(Err(WriteError::Finished));
            }
            StreamSendState::ShutdownComplete => {
                return Poll::Ready(Err(if let Some(conn_error) = &exclusive.conn_error {
                    WriteError::ConnectionLost(conn_error.clone())
                } else if let Some(error_code) = &exclusive.send_error_code {
                    WriteError::Stopped(*error_code)
                } else {
                    WriteError::Finished
                }))
            }
        }
        if let Err(err) = self.check_sending(&mut exclusive) {
            return Poll::Ready(Err(WriteError::OtherError(err)));
        }
        if exclusive.sending_seq_no.contains(&seq_no) {
            exclusive.send_waiters.push(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn send(&self, data: Vec<u8>) -> SendData<'_> {
        let mut exclusive = self.inner.exclusive.lock();
        let seq_no = exclusive.next_seq_no;
        exclusive.sending_queue.push_front(Buffer::new(seq_no, data));
        exclusive.sending_seq_no.insert(seq_no);
        exclusive.next_seq_no = exclusive.next_seq_no.wrapping_add(1);
        SendData { stream: self, seq_no }
    }

    fn poll_finish_write(&self, cx: &mut Context<'_>) -> Poll<Result<(), WriteError>> {
        let mut exclusive = self.inner.exclusive.lock();
        match exclusive.send_state {
            StreamSendState::Start => {
                exclusive.start_waiters.push(cx.waker().clone());
                return Poll::Pending;
            }
            StreamSendState::StartComplete => {
                match self
                    .msquic_stream
                    .shutdown(msquic::StreamShutdownFlags::GRACEFUL, 0)
                    .map_err(WriteError::OtherError)
                {
                    Ok(()) => {
                        exclusive.send_state = StreamSendState::Shutdown;
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
            StreamSendState::Shutdown => {}
            StreamSendState::ShutdownComplete => {
                return Poll::Ready(if let Some(conn_error) = &exclusive.conn_error {
                    Err(WriteError::ConnectionLost(conn_error.clone()))
                } else if let Some(error_code) = &exclusive.send_error_code {
                    Err(WriteError::Stopped(*error_code))
                } else {
                    Ok(())
                })
            }
            _ => {
                return Poll::Ready(Err(WriteError::Closed));
            }
        }
        exclusive.write_shutdown_waiters.push(cx.waker().clone());
        Poll::Pending
    }

    fn poll_abort(&self, cx: &mut Context<'_>, error_code: u64) -> Poll<Result<(), WriteError>> {
        let mut exclusive = self.inner.exclusive.lock();
        match exclusive.send_state {
            StreamSendState::Start => {
                exclusive.start_waiters.push(cx.waker().clone());
                return Poll::Pending;
            }
            StreamSendState::StartComplete => {
                match self
                    .msquic_stream
                    .shutdown(msquic::StreamShutdownFlags::ABORT_SEND, error_code)
                    .map_err(WriteError::OtherError)
                {
                    Ok(()) => {
                        exclusive.send_state = StreamSendState::Shutdown;
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
            StreamSendState::Shutdown => {}
            StreamSendState::ShutdownComplete => {
                return Poll::Ready(if let Some(conn_error) = &exclusive.conn_error {
                    Err(WriteError::ConnectionLost(conn_error.clone()))
                } else if let Some(error_code) = &exclusive.send_error_code {
                    Err(WriteError::Stopped(*error_code))
                } else {
                    Ok(())
                })
            }
            _ => {
                return Poll::Ready(Err(WriteError::Closed));
            }
        }
        exclusive.write_shutdown_waiters.push(cx.waker().clone());
        Poll::Pending
    }

    fn abort(&self, error_code: u64) -> Result<(), WriteError> {
        let mut exclusive = self.inner.exclusive.lock();
        match exclusive.send_state {
            StreamSendState::StartComplete => {
                self.msquic_stream
                    .shutdown(msquic::StreamShutdownFlags::ABORT_SEND, error_code)
                    .map_err(WriteError::OtherError)?;
                exclusive.send_state = StreamSendState::Shutdown;
                Ok(())
            }
            _ => Err(WriteError::Closed),
        }
    }
}

impl Drop for StreamInstance {
    fn drop(&mut self) {
        trace!("StreamInstance(Inner: {:p}) dropping", self.inner);
        let exclusive = self.inner.exclusive.lock();
        match exclusive.state {
            StreamState::Start | StreamState::StartComplete => {
                trace!("StreamInstance(Inner: {:p}) shutdown while dropping", self.inner);
                // let _ = self.msquic_stream.shutdown(
                //     msquic::StreamShutdownFlags::ABORT_SEND
                //         | msquic::StreamShutdownFlags::ABORT_RECEIVE
                //         | msquic::StreamShutdownFlags::IMMEDIATE,
                //     0,
                // );
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
struct StreamInner {
    exclusive: Mutex<StreamInnerExclusive>,
    pub(crate) shared: StreamInnerShared,
}

struct StreamInnerExclusive {
    state: StreamState,
    start_status: Option<msquic::Status>,
    send_state: StreamSendState,

    sending_queue: VecDeque<Buffer>,
    sending_seq_no: HashSet<usize>,
    next_seq_no: usize,

    send_error_code: Option<u64>,
    conn_error: Option<ConnectionError>,
    start_waiters: Vec<Waker>,
    send_waiters: Vec<Waker>,
    write_shutdown_waiters: Vec<Waker>,
}

struct StreamInnerShared {
    id: RwLock<Option<u64>>,
}

#[derive(Debug, PartialEq)]
enum StreamState {
    Open,
    Start,
    StartComplete,
    ShutdownComplete,
}

#[derive(Debug, PartialEq)]
enum StreamSendState {
    Closed,
    Start,
    StartComplete,
    Shutdown,
    ShutdownComplete,
}

impl StreamInner {
    fn new(send_state: StreamSendState) -> Self {
        Self {
            exclusive: Mutex::new(StreamInnerExclusive {
                state: StreamState::Open,
                start_status: None,
                sending_queue: VecDeque::new(),
                sending_seq_no: HashSet::new(),
                next_seq_no: 1,
                send_state,
                send_error_code: None,
                conn_error: None,
                start_waiters: Vec::new(),
                send_waiters: Vec::new(),
                write_shutdown_waiters: Vec::new(),
            }),
            shared: StreamInnerShared { id: RwLock::new(None) },
        }
    }

    fn handle_event_start_complete(
        &self,
        status: msquic::Status,
        id: u64,
        peer_accepted: bool,
    ) -> Result<(), msquic::Status> {
        if status.is_ok() {
            self.shared.id.write().replace(id);
        }
        trace!(
            "Stream({:p}, id={:?}) start complete status={:?}, peer_accepted={}, id={}",
            self,
            self.shared.id.read(),
            status,
            peer_accepted,
            id,
        );
        let mut exclusive = self.exclusive.lock();
        exclusive.start_status = Some(status.clone());
        if status.is_ok() && peer_accepted {
            exclusive.state = StreamState::StartComplete;
            exclusive.send_state = StreamSendState::StartComplete;
        }

        if status.0 == (msquic::StatusCode::QUIC_STATUS_STREAM_LIMIT_REACHED as u32)
            || peer_accepted
        {
            exclusive.start_waiters.drain(..).for_each(|waker| waker.wake());
        }
        Ok(())
    }

    fn handle_event_send_complete(
        &self,
        _canceled: bool,
        client_context: *const c_void,
    ) -> Result<(), msquic::Status> {
        trace!("Stream({:p}, id={:?}) Send complete", self, self.shared.id.read());
        let buffer = unsafe { Buffer::from_raw(client_context) };
        let seq_no = buffer.0.seq_no;
        let mut exclusive = self.exclusive.lock();
        if exclusive.sending_seq_no.remove(&seq_no) {
            exclusive.send_waiters.drain(..).for_each(|waker| waker.wake());
        }
        exclusive.write_shutdown_waiters.drain(..).for_each(|waker| waker.wake());
        Ok(())
    }

    fn handle_event_peer_receive_aborted(&self, error_code: u64) -> Result<(), msquic::Status> {
        trace!("Stream({:p}, id={:?}) Peer receive aborted", self, self.shared.id.read());
        let mut exclusive = self.exclusive.lock();
        exclusive.send_state = StreamSendState::ShutdownComplete;
        exclusive.send_error_code = Some(error_code);
        exclusive.write_shutdown_waiters.drain(..).for_each(|waker| waker.wake());
        Ok(())
    }

    fn handle_event_send_shutdown_complete(&self, _graceful: bool) -> Result<(), msquic::Status> {
        trace!("Stream({:p}, id={:?}) Send shutdown complete", self, self.shared.id.read());
        let mut exclusive = self.exclusive.lock();
        exclusive.send_state = StreamSendState::ShutdownComplete;
        exclusive.write_shutdown_waiters.drain(..).for_each(|waker| waker.wake());
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_event_shutdown_complete(
        &self,
        _msquic_stream: msquic::StreamRef,
        connection_shutdown: bool,
        _app_close_in_progress: bool,
        connection_shutdown_by_app: bool,
        connection_closed_remotely: bool,
        connection_error_code: u64,
        connection_close_status: msquic::Status,
    ) -> Result<(), msquic::Status> {
        trace!("Stream({:p}, id={:?}) Shutdown complete", self, self.shared.id.read());
        {
            let mut exclusive = self.exclusive.lock();

            exclusive.state = StreamState::ShutdownComplete;
            exclusive.send_state = StreamSendState::ShutdownComplete;
            if connection_shutdown {
                match (connection_shutdown_by_app, connection_closed_remotely) {
                    (true, true) => {
                        exclusive.conn_error =
                            Some(ConnectionError::ShutdownByPeer(connection_error_code));
                    }
                    (true, false) => {
                        exclusive.conn_error = Some(ConnectionError::ShutdownByLocal);
                    }
                    (false, true) | (false, false) => {
                        exclusive.conn_error = Some(ConnectionError::ShutdownByTransport(
                            connection_close_status,
                            connection_error_code,
                        ));
                    }
                }
            }
            exclusive.start_waiters.drain(..).for_each(|waker| waker.wake());
            exclusive.write_shutdown_waiters.drain(..).for_each(|waker| waker.wake());
        }
        // unsafe {
        //     Arc::from_raw(self as *const _);
        // }
        Ok(())
    }

    fn handle_event_ideal_send_buffer_size(&self, _byte_count: u64) -> Result<(), msquic::Status> {
        trace!("Stream({:p}, id={:?}) Ideal send buffer size", self, self.shared.id.read());
        Ok(())
    }

    fn handle_event_peer_accepted(&self) -> Result<(), msquic::Status> {
        trace!("Stream({:p}, id={:?}) Peer accepted", self, self.shared.id.read());
        let mut exclusive = self.exclusive.lock();
        exclusive.state = StreamState::StartComplete;
        exclusive.send_state = StreamSendState::StartComplete;
        exclusive.start_waiters.drain(..).for_each(|waker| waker.wake());
        Ok(())
    }

    fn callback_handler_impl(
        &self,
        msquic_stream: msquic::StreamRef,
        ev: msquic::StreamEvent,
    ) -> Result<(), msquic::Status> {
        match ev {
            msquic::StreamEvent::StartComplete { status, id, peer_accepted } => {
                self.handle_event_start_complete(status, id, peer_accepted)
            }
            msquic::StreamEvent::SendComplete { cancelled, client_context } => {
                self.handle_event_send_complete(cancelled, client_context)
            }
            msquic::StreamEvent::PeerReceiveAborted { error_code } => {
                self.handle_event_peer_receive_aborted(error_code)
            }
            msquic::StreamEvent::SendShutdownComplete { graceful } => {
                self.handle_event_send_shutdown_complete(graceful)
            }
            msquic::StreamEvent::ShutdownComplete {
                connection_shutdown,
                app_close_in_progress,
                connection_shutdown_by_app,
                connection_closed_remotely,
                connection_error_code,
                connection_close_status,
            } => self.handle_event_shutdown_complete(
                msquic_stream,
                connection_shutdown,
                app_close_in_progress,
                connection_shutdown_by_app,
                connection_closed_remotely,
                connection_error_code,
                connection_close_status,
            ),
            msquic::StreamEvent::IdealSendBufferSize { byte_count } => {
                self.handle_event_ideal_send_buffer_size(byte_count)
            }
            msquic::StreamEvent::PeerAccepted => self.handle_event_peer_accepted(),
            _ => {
                trace!("Stream({:p}) Other callback", self);
                Ok(())
            }
        }
    }
}

impl Drop for StreamInner {
    fn drop(&mut self) {
        trace!("StreamInner({:p}) dropping", self);
    }
}

impl fmt::Debug for StreamInnerExclusive {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Exclusive")
            .field("state", &self.state)
            .field("send_state", &self.send_state)
            .finish()
    }
}

impl fmt::Debug for StreamInnerShared {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Shared").field("id", &self.id).finish()
    }
}
pub struct SendData<'a> {
    stream: &'a StreamInstance,
    seq_no: usize,
}

impl Future for SendData<'_> {
    type Output = Result<(), WriteError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stream.poll_send_data(cx, self.seq_no)
    }
}

#[derive(Debug, Error, Clone)]
pub enum WriteError {
    #[error("stream not opened for writing")]
    Closed,
    #[error("stream finished")]
    Finished,
    #[error("stream stopped by peer: error {0}")]
    Stopped(u64),
    #[error("connection lost {0:?}")]
    ConnectionLost(#[from] ConnectionError),
    #[error("other error: status {0:?}")]
    OtherError(msquic::Status),
}

impl From<WriteError> for std::io::Error {
    fn from(e: WriteError) -> Self {
        let kind = match e {
            WriteError::Closed
            | WriteError::Finished
            | WriteError::ConnectionLost(ConnectionError::ConnectionClosed) => {
                std::io::ErrorKind::NotConnected
            }
            WriteError::Stopped(_) => std::io::ErrorKind::ConnectionReset,
            WriteError::ConnectionLost(_) => std::io::ErrorKind::ConnectionAborted,
            WriteError::OtherError(_) => std::io::ErrorKind::Other,
        };
        Self::new(kind, e)
    }
}

struct Buffer(Box<BufferInner>);

struct BufferInner {
    seq_no: usize,
    _data: Box<[u8]>,
    buffer: [msquic::BufferRef; 1],
}

unsafe impl Sync for Buffer {}
unsafe impl Send for Buffer {}

impl Buffer {
    fn new(seq_no: usize, data: Vec<u8>) -> Self {
        let data = data.into_boxed_slice();
        let buffer = data.as_ref().into();
        Self(Box::new(BufferInner { seq_no, _data: data, buffer: [buffer] }))
    }

    unsafe fn from_raw(inner: *const c_void) -> Self {
        Self(unsafe { Box::from_raw(inner as *mut BufferInner) })
    }

    fn into_raw(self) -> *mut c_void {
        Box::into_raw(self.0) as *mut c_void
    }

    fn get_buffers(&self) -> (*const msquic::BufferRef, usize) {
        let ptr = self.0.buffer.as_ptr();
        let len = 1;
        (ptr, len)
    }
}
