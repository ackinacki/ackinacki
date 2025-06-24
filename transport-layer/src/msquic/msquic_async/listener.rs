use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use thiserror::Error;
use tracing::trace;

use super::connection::Connection;

/// Listener for incoming connections.
pub struct Listener {
    inner: Arc<ListenerInner>,
    msquic_listener: msquic::Listener,
}

impl Listener {
    /// Create a new listener.
    pub fn new(
        registration: &msquic::Registration,
        configuration: msquic::Configuration,
    ) -> Result<Self, ListenError> {
        let inner = Arc::new(ListenerInner::new(configuration));
        let inner_in_ev = inner.clone();
        let msquic_listener = msquic::Listener::open(registration, move |_, ev| match ev {
            msquic::ListenerEvent::NewConnection { info, connection } => {
                inner_in_ev.handle_event_new_connection(info, connection)
            }
            msquic::ListenerEvent::StopComplete { app_close_in_progress } => {
                inner_in_ev.handle_event_stop_complete(app_close_in_progress)
            }
        })
        .map_err(ListenError::OtherError)?;
        trace!("Listener({:p}) new", inner);
        Ok(Self { inner, msquic_listener })
    }

    /// Start the listener.
    pub fn start<T: AsRef<[msquic::BufferRef]>>(
        &self,
        alpn: &T,
        local_address: Option<SocketAddr>,
    ) -> Result<(), ListenError> {
        let mut exclusive = self.inner.exclusive.lock().unwrap();
        match exclusive.state {
            ListenerState::Open | ListenerState::ShutdownComplete => {}
            ListenerState::StartComplete | ListenerState::Shutdown => {
                return Err(ListenError::AlreadyStarted);
            }
        }
        let local_address: Option<msquic::Addr> = local_address.map(|x| x.into());
        self.msquic_listener
            .start(alpn.as_ref(), local_address.as_ref())
            .map_err(ListenError::OtherError)?;
        exclusive.state = ListenerState::StartComplete;
        Ok(())
    }

    /// Accept a new connection.
    pub fn accept(&self) -> Accept {
        Accept(self)
    }

    /// Poll to accept a new connection.
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<Result<Connection, ListenError>> {
        trace!("Listener({:p}) poll_accept", self);
        let mut exclusive = self.inner.exclusive.lock().unwrap();

        if let Some(connection) = exclusive.new_connections.pop_front() {
            return Poll::Ready(Ok(connection));
        }

        match exclusive.state {
            ListenerState::Open => {
                return Poll::Ready(Err(ListenError::NotStarted));
            }
            ListenerState::StartComplete | ListenerState::Shutdown => {}
            ListenerState::ShutdownComplete => {
                return Poll::Ready(Err(ListenError::Finished));
            }
        }
        exclusive.new_connection_waiters.push(cx.waker().clone());
        Poll::Pending
    }

    /// Stop the listener.
    pub fn stop(&self) -> Stop {
        Stop(self)
    }

    /// Poll to stop the listener.
    pub fn poll_stop(&self, cx: &mut Context<'_>) -> Poll<Result<(), ListenError>> {
        trace!("Listener({:p}) poll_stop", self);
        let mut call_stop = false;
        {
            let mut exclusive = self.inner.exclusive.lock().unwrap();

            match exclusive.state {
                ListenerState::Open => {
                    return Poll::Ready(Err(ListenError::NotStarted));
                }
                ListenerState::StartComplete => {
                    call_stop = true;
                    exclusive.state = ListenerState::Shutdown;
                }
                ListenerState::Shutdown => {}
                ListenerState::ShutdownComplete => {
                    return Poll::Ready(Ok(()));
                }
            }
            exclusive.shutdown_complete_waiters.push(cx.waker().clone());
        }
        if call_stop {
            self.msquic_listener.stop();
        }
        Poll::Pending
    }

    /// Get the local address the listener is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, ListenError> {
        self.msquic_listener
            .get_local_addr()
            .map(|addr| addr.as_socket().expect("not a socket address"))
            .map_err(|_| ListenError::Failed)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        trace!("Listener(Inner: {:p}) dropping", self.inner);
    }
}

struct ListenerInner {
    exclusive: Mutex<ListenerInnerExclusive>,
    shared: ListenerInnerShared,
}

struct ListenerInnerExclusive {
    state: ListenerState,
    new_connections: VecDeque<Connection>,
    new_connection_waiters: Vec<Waker>,
    shutdown_complete_waiters: Vec<Waker>,
}
unsafe impl Sync for ListenerInnerExclusive {}
unsafe impl Send for ListenerInnerExclusive {}

struct ListenerInnerShared {
    configuration: msquic::Configuration,
}
unsafe impl Sync for ListenerInnerShared {}
unsafe impl Send for ListenerInnerShared {}

#[derive(Debug, Clone, PartialEq)]
enum ListenerState {
    Open,
    StartComplete,
    Shutdown,
    ShutdownComplete,
}

impl ListenerInner {
    fn new(configuration: msquic::Configuration) -> Self {
        Self {
            exclusive: Mutex::new(ListenerInnerExclusive {
                state: ListenerState::Open,
                new_connections: VecDeque::new(),
                new_connection_waiters: Vec::new(),
                shutdown_complete_waiters: Vec::new(),
            }),
            shared: ListenerInnerShared { configuration },
        }
    }

    fn handle_event_new_connection(
        &self,
        _info: msquic::NewConnectionInfo<'_>,
        connection: msquic::ConnectionRef,
    ) -> Result<(), msquic::Status> {
        trace!("Listener({:p}) New connection", self);

        connection.set_configuration(&self.shared.configuration)?;
        let new_conn = Connection::from_raw(unsafe { connection.as_raw() }, false);

        let mut exclusive = self.exclusive.lock().unwrap();
        exclusive.new_connections.push_back(new_conn);
        exclusive.new_connection_waiters.drain(..).for_each(|waker| waker.wake());
        Ok(())
    }

    fn handle_event_stop_complete(
        &self,
        app_close_in_progress: bool,
    ) -> Result<(), msquic::Status> {
        trace!(
            "Listener({:p}) Stop complete: app_close_in_progress={}",
            self,
            app_close_in_progress
        );
        {
            let mut exclusive = self.exclusive.lock().unwrap();
            exclusive.state = ListenerState::ShutdownComplete;

            exclusive.new_connection_waiters.drain(..).for_each(|waker| waker.wake());

            exclusive.shutdown_complete_waiters.drain(..).for_each(|waker| waker.wake());
            trace!(
                "Listener({:p}) new_connections's len={}",
                self,
                exclusive.new_connections.len()
            );
        }
        // unsafe {
        //     Arc::from_raw(self as *const _);
        // }
        Ok(())
    }
}

impl Drop for ListenerInner {
    fn drop(&mut self) {
        trace!("ListenerInner({:p}) dropping", self);
    }
}

/// Future generated by `[Listener::accept()]`.
pub struct Accept<'a>(&'a Listener);

impl Future for Accept<'_> {
    type Output = Result<Connection, ListenError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_accept(cx)
    }
}

/// Future generated by `[Listener::stop()]`.
pub struct Stop<'a>(&'a Listener);

impl Future for Stop<'_> {
    type Output = Result<(), ListenError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_stop(cx)
    }
}

#[derive(Debug, Error, Clone)]
pub enum ListenError {
    #[error("Not started yet")]
    NotStarted,
    #[error("already started")]
    AlreadyStarted,
    #[error("finished")]
    Finished,
    #[error("failed")]
    Failed,
    #[error("other error: status {0:?}")]
    OtherError(msquic::Status),
}
