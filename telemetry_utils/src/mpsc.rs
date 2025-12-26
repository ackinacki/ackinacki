use std::sync::atomic::AtomicIsize;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SendError;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::time::Duration;

pub trait InstrumentedChannelMetrics {
    fn report_channel(&self, channel: &'static str, delta: isize);
}

pub struct InstrumentedSender<T> {
    inner: std::sync::mpsc::Sender<T>,
    metrics: Option<Arc<ChannelMetrics>>,
}

impl<T> InstrumentedSender<T> {
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        let result = self.inner.send(t);
        if result.is_ok() {
            if let Some(x) = self.metrics.as_ref() {
                x.inc_len(1)
            }
        }
        result
    }
}

impl<T> Clone for InstrumentedSender<T> {
    fn clone(&self) -> Self {
        InstrumentedSender { inner: self.inner.clone(), metrics: self.metrics.clone() }
    }
}

pub struct InstrumentedReceiver<T> {
    inner: std::sync::mpsc::Receiver<T>,
    metrics: Option<Arc<ChannelMetrics>>,
}

impl<T> InstrumentedReceiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        let result = self.inner.recv();
        if result.is_ok() {
            if let Some(x) = self.metrics.as_ref() {
                x.inc_len(-1)
            }
        }
        result
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let result = self.inner.recv_timeout(timeout);
        if result.is_ok() {
            if let Some(x) = self.metrics.as_ref() {
                x.inc_len(-1)
            }
        }
        result
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            if let Some(x) = self.metrics.as_ref() {
                x.inc_len(-1)
            }
        }
        result
    }
}

struct ChannelMetrics {
    metrics: Box<dyn InstrumentedChannelMetrics + Send + Sync>,
    channel: &'static str,
    len: AtomicIsize,
}

impl ChannelMetrics {
    fn new(
        metrics: impl InstrumentedChannelMetrics + Send + Sync + 'static,
        channel: &'static str,
    ) -> Self {
        Self { metrics: Box::new(metrics), channel, len: AtomicIsize::new(0) }
    }

    fn inc_len(&self, delta: isize) {
        self.len.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
        self.metrics.report_channel(self.channel, delta);
    }
}

impl Drop for ChannelMetrics {
    fn drop(&mut self) {
        let len = self.len.load(std::sync::atomic::Ordering::Relaxed);
        if len > 0 {
            self.metrics.report_channel(self.channel, -len);
        }
    }
}
pub fn instrumented_channel<T>(
    metrics: Option<impl InstrumentedChannelMetrics + Send + Sync + 'static>,
    channel: &'static str,
) -> (InstrumentedSender<T>, InstrumentedReceiver<T>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let metrics = metrics.map(|x| Arc::new(ChannelMetrics::new(x, channel)));
    (
        InstrumentedSender { inner: tx, metrics: metrics.clone() },
        InstrumentedReceiver { inner: rx, metrics: metrics.clone() },
    )
}
