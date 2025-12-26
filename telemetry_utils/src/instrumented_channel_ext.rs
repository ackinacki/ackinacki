use std::sync::mpsc::RecvError;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SendError;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::time::Duration;

pub trait XInstrumentedChannelMetrics {
    fn report_channel(&self, channel: &'static str, delta: isize, label: String);
}

pub struct WrappedItem<T> {
    pub payload: T,
    pub label: String,
}

pub struct XInstrumentedSender<T> {
    inner: std::sync::mpsc::Sender<WrappedItem<T>>,
    metrics: Option<Arc<ChannelMetrics>>,
}

impl<T> XInstrumentedSender<T> {
    pub fn send(&self, t: WrappedItem<T>) -> Result<(), SendError<WrappedItem<T>>> {
        let label = t.label.clone();
        let result = self.inner.send(t);
        if result.is_ok() {
            if let Some(metrics) = &self.metrics {
                metrics.inc_len(1, label);
            }
        }
        result
    }
}

impl<T> Clone for XInstrumentedSender<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), metrics: self.metrics.clone() }
    }
}

pub struct XInstrumentedReceiver<T> {
    inner: std::sync::mpsc::Receiver<WrappedItem<T>>,
    metrics: Option<Arc<ChannelMetrics>>,
}

impl<T> XInstrumentedReceiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        let result = self.inner.recv();
        if let Ok(item) = &result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_len(-1, item.label.clone());
            }
        }
        result.map(|x| x.payload)
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let result = self.inner.recv_timeout(timeout);
        if let Ok(item) = &result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_len(-1, item.label.clone());
            }
        }
        result.map(|x| x.payload)
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let result = self.inner.try_recv();
        if let Ok(item) = &result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_len(-1, item.label.clone());
            }
        }
        result.map(|x| x.payload)
    }
}

struct ChannelMetrics {
    metrics: Box<dyn XInstrumentedChannelMetrics + Send + Sync>,
    channel: &'static str,
}

impl ChannelMetrics {
    fn new(
        metrics: impl XInstrumentedChannelMetrics + Send + Sync + 'static,
        channel: &'static str,
    ) -> Self {
        Self { metrics: Box::new(metrics), channel }
    }

    fn inc_len(&self, delta: isize, label: String) {
        self.metrics.report_channel(self.channel, delta, label);
    }
}

impl Drop for ChannelMetrics {
    fn drop(&mut self) {
        println!("That's OK to drop the channel, but we do not expect that it will be created again, otherwise it can lead to distortion of metrics.")
    }
}

pub fn instrumented_channel<T>(
    metrics: Option<impl XInstrumentedChannelMetrics + Send + Sync + 'static>,
    channel: &'static str,
) -> (XInstrumentedSender<T>, XInstrumentedReceiver<T>) {
    let (tx, rx) = std::sync::mpsc::channel::<WrappedItem<T>>();
    let shared_metrics = metrics.map(|m| Arc::new(ChannelMetrics::new(m, channel)));
    (
        XInstrumentedSender { inner: tx, metrics: shared_metrics.clone() },
        XInstrumentedReceiver { inner: rx, metrics: shared_metrics },
    )
}
