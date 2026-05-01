use std::any::Any;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedReceiver;
use telemetry_utils::mpsc::InstrumentedSender;

use crate::helper::metrics::BlockProductionMetrics;
use crate::helper::metrics::JOIN_HANDLE_MONITOR_CHANNEL;

const WARN_AFTER: Duration = Duration::from_secs(1);
const WARN_INTERVAL: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(5);

trait ErasedJoinHandle: Send {
    fn is_finished(&self) -> bool;
    fn join(self: Box<Self>) -> JoinOutcome;
}

enum JoinOutcome {
    Ok,
    Err(anyhow::Error),
    Panic(String),
}

impl<T> ErasedJoinHandle for JoinHandle<anyhow::Result<T>>
where
    T: Send + 'static,
{
    fn is_finished(&self) -> bool {
        JoinHandle::is_finished(self)
    }

    fn join(self: Box<Self>) -> JoinOutcome {
        match (*self).join() {
            Ok(Ok(_)) => JoinOutcome::Ok,
            Ok(Err(error)) => JoinOutcome::Err(error),
            Err(panic_payload) => JoinOutcome::Panic(format_panic_payload(panic_payload)),
        }
    }
}

enum Command {
    Watch { handle: Box<dyn ErasedJoinHandle>, thread_name: String, description: String },
    Shutdown,
}

#[derive(Clone)]
pub struct JoinHandleMonitorInterface {
    tx: InstrumentedSender<Command>,
    shutdown_requested: Arc<AtomicBool>,
}

impl JoinHandleMonitorInterface {
    pub fn watch<T>(&self, handle: JoinHandle<anyhow::Result<T>>, description: impl Into<String>)
    where
        T: Send + 'static,
    {
        let description = description.into();
        let thread_name = handle.thread().name().unwrap_or("<unnamed>").to_string();
        if self.shutdown_requested.load(Ordering::Acquire) {
            tracing::debug!(
                "JoinHandleMonitorService shutdown already started; dropping handle without join: thread_name={thread_name}, description={description}",
            );
            drop(handle);
            return;
        }
        let command = Command::Watch {
            handle: Box::new(handle),
            thread_name: thread_name.clone(),
            description,
        };
        if let Err(std::sync::mpsc::SendError(Command::Watch { handle, description, .. })) =
            self.tx.send(command)
        {
            if self.shutdown_requested.load(Ordering::Acquire) {
                tracing::debug!(
                    "JoinHandleMonitorService shutdown already started; dropping handle without join: thread_name={thread_name}, description={description}",
                );
            } else {
                tracing::error!(
                    "Failed to send join handle to monitor; dropping handle without join: thread_name={thread_name}, description={description}",
                );
            }
            drop(handle);
        }
    }
}

pub struct JoinHandleMonitorService {
    tx: Option<InstrumentedSender<Command>>,
    handler: Option<std::thread::JoinHandle<()>>,
    shutdown_requested: Arc<AtomicBool>,
}

struct WatchedJoinHandle {
    handle: Box<dyn ErasedJoinHandle>,
    thread_name: String,
    description: String,
    received_at: Instant,
    last_warn_at: Option<Instant>,
}

impl JoinHandleMonitorService {
    pub fn new(metrics: Option<BlockProductionMetrics>) -> anyhow::Result<Self> {
        let (tx, rx) = instrumented_channel(metrics.clone(), JOIN_HANDLE_MONITOR_CHANNEL);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let handler = std::thread::Builder::new()
            .name("JoinHandle monitor".to_string())
            .spawn(move || Self::run(rx, metrics))?;
        Ok(Self { tx: Some(tx), handler: Some(handler), shutdown_requested })
    }

    pub fn interface(&self) -> JoinHandleMonitorInterface {
        JoinHandleMonitorInterface {
            tx: self.tx.as_ref().expect("JoinHandleMonitorService sender must exist").clone(),
            shutdown_requested: Arc::clone(&self.shutdown_requested),
        }
    }

    pub fn shutdown(&mut self) {
        let already_requested = self.shutdown_requested.swap(true, Ordering::AcqRel);
        if !already_requested {
            if let Some(tx) = self.tx.as_ref() {
                if tx.send(Command::Shutdown).is_err() {
                    tracing::trace!(
                        "JoinHandleMonitorService shutdown command could not be delivered because monitor thread is already gone"
                    );
                }
            }
        }

        self.tx.take();
        self.join_handler();
    }

    fn run(rx: InstrumentedReceiver<Command>, metrics: Option<BlockProductionMetrics>) {
        let mut buffer = Vec::new();
        let mut accept_commands = true;
        let mut shutdown_started_at = None;

        tracing::trace!("JoinHandleMonitorService started");

        loop {
            if accept_commands {
                match rx.recv_timeout(POLL_INTERVAL) {
                    Ok(command) => {
                        Self::handle_command(
                            &mut buffer,
                            command,
                            &mut accept_commands,
                            &mut shutdown_started_at,
                        );
                        if accept_commands {
                            Self::drain_pending_commands(
                                &rx,
                                &mut buffer,
                                &mut accept_commands,
                                &mut shutdown_started_at,
                            );
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        tracing::trace!(
                            "JoinHandleMonitorService channel closed; draining remaining handles"
                        );
                        Self::enter_shutdown_mode(&mut accept_commands, &mut shutdown_started_at);
                    }
                }
            } else if buffer.is_empty() {
                break;
            } else {
                std::thread::sleep(POLL_INTERVAL);
            }

            Self::poll_buffer(&mut buffer);

            if let Some(metrics) = metrics.as_ref() {
                metrics.report_join_handle_monitor_buffer_size(buffer.len() as u64);
            }

            if let Some(shutdown_started_at) = shutdown_started_at {
                if !buffer.is_empty() && shutdown_started_at.elapsed() >= SHUTDOWN_GRACE_PERIOD {
                    Self::log_shutdown_timeout(&buffer, shutdown_started_at.elapsed());
                    break;
                }
            }

            if !accept_commands && buffer.is_empty() {
                break;
            }
        }

        if let Some(metrics) = metrics.as_ref() {
            metrics.report_join_handle_monitor_buffer_size(0);
        }
        tracing::trace!("JoinHandleMonitorService stopped");
    }

    fn drain_pending_commands(
        rx: &InstrumentedReceiver<Command>,
        buffer: &mut Vec<WatchedJoinHandle>,
        accept_commands: &mut bool,
        shutdown_started_at: &mut Option<Instant>,
    ) {
        loop {
            match rx.try_recv() {
                Ok(command) => {
                    Self::handle_command(buffer, command, accept_commands, shutdown_started_at);
                    if !*accept_commands {
                        break;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    Self::enter_shutdown_mode(accept_commands, shutdown_started_at);
                    break;
                }
            }
        }
    }

    fn handle_command(
        buffer: &mut Vec<WatchedJoinHandle>,
        command: Command,
        accept_commands: &mut bool,
        shutdown_started_at: &mut Option<Instant>,
    ) {
        match command {
            Command::Watch { .. } => Self::push_command(buffer, command),
            Command::Shutdown => {
                tracing::trace!(
                    "JoinHandleMonitorService received explicit shutdown command; draining remaining handles"
                );
                Self::enter_shutdown_mode(accept_commands, shutdown_started_at);
            }
        }
    }

    fn enter_shutdown_mode(accept_commands: &mut bool, shutdown_started_at: &mut Option<Instant>) {
        *accept_commands = false;
        if shutdown_started_at.is_none() {
            *shutdown_started_at = Some(Instant::now());
        }
    }

    fn push_command(buffer: &mut Vec<WatchedJoinHandle>, command: Command) {
        match command {
            Command::Watch { handle, thread_name, description } => {
                tracing::trace!(
                    "JoinHandleMonitorService watching handle: thread_name={thread_name}, description={description}",
                );
                buffer.push(WatchedJoinHandle {
                    handle,
                    thread_name,
                    description,
                    received_at: Instant::now(),
                    last_warn_at: None,
                });
            }
            Command::Shutdown => unreachable!("shutdown is handled before push_command"),
        }
    }

    fn poll_buffer(buffer: &mut Vec<WatchedJoinHandle>) {
        let now = Instant::now();
        let mut index = 0;

        while index < buffer.len() {
            if buffer[index].handle.is_finished() {
                let watched = buffer.swap_remove(index);
                Self::join_finished_handle(watched, now, buffer.len());
                continue;
            }

            let elapsed = now.duration_since(buffer[index].received_at);
            let should_warn = elapsed >= WARN_AFTER
                && buffer[index]
                    .last_warn_at
                    .map(|last_warn_at| now.duration_since(last_warn_at) >= WARN_INTERVAL)
                    .unwrap_or(true);
            if should_warn {
                buffer[index].last_warn_at = Some(now);
                tracing::warn!(
                    "JoinHandleMonitorService is still waiting for thread completion: thread_name={}, description={}, elapsed={elapsed:?}, buffer_size={}",
                    buffer[index].thread_name,
                    buffer[index].description,
                    buffer.len(),
                );
            }

            index += 1;
        }
    }

    fn log_shutdown_timeout(buffer: &[WatchedJoinHandle], elapsed: Duration) {
        tracing::error!(
            "JoinHandleMonitorService shutdown grace period expired; dropping unfinished handles without join: elapsed={elapsed:?}, buffer_size={}",
            buffer.len(),
        );
        for watched in buffer {
            tracing::error!(
                "JoinHandleMonitorService abandoned unfinished handle during shutdown: thread_name={}, description={}, waited={:?}",
                watched.thread_name,
                watched.description,
                watched.received_at.elapsed(),
            );
        }
    }

    fn join_finished_handle(
        watched: WatchedJoinHandle,
        now: Instant,
        remaining_buffer_size: usize,
    ) {
        let elapsed = now.duration_since(watched.received_at);
        match watched.handle.join() {
            JoinOutcome::Ok => {
                tracing::trace!(
                    "JoinHandleMonitorService joined thread successfully: thread_name={}, description={}, elapsed={elapsed:?}, buffer_size={remaining_buffer_size}",
                    watched.thread_name,
                    watched.description,
                );
            }
            JoinOutcome::Err(error) => {
                tracing::error!(
                    "JoinHandleMonitorService joined thread with error: thread_name={}, description={}, elapsed={elapsed:?}, buffer_size={remaining_buffer_size}, error={error:?}",
                    watched.thread_name,
                    watched.description,
                );
            }
            JoinOutcome::Panic(panic_message) => {
                tracing::error!(
                    "JoinHandleMonitorService joined panicked thread: thread_name={}, description={}, elapsed={elapsed:?}, buffer_size={remaining_buffer_size}, panic={panic_message}",
                    watched.thread_name,
                    watched.description,
                );
            }
        }
    }

    fn join_handler(&mut self) {
        if let Some(handler) = self.handler.take() {
            if let Err(panic_payload) = handler.join() {
                let panic_message = format_panic_payload(panic_payload);
                tracing::error!(
                    "JoinHandleMonitorService thread panicked during shutdown: panic={panic_message}"
                );
            }
        }
    }
}

impl Drop for JoinHandleMonitorService {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn format_panic_payload(payload: Box<dyn Any + Send + 'static>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "non-string panic payload".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::JoinHandleMonitorService;

    #[test]
    fn service_accepts_and_joins_finished_handles() -> anyhow::Result<()> {
        let mut service = JoinHandleMonitorService::new(None)?;
        let interface = service.interface();
        let unit_handle = std::thread::Builder::new()
            .name("join-handle-monitor-test".to_string())
            .spawn(|| -> anyhow::Result<()> {
                std::thread::sleep(Duration::from_millis(10));
                Ok(())
            })?;
        let value_handle = std::thread::Builder::new()
            .name("join-handle-monitor-test-value".to_string())
            .spawn(|| -> anyhow::Result<usize> {
                std::thread::sleep(Duration::from_millis(10));
                Ok(42)
            })?;

        interface.watch(unit_handle, "unit handle");
        interface.watch(value_handle, "value handle");

        std::thread::sleep(Duration::from_millis(200));
        drop(interface);
        service.shutdown();
        Ok(())
    }

    #[test]
    fn shutdown_completes_with_live_interface_clone() -> anyhow::Result<()> {
        let service = JoinHandleMonitorService::new(None)?;
        let interface = service.interface();
        let interface_clone = interface.clone();
        let (done_tx, done_rx) = mpsc::channel();

        let join = std::thread::Builder::new().name("test".to_string()).spawn(move || {
            let mut service = service;
            service.shutdown();
            done_tx.send(()).unwrap();
        })?;

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(interface_clone);
        drop(interface);
        join.join().unwrap();
        Ok(())
    }

    #[test]
    fn shutdown_is_idempotent() -> anyhow::Result<()> {
        let service = JoinHandleMonitorService::new(None)?;
        let (done_tx, done_rx) = mpsc::channel();

        let join = std::thread::Builder::new().name("test".to_string()).spawn(move || {
            let mut service = service;
            service.shutdown();
            service.shutdown();
            done_tx.send(()).unwrap();
        })?;

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        join.join().unwrap();
        Ok(())
    }

    #[test]
    fn watch_after_shutdown_does_not_block_or_panic() -> anyhow::Result<()> {
        let mut service = JoinHandleMonitorService::new(None)?;
        let interface = service.interface();
        service.shutdown();

        let (worker_done_tx, worker_done_rx) = mpsc::channel();
        let (watch_done_tx, watch_done_rx) = mpsc::channel();
        let handle = std::thread::Builder::new()
            .name("join-handle-monitor-test-post-shutdown".to_string())
            .spawn(move || -> anyhow::Result<()> {
                std::thread::sleep(Duration::from_millis(10));
                worker_done_tx.send(()).unwrap();
                Ok(())
            })?;

        let join = std::thread::Builder::new().name("test".to_string()).spawn(move || {
            interface.watch(handle, "post-shutdown handle");
            watch_done_tx.send(()).unwrap();
        })?;

        watch_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        worker_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        join.join().unwrap();
        Ok(())
    }
}
