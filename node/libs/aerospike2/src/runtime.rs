//! Process-wide tokio runtime for the Aerospike v2 client.
//!
//! The aerospike v2 client is async-based and needs a tokio runtime in
//! scope on every call. Building a fresh runtime per call (the previous
//! behaviour) is not free — each `RuntimeBuilder::build` spawns OS
//! threads and sets up reactors. Under bulk-load workloads (snapshot
//! import does ~10k batches × multiple ops per batch) the cumulative
//! cost is significant.
//!
//! `with_runtime` reuses one process-wide multi-threaded runtime. If
//! the caller is already inside a tokio task, that runtime is used
//! instead (no nested runtime).

use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::Builder as RuntimeBuilder;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;

static SHARED_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_shared_runtime() -> anyhow::Result<&'static Runtime> {
    if let Some(rt) = SHARED_RUNTIME.get() {
        return Ok(rt);
    }
    let rt = RuntimeBuilder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .thread_name("aerospike2-rt")
        .build()
        .map_err(|err| {
            anyhow::anyhow!("Failed to build shared Tokio runtime for Aerospike: {err}")
        })?;
    // Race: another thread might have set it first. Either result is fine.
    let _ = SHARED_RUNTIME.set(rt);
    Ok(SHARED_RUNTIME.get().expect("just set above"))
}

/// Run an async aerospike future to completion synchronously, on the
/// process-wide shared `aerospike2-rt` runtime. We never use the caller's
/// runtime, even when one is current, because the V2 client's cluster
/// (TCP connections + tend task) is bound to `aerospike2-rt` at construction
/// (see `Aerospike2Backend::new`), and tokio I/O resources only work
/// when polled by the runtime that owns them.
///
/// `block_on` here is `tokio::runtime::Runtime::block_on` — the proper
/// sync↔async bridge. The previous implementation used `aerospike-sync`,
/// which wraps each call in `futures::executor::block_on`. That executor
/// drives the future on the calling thread via `std::thread::park`, while
/// the future itself uses `tokio::spawn` for parallel per-node batch
/// dispatch. The combination is brittle: the spawned tasks need tokio
/// workers to make progress, and the parker can fail to wake under some
/// scheduling patterns, producing silent multi-minute hangs that surface
/// at oddly deterministic call counts.
pub(crate) fn block_on<T>(future: impl Future<Output = T>) -> anyhow::Result<T> {
    if let Ok(handle) = Handle::try_current() {
        // We're on a tokio worker. Calling `Runtime::block_on` from inside
        // an async runtime is not allowed (it panics). Use `block_in_place`
        // to escape the worker first, then drive the future on the shared
        // aerospike runtime. `block_in_place` requires a multi-threaded
        // runtime — which all of acki-nacki uses.
        let rt = get_shared_runtime()?;
        Ok(tokio::task::block_in_place(|| {
            let _ = handle;
            rt.block_on(future)
        }))
    } else {
        let rt = get_shared_runtime()?;
        Ok(rt.block_on(future))
    }
}
