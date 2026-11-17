//! Process RSS, swap and optional system-allocator observations.
//! RSS and swap use cheap OS queries. Allocator inspection stays in its own
//! opt-in sampler: mallinfo2 can hold malloc locks while walking free lists.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use opentelemetry::metrics::Meter;
use opentelemetry::metrics::ObservableGauge;
use parking_lot::Mutex;

#[derive(Clone, Copy, Default)]
pub(super) struct MemoryUsage {
    pub rss_bytes: Option<u64>,
    pub swap_bytes: Option<u64>,
}

/// Both observable gauges share one OS sample. Refresh on collection, at most
/// once per minute, so no sampler thread or shutdown lifecycle is needed.
/// Missing measurements stay absent rather than suggesting zero memory usage.
pub(super) fn sample() -> MemoryUsage {
    static CACHE: OnceLock<Mutex<(Option<Instant>, MemoryUsage)>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(|| Mutex::new((None, MemoryUsage::default()))).lock();
    if cache.0.is_none_or(|last| last.elapsed() >= Duration::from_secs(60)) {
        cache.1 = read();
        cache.0 = Some(Instant::now());
    }
    cache.1
}

#[cfg(target_os = "linux")]
fn read() -> MemoryUsage {
    std::fs::read_to_string("/proc/self/status")
        .map(|status| parse_linux_status(&status))
        .unwrap_or_default()
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_status(status: &str) -> MemoryUsage {
    fn bytes(value: &str) -> Option<u64> {
        let mut fields = value.split_whitespace();
        let kib = fields.next()?.parse::<u64>().ok()?;
        // Linux /proc reports these fields in KiB, spelled "kB". Do not
        // assume a 4 KiB page size, or confuse unavailable fields with zero.
        (fields.next()? == "kB").then_some(())?;
        kib.checked_mul(1024)
    }

    let mut usage = MemoryUsage::default();
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            usage.rss_bytes = bytes(value);
        } else if let Some(value) = line.strip_prefix("VmSwap:") {
            // VmSwap covers private anonymous pages, excluding shared-memory swap.
            usage.swap_bytes = bytes(value);
        }
    }
    usage
}

#[cfg(target_os = "macos")]
fn read() -> MemoryUsage {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::uninit();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    // SAFETY: task_info receives a buffer of the exact type and size required
    // by MACH_TASK_BASIC_INFO. Read it only after a successful, complete reply.
    // libc deprecates this wrapper in favor of mach2, but already provides all
    // the Mach types we need; keep the single platform dependency here.
    #[allow(deprecated)]
    let result = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS || count != libc::MACH_TASK_BASIC_INFO_COUNT {
        return MemoryUsage::default();
    }
    // SAFETY: the successful task_info call initialized the full structure.
    let info = unsafe { info.assume_init() };
    MemoryUsage {
        rss_bytes: Some(info.resident_size),
        // macOS has no equivalent of Linux's per-process VmSwap measurement.
        swap_bytes: None,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read() -> MemoryUsage {
    MemoryUsage::default()
}

const INTERVAL_ENV: &str = "NODE_ALLOCATOR_STATS_INTERVAL_SECS";

pub(super) struct AllocatorStats {
    _gauge: ObservableGauge<u64>,
    // Dropping the last metrics owner disconnects this channel and wakes the
    // sleeping sampler. Do not join it: an in-flight malloc inspection can wait
    // for application allocations, and must not delay metrics teardown.
    _stop: mpsc::Sender<()>,
}

fn interval(value: Option<&str>) -> Result<Option<Duration>, std::num::ParseIntError> {
    let seconds = value.unwrap_or("0").parse::<u64>()?;
    Ok((seconds > 0).then(|| Duration::from_secs(seconds)))
}

impl AllocatorStats {
    pub(super) fn from_env(meter: &Meter) -> Option<Self> {
        let value = match std::env::var(INTERVAL_ENV) {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => {
                tracing::warn!(%error, "Invalid allocator statistics interval; collection disabled");
                return None;
            }
        };
        let interval = match interval(value.as_deref()) {
            Ok(interval) => interval?,
            Err(error) => {
                tracing::warn!(%error, "Invalid allocator statistics interval; collection disabled");
                return None;
            }
        };
        if !available() {
            tracing::warn!(
                "Allocator statistics require Linux with glibc 2.33 or newer; collection disabled"
            );
            return None;
        }

        let bytes = Arc::new(AtomicU64::new(0));
        let initialized = Arc::new(AtomicBool::new(false));
        let observed_bytes = bytes.clone();
        let observed_initialized = initialized.clone();
        let gauge = meter
            .u64_observable_gauge("node_allocator_allocated_bytes")
            .with_description(
                "System allocator in-use byte estimate, including overhead and caches",
            )
            .with_callback(move |observer| {
                // Do not publish a misleading zero before the first sample.
                if observed_initialized.load(Ordering::Acquire) {
                    observer.observe(observed_bytes.load(Ordering::Relaxed), &[]);
                }
            })
            .build();
        let (stop, receiver) = mpsc::channel();
        let worker = std::thread::Builder::new().name("allocator-stats".into()).spawn(move || {
            loop {
                if let Some(value) = allocated_bytes() {
                    bytes.store(value, Ordering::Relaxed);
                    initialized.store(true, Ordering::Release);
                }
                // Wait after sampling, rather than catching up missed ticks.
                // A slow inspection must not trigger a burst of further scans.
                if receiver.recv_timeout(interval) != Err(mpsc::RecvTimeoutError::Timeout) {
                    break;
                }
            }
        });
        if let Err(error) = worker {
            tracing::warn!(%error, "Could not start allocator statistics sampler");
            return None;
        }
        tracing::info!(interval_secs = interval.as_secs(), "Allocator statistics sampling enabled");
        Some(Self { _gauge: gauge, _stop: stop })
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn mallinfo2_function() -> Option<unsafe extern "C" fn() -> libc::mallinfo2> {
    type Mallinfo2 = unsafe extern "C" fn() -> libc::mallinfo2;
    static FUNCTION: std::sync::OnceLock<Option<Mallinfo2>> = std::sync::OnceLock::new();
    *FUNCTION.get_or_init(|| {
        // Resolve lazily so an optional metric does not raise the node's minimum
        // glibc version when collection is disabled or the symbol is absent.
        // SAFETY: the name is NUL-terminated; RTLD_DEFAULT searches loaded libs.
        let symbol = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"mallinfo2".as_ptr()) };
        if symbol.is_null() {
            None
        } else {
            // SAFETY: glibc exports mallinfo2 with this exact C signature, and
            // remains loaded for the lifetime of the process.
            Some(unsafe { std::mem::transmute::<*mut libc::c_void, Mallinfo2>(symbol) })
        }
    })
}

fn available() -> bool {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        mallinfo2_function().is_some()
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        false
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn allocated_bytes() -> Option<u64> {
    // SAFETY: glibc provides this thread-safe function without pointer arguments.
    let info = unsafe { mallinfo2_function()?() };
    // Large allocations made through malloc can be separate mmap regions and
    // are reported outside uordblks. Thread caches still count as in use: this
    // is allocator accounting, not an exact sum of live application objects.
    info.uordblks.checked_add(info.hblkhd).and_then(|bytes| u64::try_from(bytes).ok())
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn allocated_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_status_distinguishes_zero_from_missing_and_checks_units() {
        let usage = parse_linux_status("Name:\tnode\nVmRSS:\t123456 kB\nVmSwap:\t0 kB\n");
        assert_eq!(usage.rss_bytes, Some(123456 * 1024));
        assert_eq!(usage.swap_bytes, Some(0));
        let usage = parse_linux_status("VmRSS: 1 MB\n");
        assert_eq!(usage.rss_bytes, None);
        assert_eq!(usage.swap_bytes, None);
        let usage = parse_linux_status("VmRSS: 18446744073709551615 kB\nVmSwap: invalid kB\n");
        assert_eq!(usage.rss_bytes, None);
        assert_eq!(usage.swap_bytes, None);
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn current_process_has_resident_memory() {
        let usage = read();
        assert!(usage.rss_bytes.is_some_and(|bytes| bytes > 0));
        #[cfg(target_os = "linux")]
        assert!(usage.swap_bytes.is_some());
        #[cfg(target_os = "macos")]
        assert!(usage.swap_bytes.is_none());
    }

    #[test]
    fn allocator_interval_is_opt_in_and_rejects_invalid_settings() {
        assert_eq!(interval(None).unwrap(), None);
        assert_eq!(interval(Some("0")).unwrap(), None);
        assert_eq!(interval(Some("60")).unwrap(), Some(Duration::from_secs(60)));
        assert!(interval(Some("-1")).is_err());
        assert!(interval(Some("invalid")).is_err());
    }

    #[test]
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn allocator_accounts_for_live_allocation() {
        let body = vec![1u8; 1024 * 1024];
        assert!(allocated_bytes().is_some_and(|bytes| bytes >= body.len() as u64));
        std::hint::black_box(body);
    }
}
