use std::sync::OnceLock;

const MAX_APPLY_THREADS: usize = 8;

// All blockchain threads share this bounded pool. Laziness avoids creating
// workers in repositories used only for production or snapshot operations.
// A separate Rayon registry keeps replay work out of the producer's queues;
// it does not reserve physical CPUs or prevent OS-level CPU competition.
static APPLY_POOL: OnceLock<Result<rayon::ThreadPool, rayon::ThreadPoolBuildError>> =
    OnceLock::new();

pub(super) fn get() -> anyhow::Result<&'static rayon::ThreadPool> {
    APPLY_POOL
        .get_or_init(|| {
            let threads = std::thread::available_parallelism()
                .map(|n| n.get().min(MAX_APPLY_THREADS))
                .unwrap_or(1);
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .thread_name(|index| format!("acct-apply-{index}"))
                .build()
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("Failed to create account apply pool: {error}"))
}

#[cfg(test)]
mod tests {
    use rayon::prelude::*;

    #[test]
    fn nested_parallel_apply_uses_reused_private_workers() -> anyhow::Result<()> {
        let apply = super::get()?;
        assert!(std::ptr::eq(apply, super::get()?));
        assert!((1..=super::MAX_APPLY_THREADS).contains(&apply.current_num_threads()));
        let producer = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|i| format!("producer-test-{i}"))
            .build()?;
        let names: Vec<_> = producer.install(|| {
            apply.install(|| {
                (0..300)
                    .into_par_iter()
                    .map(|_| std::thread::current().name().unwrap().to_owned())
                    .collect()
            })
        });
        assert!(names.iter().all(|name| name.starts_with("acct-apply-")));
        Ok(())
    }
}
