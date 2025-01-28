pub trait SpawnCritical {
    fn spawn_critical<F, T>(self, f: F) -> std::io::Result<std::thread::JoinHandle<T>>
    where
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static;

    fn spawn_scoped_critical<'scope, 'env, F, T>(
        self,
        scope: &'scope std::thread::Scope<'scope, 'env>,
        f: F,
    ) -> std::io::Result<std::thread::ScopedJoinHandle<'scope, T>>
    where
        F: FnOnce() -> anyhow::Result<T> + Send + 'scope,
        T: Send + 'scope;
}

impl SpawnCritical for std::thread::Builder {
    fn spawn_critical<F, T>(self, f: F) -> std::io::Result<std::thread::JoinHandle<T>>
    where
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.spawn(move || match f() {
            Ok(e) => e,
            Err(e) => {
                let name = std::thread::current().name().unwrap_or("unknown").to_owned();
                tracing::error!("Critical <{name}> thread exited with an error: {e}");
                std::process::exit(101);
            }
        })
    }

    fn spawn_scoped_critical<'scope, 'env, F, T>(
        self,
        scope: &'scope std::thread::Scope<'scope, 'env>,
        f: F,
    ) -> std::io::Result<std::thread::ScopedJoinHandle<'scope, T>>
    where
        F: FnOnce() -> anyhow::Result<T> + Send + 'scope,
        T: Send + 'scope,
    {
        self.spawn_scoped(scope, move || match f() {
            Ok(e) => e,
            Err(e) => {
                let name = std::thread::current().name().unwrap_or("unknown").to_owned();
                tracing::error!("Critical scoped <{name}> thread exited with an error: {e}");
                std::process::exit(101);
            }
        })
    }
}
