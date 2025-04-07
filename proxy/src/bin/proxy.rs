// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.

fn main() -> anyhow::Result<()> {
    proxy::tracing::init_tracing();
    if let Err(error) = proxy::server::run() {
        tracing::error!("{:#?}", error);
        return Err(error);
    }
    tracing::info!("Server stopped gracefully.");
    Ok(())
}
