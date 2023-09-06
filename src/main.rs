use std::{net::SocketAddr, sync::Arc};

use structured_logger::{async_json::new_writer, Builder};
use tokio::{io, signal};

mod api;
mod conf;
mod crypto;
mod db;
mod router;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().expect(".env file not found");
    let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));

    Builder::with_level(cfg.log.level.as_str())
        .with_target_writer("*", new_writer(io::stdout()))
        .init();

    log::debug!("{:?}", cfg);

    let server_cfg = cfg.server.clone();
    let server_env = cfg.env.clone();
    let (app_state, app) = router::new(cfg).await?;

    let addr = SocketAddr::from(([0, 0, 0, 0], server_cfg.port));
    log::info!(
        "{}@{} start {} at {}",
        api::APP_NAME,
        api::APP_VERSION,
        server_env,
        &addr
    );
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_signal(app_state, server_cfg.graceful_shutdown))
        .await?;

    Ok(())
}

async fn shutdown_signal(_app: Arc<api::AppState>, _wait_secs: usize) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    log::info!("signal received, Goodbye!");
}
