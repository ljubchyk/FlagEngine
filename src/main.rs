mod app_error;
mod audit_repo;
mod cache;
mod config;
mod db;
mod domain;
mod flag_repo;
mod handlers;
mod http;
mod outbox_repo;
mod service;
mod worker;
mod admin;

use std::{error::Error, sync::Arc};

use tokio::signal::unix::{SignalKind, signal};

use crate::{cache::FlagCache, flag_repo::FlagFilter, http::AppState, service::FlagService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();
    
    let config = config::Config::from_env()?;

    let conn = db::init(&config.db_path)?;

    let cache = Arc::new(FlagCache::new());
    let flags = flag_repo::find_all(&conn, FlagFilter::Active)?;

    cache.hydrate(
        flags
            .into_iter()
            .map(|flag| (flag.key().to_owned(), flag.is_enabled())),
    );

    let (waker, wake_rx) = std::sync::mpsc::sync_channel::<()>(1);

    worker::spawn_outbox_worker(&config.db_path, config.poll_interval, cache, wake_rx)?;

    let pool = db::create_pool(&config.db_path)?;
    let state = AppState {
        service: Arc::new(FlagService::new(pool, waker)),
        admin_key: Arc::from(config.admin_key),
    };

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    println!("FlagEngine listening on {}", listener.local_addr()?);

    axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");

    tokio::select! {
        res = ctrl_c => {
            res.expect("failed to listen for ctrl-c")
        }
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c")
}
