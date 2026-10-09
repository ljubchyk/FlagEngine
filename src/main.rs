mod audit_repo;
mod cache;
mod db;
mod domain;
mod flag_repo;
mod handlers;
mod http;
mod outbox_repo;
mod service;
mod worker;
mod config;

use std::{sync::Arc, time::Duration};

use crate::{cache::FlagCache, flag_repo::FlagFilter, http::AppState, service::FlagService};

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("connection pool error: {0}")]
    Pool(#[from] r2d2::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), StartupError> {
    let config = config::Config::from_env()?;

    let conn = db::init(&config.db_path)?;

    let cache = Arc::new(FlagCache::new());
    let flags = flag_repo::find_all(&conn, FlagFilter::Active)?;

    cache.hydrate(
        flags
            .into_iter()
            .map(|flag| (flag.key().to_owned(), flag.is_enabled())),
    );

    worker::spawn_outbox_worker(&config.db_path, Duration::from_millis(500), cache.clone())?;

    let pool = db::create_pool(&config.db_path)?;
    let state = AppState {
        service: Arc::new(FlagService::new(pool, cache)),
        admin_key: Arc::from(config.admin_key),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("FlagEngine listening on {}", listener.local_addr()?);

    axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c")
}
