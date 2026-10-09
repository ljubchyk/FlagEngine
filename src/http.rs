use axum::{Router, routing::get};
use std::sync::Arc;

use crate::service::{FlagService, ServiceError};

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    Service(#[from] ServiceError),

    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<FlagService>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .with_state(state)
}

pub async fn health() -> &'static str {
    "ok"
}
