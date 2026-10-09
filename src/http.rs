use axum::{Router, routing::get};
use std::sync::Arc;

use crate::service::FlagService;

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
