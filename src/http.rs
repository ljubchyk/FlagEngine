use axum::{
    Json, Router,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use subtle::ConstantTimeEq;

use crate::service::{FlagService, ServiceError};

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    Service(#[from] ServiceError),

    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::Service(ServiceError::FlagNotFound(_)) => StatusCode::NOT_FOUND,
            ApiError::Service(ServiceError::DuplicateKey(_)) => StatusCode::CONFLICT,
            ApiError::Service(ServiceError::Domain(_)) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::Database(_) | ServiceError::Pool(_))
            | ApiError::Join(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // Внутрішні помилки клієнту не показуємо, лише пишемо в stderr.
        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            eprintln!("[http] internal error: {self}");
            "internal server error".to_owned()
        } else {
            self.to_string()
        };

        (status, Json(ErrorBody { error: message })).into_response()
    }
}

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<FlagService>,
    pub admin_key: Arc<String>,
}

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/flags", post(create_flag))
        .route("/flags/{key}/toggle", post(toggle_flag))
        .route("/flags/{key}/archive", post(archive_flag))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_key,
        ));

    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
}

async fn require_admin_key(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    let authorized = match provided {
        Some(key) => bool::from(key.as_bytes().ct_eq(state.admin_key.as_bytes())),
        None => false,
    };

    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorBody {
                error: "unauthorized".to_owned(),
            }),
        )
            .into_response();
    }

    next.run(req).await
}

pub async fn health() -> &'static str {
    "ok"
}

#[derive(Deserialize)]
struct CreateFlagRequest {
    key: String,
    actor: String,
}

async fn create_flag(
    State(state): State<AppState>,
    Json(body): Json<CreateFlagRequest>,
) -> Result<StatusCode, ApiError> {
    let service = state.service.clone();

    tokio::task::spawn_blocking(move || service.create_flag(body.key, body.actor)).await??;

    Ok(StatusCode::CREATED)
}

#[derive(Deserialize)]
struct ToggleFlagRequest {
    enabled: bool,
    actor: String,
}

async fn toggle_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<ToggleFlagRequest>,
) -> Result<StatusCode, ApiError> {
    let service = state.service.clone();

    tokio::task::spawn_blocking(move || service.toggle_flag(key, body.enabled, body.actor))
        .await??;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ArchiveFlagRequest {
    actor: String,
}

async fn archive_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<ArchiveFlagRequest>,
) -> Result<StatusCode, ApiError> {
    let service = state.service.clone();

    tokio::task::spawn_blocking(move || service.archive_flag(key, body.actor)).await??;

    Ok(StatusCode::NO_CONTENT)
}
