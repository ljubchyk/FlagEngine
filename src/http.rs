use axum::{
    Json, Router, extract::{DefaultBodyLimit, Path, Request, State}, http::{StatusCode, header}, middleware::{self, Next}, response::{IntoResponse, Response}, routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tower_http::timeout::TimeoutLayer;
use std::{sync::Arc, time::Duration};
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
        .route("/flags/{key}/enable", post(enable_flag))
        .route("/flags/{key}/disable", post(disable_flag))
        .route("/flags/{key}/archive", post(archive_flag))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_key,
        ));

    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(10),
        ))
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
struct EnableFlagRequest {
    actor: String,
}

async fn enable_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<EnableFlagRequest>,
) -> Result<StatusCode, ApiError> {
    let service = state.service.clone();

    tokio::task::spawn_blocking(move || service.enable_flag(key, body.actor))
        .await??;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct DisableFlagRequest {
    actor: String,
}

async fn disable_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<DisableFlagRequest>,
) -> Result<StatusCode, ApiError> {
    let service = state.service.clone();

    tokio::task::spawn_blocking(move || service.disable_flag(key, body.actor))
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
