use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tower_http::timeout::TimeoutLayer;

use crate::{app_error::AppError, domain::Flag, flag_repo::FlagFilter, service::FlagService};

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self{
            AppError::InvalidKey(_) => StatusCode::BAD_REQUEST,
            AppError::Archived => StatusCode::CONFLICT,
            AppError::DuplicateKey(_) => StatusCode::CONFLICT,
            AppError::FlagNotFound(_) => StatusCode::NOT_FOUND,
            AppError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Pool(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Join(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // Внутрішні помилки клієнту не показуємо, лише пишемо в stderr.
        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            eprintln!("[http] internal error: {self}");
            status.canonical_reason().unwrap_or("internal server error").to_owned()
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
        .route("/flags", post(create_flag).get(list_flags))
        .route("/flags/{key}", get(get_flag))
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
) -> Result<StatusCode, AppError> {
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
) -> Result<StatusCode, AppError> {
    let service = state.service.clone();

    tokio::task::spawn_blocking(move || service.enable_flag(key, body.actor)).await??;

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
) -> Result<StatusCode, AppError> {
    let service = state.service.clone();

    tokio::task::spawn_blocking(move || service.disable_flag(key, body.actor)).await??;

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
) -> Result<StatusCode, AppError> {
    let service = state.service.clone();

    tokio::task::spawn_blocking(move || service.archive_flag(key, body.actor)).await??;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum StatusQuery {
    #[default]
    Active,
    Archived,
    All,
}

#[derive(Deserialize)]
struct ListFlagsQuery {
    #[serde(default)]
    status: StatusQuery,
}

#[derive(Serialize)]
struct FlagResponse {
    key: String,
    enabled: bool,
    archived: bool,
    updated_at: i64,
}

impl From<&Flag> for FlagResponse {
    fn from(flag: &Flag) -> Self {
        Self {
            key: flag.key().to_owned(),
            enabled: flag.is_enabled(),
            archived: flag.is_archived(),
            updated_at: flag.updated_at(),
        }
    }
}

async fn get_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<FlagResponse>, AppError> {
    let service = state.service.clone();

    let key_clone = key.clone();
    let flag = tokio::task::spawn_blocking(move || service.find_flag(&key_clone)).await??;

    match flag {
        Some(flag) => Ok(Json(FlagResponse::from(&flag))),
        None => Err(AppError::FlagNotFound(key)),
    }
}

async fn list_flags(
    State(state): State<AppState>,
    Query(query): Query<ListFlagsQuery>,
) -> Result<Json<Vec<FlagResponse>>, AppError> {
    let filter = match query.status {
        StatusQuery::Active => FlagFilter::Active,
        StatusQuery::Archived => FlagFilter::Archived,
        StatusQuery::All => FlagFilter::All,
    };
    let service = state.service.clone();

    let flags = tokio::task::spawn_blocking(move || service.find_flags(filter)).await??;

    Ok(Json(flags.iter().map(FlagResponse::from).collect()))
}
