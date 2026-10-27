use axum::{
    Form, Router, extract::{Path, Query, State}, response::{Html, Redirect}, routing::{get, post},
};
use sailfish::Template;
use serde::Deserialize;

use crate::{
    app_error::AppError,
    domain::Flag,
    flag_repo::FlagFilter,
    http::{AppState, ListFlagsQuery},
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/admin", get(flags_page))
        .route("/admin/flags", post(create_flag))
        .route("/admin/flags/{key}/enable", post(enable_flag))
        .route("/admin/flags/{key}/disable", post(disable_flag))
        .route("/admin/flags/{key}/archive", post(archive_flag))
}

#[derive(Template)]
#[template(path = "flags.stpl")]
struct FlagsPage {
    status: &'static str,
    flags: Vec<Flag>,
}

async fn flags_page(
    State(state): State<AppState>,
    Query(query): Query<ListFlagsQuery>,
) -> Result<Html<String>, AppError> {
    let service = state.service.clone();
    let filter = FlagFilter::from(query.status);

    let flags = tokio::task::spawn_blocking(move || service.find_flags(filter)).await??;

    let page = FlagsPage {
        status: query.status.as_str(),
        flags,
    };
    Ok(Html(page.render()?))
}

#[derive(Deserialize)]
struct CreateForm {
    key: String,
}

async fn create_flag(
    State(state): State<AppState>,
    Form(form): Form<CreateForm>,
) -> Result<Redirect, AppError> {
    let service = state.service.clone();
    
    tokio::task::spawn_blocking(move || service.create_flag(form.key, "admin".to_owned()))
        .await??;

    Ok(Redirect::to("/admin"))
}

async fn enable_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Redirect, AppError> {
    let service = state.service.clone();
    tokio::task::spawn_blocking(move || service.enable_flag(key, "admin".to_owned()))
        .await??;
    Ok(Redirect::to("/admin"))
}

async fn disable_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Redirect, AppError> {
    let service = state.service.clone();
    tokio::task::spawn_blocking(move || service.disable_flag(key, "admin".to_owned()))
        .await??;
    Ok(Redirect::to("/admin"))
}

async fn archive_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Redirect, AppError> {
    let service = state.service.clone();
    tokio::task::spawn_blocking(move || service.archive_flag(key, "admin".to_owned()))
        .await??;
    Ok(Redirect::to("/admin"))
}
