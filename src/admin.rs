use axum::{
    Form, Router,
    extract::{Path, Query, State},
    response::{Html, Redirect},
    routing::{get, post},
};
use sailfish::Template;
use serde::Deserialize;
use time::OffsetDateTime;

use crate::{
    app_error::AppError::{self, FlagNotFound},
    domain::{DomainEvent, EventPayload, Flag},
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
        .route("/admin/flags/{key}/audit", get(audit_page))
}

fn format_utc(ms: i64) -> String {
    let Ok(dt) = OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000) else {
        return ms.to_string();
    };

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
    )
}

struct FlagView {
    key: String,
    enabled: bool,
    archived: bool,
    updated_at: String,
}

impl From<&Flag> for FlagView {
    fn from(flag: &Flag) -> Self {
        let updated_at = match OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(flag.updated_at()) * 1_000_000,
        ) {
            Ok(dt) => format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}UTC",
                dt.year(),
                u8::from(dt.month()),
                dt.day(),
                dt.hour(),
                dt.minute(),
                dt.second(),
            ),
            Err(_) => flag.updated_at().to_string(),
        };

        Self {
            key: flag.key().to_owned(),
            enabled: flag.is_enabled(),
            archived: flag.is_archived(),
            updated_at: updated_at,
        }
    }
}

#[derive(Template)]
#[template(path = "flags.stpl")]
struct FlagsView {
    status: &'static str,
    flags: Vec<FlagView>,
}

async fn flags_page(
    State(state): State<AppState>,
    Query(query): Query<ListFlagsQuery>,
) -> Result<Html<String>, AppError> {
    let service = state.service.clone();
    let filter = FlagFilter::from(query.status);

    let flags = tokio::task::spawn_blocking(move || service.find_flags(filter)).await??;

    let page = FlagsView {
        status: query.status.as_str(),
        flags: flags.iter().map(FlagView::from).collect(),
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
    tokio::task::spawn_blocking(move || service.enable_flag(key, "admin".to_owned())).await??;
    Ok(Redirect::to("/admin"))
}

async fn disable_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Redirect, AppError> {
    let service = state.service.clone();
    tokio::task::spawn_blocking(move || service.disable_flag(key, "admin".to_owned())).await??;
    Ok(Redirect::to("/admin"))
}

async fn archive_flag(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Redirect, AppError> {
    let service = state.service.clone();
    tokio::task::spawn_blocking(move || service.archive_flag(key, "admin".to_owned())).await??;
    Ok(Redirect::to("/admin"))
}

#[derive(Deserialize)]
struct AuditQuery {
    before: Option<i64>,
}

struct EventView {
    actor: String,
    action: &'static str,
    created_at: String,
}

#[derive(Template)]
#[template(path = "audit.stpl")]
struct AuditView {
    flag: FlagView,
    events: Vec<EventView>,
    next_before: Option<i64>,
}

async fn audit_page(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(query): Query<AuditQuery>,
) -> Result<Html<String>, AppError> {
    let service = state.service.clone();

    let (flag, audit) = tokio::task::spawn_blocking(move || {
        let flag = service
            .find_flag(&key)?
            .ok_or_else(|| FlagNotFound(key.clone()))?;

        let audit = service.find_audit(&key, query.before, 20)?;

        Ok::<_, AppError>((flag, audit))
    })
    .await??;

    let action = |e: &DomainEvent| match e.payload {
        EventPayload::FlagCreated { .. } => "Created",
        EventPayload::FlagEnabled => "Enabled",
        EventPayload::FlagDisabled => "Disabled",
        EventPayload::FlagArchived => "Archived",
    };

    let page = AuditView {
        flag: FlagView::from(&flag),
        events: audit
            .events
            .iter()
            .map(|e| EventView {
                actor: e.actor.clone(),
                action: action(e),
                created_at: format_utc(e.occurred_at),
            })
            .collect(),
        next_before: audit.next_before,
    };
    Ok(Html(page.render()?))
}
