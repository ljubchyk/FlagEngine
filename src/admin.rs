use axum::{
    Router,
    extract::{Query, State},
    response::Html,
    routing::get,
};
use sailfish::Template;

use crate::{
    app_error::AppError, domain::Flag, flag_repo::FlagFilter, http::{AppState, ListFlagsQuery},
};

pub fn router() -> Router<AppState> {
    Router::new().route("/admin", get(flags_page))
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

    let page = FlagsPage { status: query.status.as_str(), flags };
    Ok(Html(page.render()?))
}