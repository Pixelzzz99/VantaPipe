use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
};

use crate::config::{load_config, parse_cron_schedule};
use crate::scheduler::PipelineCommand;
use crate::web::AppState;

pub async fn dashboard() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

pub async fn status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let response = state.get_status_response();
    Json(serde_json::json!({
        "uptime_secs": response.uptime_secs,
        "total_rows": response.total_rows,
        "total_errors": response.total_errors,
        "pipelines": response.pipelines,
    }))
}

pub async fn logs(State(state): State<AppState>) -> Json<serde_json::Value> {
    let logs = state.get_logs();
    Json(serde_json::json!({"logs": logs}))
}

pub async fn all_history(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "runs": state.history.all() }))
}

pub async fn pipeline_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "runs": state.history.for_pipeline(&id) }))
}

pub async fn pause(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    command_response(&state, &id, PipelineCommand::Pause)
}

pub async fn resume(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    command_response(&state, &id, PipelineCommand::Resume)
}

pub async fn stop(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    command_response(&state, &id, PipelineCommand::Stop)
}

pub async fn run_once(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    command_response(&state, &id, PipelineCommand::Trigger)
}

/// Register a brand-new pipeline at runtime, without a restart. Only
/// available in directory mode (`state.registry` is `Some`). The request
/// body is the full pipeline config JSON and must include an explicit
/// `"id"` field — that's what the new config file on disk is named after.
pub async fn create_pipeline(
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    let Some(registry) = state.registry.clone() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Dynamic pipeline registration requires directory mode (started with a config directory, not a single file)"
            })),
        )
            .into_response();
    };

    let parsed: Result<crate::config::PipelineConfig, _> = serde_json::from_str(&body);
    let Ok(config) = parsed else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid JSON config"})),
        )
            .into_response();
    };

    let Some(id) = config.id.clone() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "config must include an explicit \"id\" field to create a new pipeline"
            })),
        )
            .into_response();
    };

    if let Some(ref expr) = config.schedule {
        if let Err(e) = parse_cron_schedule(expr) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    }

    if state.config_path(&id).is_some() {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": format!("Pipeline '{}' already exists", id)})),
        )
            .into_response();
    }

    let other_paths = state.all_config_paths();
    if let Err(e) =
        crate::config::validate_depends_on_for_reload(&id, &config.depends_on, &other_paths)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    let path = registry.config_dir.join(format!("{}.json", id));
    if path.exists() {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": format!("Config file {} already exists", path.display())
            })),
        )
            .into_response();
    }

    if let Err(e) = std::fs::write(&path, &body) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    // Suppress the file watcher's own attempt to pick up the file we just
    // wrote as an "unrecognized new pipeline" — we're registering it here.
    crate::watcher::mark_reloaded(&state.reload_debounce, &id);

    let state_path = registry
        .state_dir
        .join(format!("{}.json", id))
        .to_string_lossy()
        .into_owned();

    match crate::registry::register_and_spawn_pipeline(
        &state,
        &state.history,
        id.clone(),
        path.clone(),
        state_path,
        config,
    )
    .await
    {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"ok": true, "id": id})),
        )
            .into_response(),
        Err(e) => {
            // Registration failed after the file was written (e.g. a
            // concurrent request registered the same id first) — remove it
            // rather than leave an orphaned, unregistered config on disk.
            let _ = std::fs::remove_file(&path);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }
}

/// Deregister a pipeline at runtime: stops its worker (by dropping its
/// control channel — see `AppState::unregister_pipeline`), removes it from
/// the dashboard/API, and deletes its config + state files. Only available
/// in directory mode, mirroring `create_pipeline`.
pub async fn delete_pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let Some(registry) = state.registry.clone() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Dynamic pipeline deregistration requires directory mode (started with a config directory, not a single file)"
            })),
        )
            .into_response();
    };

    if state.config_path(&id).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "pipeline not found"})),
        )
            .into_response();
    }

    state.unregister_pipeline(&id);

    let config_path = registry.config_dir.join(format!("{}.json", id));
    let _ = std::fs::remove_file(&config_path);
    let state_path = registry.state_dir.join(format!("{}.json", id));
    let _ = std::fs::remove_file(&state_path);

    Json(serde_json::json!({"ok": true, "id": id})).into_response()
}

pub async fn get_config(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let Some(path) = state.config_path(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "pipeline not found"})),
        )
            .into_response();
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => Json(serde_json::json!({"id": id, "path": path, "config": text })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn put_config(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: String,
) -> impl IntoResponse {
    let Some(path) = state.config_path(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "pipeline not found"})),
        )
            .into_response();
    };

    let parsed: Result<crate::config::PipelineConfig, _> = serde_json::from_str(&body);
    let Ok(config) = parsed else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid JSON config"})),
        )
            .into_response();
    };

    if let Some(ref expr) = config.schedule {
        if let Err(e) = parse_cron_schedule(expr) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    }

    let other_paths = state.all_config_paths();
    if let Err(e) =
        crate::config::validate_depends_on_for_reload(&id, &config.depends_on, &other_paths)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    if let Err(e) = std::fs::write(&path, &body) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    if let Err(e) = load_config(&path) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    crate::watcher::mark_reloaded(&state.reload_debounce, &id);

    match state.send_command(&id, PipelineCommand::Reload) {
        Ok(()) => Json(serde_json::json!({"ok": true, "id": id})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )
            .into_response(),
    }
}

fn command_response(
    state: &AppState,
    id: &str,
    cmd: PipelineCommand,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.send_command(id, cmd) {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"ok": true, "id": id}))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::parse_cron_schedule;

    #[test]
    fn test_bad_cron_rejected() {
        assert!(parse_cron_schedule("not-cron").is_err());
        assert!(parse_cron_schedule("0 */5 * * * *").is_ok());
    }
}
