pub mod handlers;
pub mod ws;

use crate::history::RunHistoryStore;
use crate::registry::PipelineRegistry;
use crate::scheduler::PipelineCommand;
use crate::state::LogBuffer;
use crate::watcher::ReloadDebounce;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<RwLock<AppStateInner>>,
    pub controls: Arc<RwLock<HashMap<String, mpsc::Sender<PipelineCommand>>>>,
    pub history: RunHistoryStore,
    pub reload_debounce: ReloadDebounce,
    /// `Some` in directory mode — lets the API and file watcher register
    /// brand-new pipelines at runtime. `None` in legacy single-file mode.
    pub registry: Option<PipelineRegistry>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PipelineStatus {
    Running,
    Idle,
    Paused,
    Stopped,
    Error(String),
    /// Waiting on a `depends_on` dependency that hasn't (yet, or freshly
    /// enough) succeeded. Set right before a scheduled tick would otherwise
    /// run; a manual trigger still bypasses this. Cleared automatically the
    /// next time `set_pipeline_status` is called with any other status
    /// (e.g. once the tick actually runs and moves to `Running`).
    Blocked(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineSnapshot {
    pub id: String,
    pub status: PipelineStatus,
    pub total_rows: u64,
    pub total_errors: u64,
    pub config_path: String,
    pub schedule: String,
    pub error_kind: Option<String>,
}

#[derive(Debug)]
pub struct AppStateInner {
    pub started_at: Instant,
    pub logs: LogBuffer,
    pub pipelines: HashMap<String, PipelineSnapshot>,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub uptime_secs: u64,
    pub total_rows: u64,
    pub total_errors: u64,
    pub pipelines: Vec<PipelineSnapshot>,
}

impl AppState {
    pub fn new(
        history: RunHistoryStore,
        reload_debounce: ReloadDebounce,
        registry: Option<PipelineRegistry>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(AppStateInner {
                started_at: Instant::now(),
                logs: LogBuffer::new(100),
                pipelines: HashMap::new(),
            })),
            controls: Arc::new(RwLock::new(HashMap::new())),
            history,
            reload_debounce,
            registry,
        }
    }

    pub fn register_control(&self, id: String, tx: mpsc::Sender<PipelineCommand>) {
        if let Ok(mut map) = self.controls.write() {
            map.insert(id, tx);
        }
    }

    pub fn send_command(&self, id: &str, cmd: PipelineCommand) -> Result<(), String> {
        let map = self.controls.read().unwrap();
        let tx = map
            .get(id)
            .ok_or_else(|| format!("Unknown pipeline '{}'", id))?;
        tx.try_send(cmd)
            .map_err(|e| format!("Failed to send command: {}", e))
    }

    pub fn register_pipeline(
        &self,
        id: String,
        config_path: String,
        schedule: String,
        total_rows: u64,
        total_errors: u64,
    ) {
        if let Ok(mut inner) = self.inner.write() {
            inner.pipelines.insert(
                id.clone(),
                PipelineSnapshot {
                    id,
                    status: PipelineStatus::Idle,
                    total_rows,
                    total_errors,
                    config_path,
                    schedule,
                    error_kind: None,
                },
            );
        }
    }

    /// Removes the pipeline from the live set and drops its control
    /// sender, which closes `WorkerContext.cmd_rx` — the worker's
    /// `cmd_rx.recv()` then returns `None` and `run_pipeline_worker`
    /// exits its loop on its own, no separate shutdown command needed.
    /// Returns `false` if the id wasn't registered.
    pub fn unregister_pipeline(&self, id: &str) -> bool {
        let existed = self
            .inner
            .write()
            .map(|mut inner| inner.pipelines.remove(id).is_some())
            .unwrap_or(false);
        if let Ok(mut controls) = self.controls.write() {
            controls.remove(id);
        }
        existed
    }

    pub fn config_path(&self, id: &str) -> Option<String> {
        let inner = self.inner.read().unwrap();
        inner.pipelines.get(id).map(|p| p.config_path.clone())
    }

    /// (id, config_path) for every currently registered pipeline. Used to
    /// re-validate depends_on integrity against the live set at hot-reload
    /// time, not just against whatever was loaded at startup.
    pub fn all_config_paths(&self) -> Vec<(String, String)> {
        let inner = self.inner.read().unwrap();
        inner
            .pipelines
            .values()
            .map(|p| (p.id.clone(), p.config_path.clone()))
            .collect()
    }

    pub fn update_pipeline_schedule(&self, id: &str, schedule: String) {
        if let Ok(mut inner) = self.inner.write() {
            if let Some(p) = inner.pipelines.get_mut(id) {
                p.schedule = schedule;
            }
        }
    }

    pub fn get_pipeline_status(&self, id: &str) -> Option<PipelineStatus> {
        let inner = self.inner.read().unwrap();
        inner.pipelines.get(id).map(|p| p.status.clone())
    }

    pub fn log(&self, line: String) {
        if let Ok(mut inner) = self.inner.write() {
            inner.logs.push(line);
        }
    }

    pub fn set_pipeline_status(&self, id: &str, status: PipelineStatus) {
        if let Ok(mut inner) = self.inner.write() {
            if let Some(p) = inner.pipelines.get_mut(id) {
                if !matches!(status, PipelineStatus::Error(_)) {
                    p.error_kind = None;
                }
                p.status = status;
            }
        }
    }

    pub fn set_pipeline_error(&self, id: &str, message: String, kind: &'static str) {
        if let Ok(mut inner) = self.inner.write() {
            if let Some(p) = inner.pipelines.get_mut(id) {
                p.error_kind = Some(kind.to_string());
                p.status = PipelineStatus::Error(message);
            }
        }
    }

    pub fn add_pipeline_rows(&self, id: &str, count: u64) {
        if let Ok(mut inner) = self.inner.write() {
            if let Some(p) = inner.pipelines.get_mut(id) {
                p.total_rows += count;
            }
        }
    }

    pub fn add_pipeline_error(&self, id: &str) {
        if let Ok(mut inner) = self.inner.write() {
            if let Some(p) = inner.pipelines.get_mut(id) {
                p.total_errors += 1;
            }
        }
    }

    pub fn get_status_response(&self) -> StatusResponse {
        let inner = self.inner.read().unwrap();
        let mut pipelines: Vec<_> = inner.pipelines.values().cloned().collect();
        pipelines.sort_by(|a, b| a.id.cmp(&b.id));
        let total_rows = pipelines.iter().map(|p| p.total_rows).sum();
        let total_errors = pipelines.iter().map(|p| p.total_errors).sum();
        StatusResponse {
            uptime_secs: inner.started_at.elapsed().as_secs(),
            total_rows,
            total_errors,
            pipelines,
        }
    }

    pub fn get_logs(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.logs.get_all()
    }
}

pub async fn start_server(
    state: AppState,
    port: u16,
    auth_config: Option<crate::auth::BasicAuthConfig>,
) -> Result<(), std::io::Error> {
    use axum::{
        Router, middleware,
        routing::{delete, get, post},
    };
    use tower_http::cors::CorsLayer;

    let auth_state = Arc::new(auth_config);

    let app = Router::new()
        .route("/", get(handlers::dashboard))
        .route("/api/status", get(handlers::status))
        .route("/api/pipelines", post(handlers::create_pipeline))
        .route("/api/pipelines/:id", delete(handlers::delete_pipeline))
        .route("/api/logs", get(handlers::logs))
        .route("/api/history", get(handlers::all_history))
        .route("/api/pipelines/:id/pause", post(handlers::pause))
        .route("/api/pipelines/:id/resume", post(handlers::resume))
        .route("/api/pipelines/:id/stop", post(handlers::stop))
        .route("/api/pipelines/:id/run", post(handlers::run_once))
        .route(
            "/api/pipelines/:id/config",
            get(handlers::get_config).put(handlers::put_config),
        )
        .route("/api/pipelines/:id/history", get(handlers::pipeline_history))
        .route("/ws/logs", get(ws::ws_handler))
        .layer(CorsLayer::permissive())
        .layer(middleware::from_fn_with_state(
            auth_state,
            crate::auth::require_basic_auth,
        ))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            log::error!(
                "Port {} is already in use (another process is listening). \
                 Web UI will not start. Try a different port: \
                 cargo run -- <config> <state> <port>",
                port
            );
            return Err(e);
        }
        Err(e) => return Err(e),
    };

    log::info!("Web UI running at http://localhost:{}", port);
    axum::serve(listener, app).await
}
