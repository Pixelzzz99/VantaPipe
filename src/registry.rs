use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;

use crate::config::{PipelineConfig, validate_depends_on_for_reload};
use crate::error::EtlError;
use crate::history::RunHistoryStore;
use crate::runtime::build_pipeline;
use crate::scheduler::{WorkerContext, run_pipeline_worker};
use crate::web::AppState;

/// Directory-mode context needed to register a brand-new pipeline at
/// runtime — via `POST /api/pipelines` or by dropping a new config file
/// into the watched directory — without restarting the process.
///
/// Only meaningful in directory mode (a single fixed config file has
/// nowhere to add new pipelines to); `AppState::registry` is `None` in
/// legacy single-file mode.
#[derive(Clone)]
pub struct PipelineRegistry {
    pub config_dir: PathBuf,
    pub state_dir: PathBuf,
}

impl PipelineRegistry {
    pub fn new(config_dir: PathBuf, state_dir: PathBuf) -> Self {
        Self {
            config_dir,
            state_dir,
        }
    }
}

/// Build, register, and spawn a worker for a brand-new pipeline. Used at
/// startup for pre-existing config files, and at runtime both by the
/// `POST /api/pipelines` handler and by the file watcher when it sees an
/// unrecognized `*.json` file appear in a watched directory.
///
/// Rejects (without side effects) if `id` is already registered, or if the
/// new pipeline's `depends_on` references an unknown id or would close a
/// dependency cycle against the *currently live* set of pipelines.
///
/// `state_path` is precomputed by the caller (rather than derived here from
/// a `PipelineRegistry`) so this same function serves both directory mode
/// (`{state_dir}/{id}.json`, see `PipelineRegistry`) and legacy single-file
/// mode (one fixed state file, see `Args::state_path_for`) at startup.
pub async fn register_and_spawn_pipeline(
    app_state: &AppState,
    history: &RunHistoryStore,
    id: String,
    path: PathBuf,
    state_path: String,
    config: PipelineConfig,
) -> Result<(), EtlError> {
    if app_state.config_path(&id).is_some() {
        return Err(EtlError::ConfigError(format!(
            "Pipeline '{}' is already registered",
            id
        )));
    }

    let other_paths = app_state.all_config_paths();
    validate_depends_on_for_reload(&id, &config.depends_on, &other_paths)?;

    if let Some(parent) = Path::new(&state_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    history.load_pipeline(&id);

    let built = build_pipeline(
        id.clone(),
        path.to_string_lossy().into_owned(),
        state_path,
        &config,
    )
    .await?;

    let rows = {
        let ps = built.persistent_state.lock().unwrap();
        (ps.total_rows_processed, ps.total_errors)
    };

    app_state.register_pipeline(
        built.id.clone(),
        built.config_path.clone(),
        built.schedule_label.clone(),
        rows.0,
        rows.1,
    );

    log::info!(
        "[{}] Schedule: {} (config {})",
        built.id, built.schedule_label, built.config_path
    );

    let (cmd_tx, cmd_rx) = mpsc::channel(32);
    app_state.register_control(built.id.clone(), cmd_tx);

    let ctx = WorkerContext {
        id: built.id,
        config_path: built.config_path,
        state_path: built.state_path,
        pipeline: Arc::new(RwLock::new(Arc::new(built.pipeline))),
        schedule: Arc::new(RwLock::new(built.schedule)),
        depends_on: Arc::new(RwLock::new(config.depends_on.unwrap_or_default())),
        alert_webhook: Arc::new(RwLock::new(config.alert_webhook)),
        pipeline_state: built.pipeline_state,
        persistent_state: built.persistent_state,
        app_state: app_state.clone(),
        history: history.clone(),
        cmd_rx,
        dep_rx: app_state.subscribe_success(),
    };

    tokio::spawn(run_pipeline_worker(ctx));

    Ok(())
}
