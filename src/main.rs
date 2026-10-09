mod alert;
mod config;
mod error;
mod extractor;
mod history;
mod loader;
mod pipeline;
mod registry;
mod retry;
mod runtime;
mod s3_store;
mod scheduler;
mod state;
mod transformer;
mod types;
mod watcher;
mod web;

use std::path::{Path, PathBuf};

use config::{
    LoadedPipeline, load_config, load_configs_from_dir, resolve_pipeline_id, validate_depends_on,
};
use history::{RunHistoryStore, history_dir_from_state_arg};
use metrics_exporter_prometheus::PrometheusBuilder;
use registry::{PipelineRegistry, register_and_spawn_pipeline};
use watcher::{ReloadDebounce, spawn_config_watcher};
use web::{AppState, start_server};

#[tokio::main]
async fn main() {
    init_logging();

    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .unwrap_or_else(|e| {
            log::error!("Failed to install Prometheus recorder: {}", e);
            std::process::exit(1);
        });

    let args = parse_args();
    log::info!("Config: {}", args.config_path);
    log::info!("State: {}", args.state_display());
    log::info!(
        "Internal API (not for direct public exposure — front it with the Node gateway): http://localhost:{}",
        args.web_port
    );

    let loaded = load_pipelines(&args).unwrap_or_else(|e| {
        log::error!("Failed to load config(s): {}", e);
        std::process::exit(1);
    });

    let history_dir = history_dir_from_state_arg(&args.state_arg, args.legacy_state_file);
    let history = RunHistoryStore::new(history_dir);
    let reload_debounce: ReloadDebounce = Default::default();

    // Directory mode (and not the legacy single-state-file variant of it)
    // is the only case where it's meaningful to register brand-new
    // pipelines at runtime — a single fixed config file has nowhere to add
    // new pipelines to.
    let registry = if Path::new(&args.config_path).is_dir() && !args.legacy_state_file {
        Some(PipelineRegistry::new(
            PathBuf::from(&args.config_path),
            PathBuf::from(&args.state_arg),
        ))
    } else {
        None
    };

    let app_state = AppState::new(
        history.clone(),
        reload_debounce.clone(),
        registry.clone(),
        prometheus_handle,
    );

    let mut pipeline_count = 0usize;
    for item in loaded {
        let state_path = args.state_path_for(&item.id);
        let id = item.id.clone();
        register_and_spawn_pipeline(&app_state, &history, item.id, item.path, state_path, item.config)
            .await
            .unwrap_or_else(|e| {
                log::error!("[{}] Failed to build pipeline: {}", id, e);
                std::process::exit(1);
            });
        pipeline_count += 1;
    }

    let extra_watch_dirs = if registry.is_some() {
        vec![PathBuf::from(&args.config_path)]
    } else {
        vec![]
    };
    spawn_config_watcher(
        app_state.clone(),
        history,
        registry,
        extra_watch_dirs,
        reload_debounce,
    );
    spawn_web_server(app_state, args.web_port);

    log::info!("ETL Engine started with {} pipeline(s)", pipeline_count);

    // Pipeline workers and the web server run as detached background tasks
    // (new ones can be spawned any time via the API or the file watcher, so
    // there's no fixed set to join on) — block forever, exiting only on
    // process signal/kill.
    std::future::pending::<()>().await;
}

struct Args {
    config_path: String,
    state_arg: String,
    legacy_state_file: bool,
    web_port: u16,
}

impl Args {
    fn state_display(&self) -> &str {
        &self.state_arg
    }

    fn state_path_for(&self, pipeline_id: &str) -> String {
        if self.legacy_state_file {
            self.state_arg.clone()
        } else {
            Path::new(&self.state_arg)
                .join(format!("{}.json", pipeline_id))
                .to_string_lossy()
                .into_owned()
        }
    }
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "config/pipeline.json".into());

    let state_raw = args.get(2).cloned();
    let port_raw = args.get(3).cloned();

    let (state_arg, legacy_state_file, web_port) = match state_raw {
        Some(s) if s.ends_with(".json") && !Path::new(&s).is_dir() => {
            let port = port_raw
                .as_deref()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3000);
            (s, true, port)
        }
        Some(s) => {
            let port = port_raw
                .as_deref()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3000);
            (s, false, port)
        }
        None => {
            let config_is_dir = Path::new(&config_path).is_dir();
            if config_is_dir {
                ("etl_state".into(), false, 3000)
            } else {
                ("etl_state.json".into(), true, 3000)
            }
        }
    };

    Args {
        config_path,
        state_arg,
        legacy_state_file,
        web_port,
    }
}

fn load_pipelines(args: &Args) -> Result<Vec<LoadedPipeline>, crate::error::EtlError> {
    let path = Path::new(&args.config_path);
    let loaded = if path.is_dir() {
        load_configs_from_dir(&args.config_path)?
    } else {
        let config = load_config(&args.config_path)?;
        let path_buf = PathBuf::from(&args.config_path);
        let id = resolve_pipeline_id(&path_buf, &config);
        vec![LoadedPipeline {
            path: path_buf,
            id,
            config,
        }]
    };
    validate_depends_on(&loaded)?;
    Ok(loaded)
}

/// Plain text by default (today's behavior — readable for `cargo run`/
/// local dev). Set `LOG_FORMAT=json` for one-JSON-object-per-line output
/// (container/prod use, machine-parseable). Every `log::info!`/`warn!`/
/// `error!` call site elsewhere in the codebase is unaffected — this only
/// changes how a record is formatted into a line, not what gets logged.
fn init_logging() {
    let json = std::env::var("LOG_FORMAT")
        .map(|v| v == "json")
        .unwrap_or(false);
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    if json {
        builder.format(|buf, record| {
            use std::io::Write;
            let entry = serde_json::json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "level": record.level().to_string(),
                "target": record.target(),
                "message": record.args().to_string(),
            });
            writeln!(buf, "{}", entry)
        });
    }
    builder.init();
}

fn spawn_web_server(app_state: AppState, port: u16) {
    tokio::spawn(async move {
        if let Err(e) = start_server(app_state, port).await {
            log::error!("Web server stopped: {}", e);
        }
    });
}
