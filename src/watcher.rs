use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::config::{load_config, resolve_pipeline_id};
use crate::history::RunHistoryStore;
use crate::registry::{PipelineRegistry, register_and_spawn_pipeline};
use crate::scheduler::PipelineCommand;
use crate::web::AppState;

/// Shared "last fired at" timestamps, keyed by pipeline id (for reloads) or
/// by resolved id (for new-file registration). Written both by this watcher
/// and by `mark_reloaded` (called from the config-editor API), so an
/// API-triggered reload suppresses the redundant fs-change reload that the
/// resulting file write would otherwise also trigger.
pub type ReloadDebounce = Arc<Mutex<HashMap<String, Instant>>>;

const DEBOUNCE: Duration = Duration::from_millis(800);

/// Record that `id` was just reloaded outside the file watcher (e.g. via the
/// JSON editor's "Save & reload"), so the watcher's own change-detection
/// skips firing a second, redundant reload for the write that caused it.
/// Best-effort: a watcher poll that lands in the brief window before this
/// call still fires one extra (harmless) reload.
pub fn mark_reloaded(debounce: &ReloadDebounce, id: &str) {
    if let Ok(mut map) = debounce.lock() {
        map.insert(id.to_string(), Instant::now());
    }
}

fn should_fire(debounce: &ReloadDebounce, key: &str) -> bool {
    let now = Instant::now();
    let Ok(mut map) = debounce.lock() else {
        return true;
    };
    if let Some(prev) = map.get(key) {
        if now.duration_since(*prev) < DEBOUNCE {
            return false;
        }
    }
    map.insert(key.to_string(), now);
    true
}

fn paths_match(registered: &str, event_path: &Path, event_canonical: &Path) -> bool {
    let registered = Path::new(registered);
    let registered_canonical =
        std::fs::canonicalize(registered).unwrap_or_else(|_| registered.to_path_buf());
    if registered_canonical == event_canonical {
        return true;
    }
    // Fallback for paths that don't canonicalize identically (e.g. a file
    // briefly missing during an atomic rename): compare filename + parent.
    registered.file_name() == event_path.file_name()
        && registered
            .parent()
            .map(|par| {
                event_path
                    .parent()
                    .and_then(|ep| std::fs::canonicalize(ep).ok())
                    .as_ref()
                    == Some(&std::fs::canonicalize(par).unwrap_or_else(|_| par.to_path_buf()))
            })
            .unwrap_or(false)
}

/// Watch config file(s)/director(y/ies) for changes:
/// - a change to an already-registered pipeline's config file triggers a
///   reload (debounced);
/// - in directory mode (`registry` is `Some`), an unrecognized `*.json` file
///   appearing in a watched directory is registered as a brand-new pipeline,
///   without a process restart.
///
/// `extra_watch_dirs` are watched from the start even if no pipeline is
/// registered yet — needed so a directory-mode engine started with zero
/// pipelines still notices the first config file dropped into it.
pub fn spawn_config_watcher(
    app_state: AppState,
    history: RunHistoryStore,
    registry: Option<PipelineRegistry>,
    extra_watch_dirs: Vec<PathBuf>,
    debounce: ReloadDebounce,
) {
    if app_state.all_config_paths().is_empty() && extra_watch_dirs.is_empty() {
        return;
    }

    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || {
        let (tx, rx) = channel();
        let mut watcher: RecommendedWatcher = match Watcher::new(
            tx,
            notify::Config::default().with_poll_interval(Duration::from_millis(500)),
        ) {
            Ok(w) => w,
            Err(e) => {
                log::error!("File watcher disabled: {}", e);
                return;
            }
        };

        let mut watched_dirs = std::collections::HashSet::new();
        let known_dirs: Vec<PathBuf> = app_state
            .all_config_paths()
            .into_iter()
            .filter_map(|(_, path)| {
                Path::new(&path)
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .map(|p| p.to_path_buf())
            })
            .collect();

        for watch_path in known_dirs.into_iter().chain(extra_watch_dirs) {
            let watch_path = if watch_path.as_os_str().is_empty() {
                PathBuf::from(".")
            } else {
                watch_path
            };
            if watched_dirs.insert(watch_path.clone()) {
                if let Err(e) = watcher.watch(&watch_path, RecursiveMode::NonRecursive) {
                    log::warn!("Cannot watch {}: {}", watch_path.display(), e);
                } else {
                    log::info!("Watching configs in {}", watch_path.display());
                }
            }
        }

        for res in rx {
            let Ok(event) = res else { continue };
            if !matches!(
                event.kind,
                EventKind::Modify(_) | EventKind::Create(_) | EventKind::Any
            ) {
                continue;
            }

            for path in event.paths {
                let is_json = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.eq_ignore_ascii_case("json"))
                    .unwrap_or(false);
                if !is_json {
                    continue;
                }

                let canonical = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());

                let known_id = app_state
                    .all_config_paths()
                    .into_iter()
                    .find(|(_, registered_path)| paths_match(registered_path, &path, &canonical))
                    .map(|(id, _)| id);

                if let Some(id) = known_id {
                    if !should_fire(&debounce, &id) {
                        continue;
                    }
                    log::info!("[{}] Config file changed — reloading", id);
                    if let Err(e) = app_state.send_command(&id, PipelineCommand::Reload) {
                        log::warn!("[{}] Failed to send reload command: {}", id, e);
                    }
                    continue;
                }

                let Some(registry) = &registry else { continue };

                // A delete (e.g. via `DELETE /api/pipelines/:id`) fires a
                // filesystem event too; the file's gone by the time we get
                // here, so there's nothing to register — skip quietly
                // rather than logging a spurious "invalid config" warning.
                if !path.exists() {
                    continue;
                }

                let path_str = path.to_string_lossy().to_string();
                let config = match load_config(&path_str) {
                    Ok(c) => c,
                    Err(e) => {
                        log::warn!(
                            "New config file {} is invalid, skipping: {}",
                            path.display(),
                            e
                        );
                        continue;
                    }
                };
                let new_id = resolve_pipeline_id(&path, &config);

                if !should_fire(&debounce, &new_id) {
                    continue;
                }

                let state_path = registry
                    .state_dir
                    .join(format!("{}.json", new_id))
                    .to_string_lossy()
                    .into_owned();

                log::info!("[{}] New config file detected: {}", new_id, path.display());
                let result = handle.block_on(register_and_spawn_pipeline(
                    &app_state,
                    &history,
                    new_id.clone(),
                    path.clone(),
                    state_path,
                    config,
                ));
                if let Err(e) = result {
                    log::error!("[{}] Failed to register new pipeline: {}", new_id, e);
                }
            }
        }
    });
}
