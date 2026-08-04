use crate::error::EtlError;
use cron::Schedule;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    /// Optional stable id (defaults to config file stem).
    pub id: Option<String>,
    /// Optional cron schedule (sec min hour day month dow). When set, overrides interval polling.
    pub schedule: Option<String>,
    /// Optional list of pipelines that must have last succeeded before this pipeline ticks.
    /// Each entry is either a bare id string (no staleness limit — any past success
    /// satisfies it, however old) or `{"id": ..., "max_staleness_secs": ...}` (the
    /// dependency's last success must also be no older than that many seconds).
    pub depends_on: Option<Vec<DependsOnEntry>>,
    pub source: SourceConfig,
    pub transforms: Vec<TransformConfig>,
    pub destination: DestinationConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum DependsOnEntry {
    Simple(String),
    Detailed {
        id: String,
        /// Maximum age, in seconds, of the dependency's last successful run for
        /// it to still count as satisfied. Omit for no staleness limit.
        max_staleness_secs: Option<u64>,
    },
}

impl DependsOnEntry {
    pub fn id(&self) -> &str {
        match self {
            DependsOnEntry::Simple(id) => id,
            DependsOnEntry::Detailed { id, .. } => id,
        }
    }

    pub fn max_staleness_secs(&self) -> Option<u64> {
        match self {
            DependsOnEntry::Simple(_) => None,
            DependsOnEntry::Detailed {
                max_staleness_secs, ..
            } => *max_staleness_secs,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Postgres {
        connection_string: String,
        query: String,
        #[serde(default = "default_poll_interval_secs")]
        poll_interval_secs: u64,
    },
    Csv {
        watch_dir: String,
        processed_dir: String,
        #[serde(default = "default_delimiter")]
        delimiter: char,
        #[serde(default = "default_chunk_size")]
        chunk_size: usize,
        #[serde(default = "default_poll_interval_secs")]
        poll_interval_secs: u64,
    },
    #[serde(rename = "clickhouse")]
    ClickHouse {
        host: String,
        database: String,
        query: String,
        #[serde(default)]
        username: String,
        #[serde(default)]
        password: String,
        #[serde(default = "default_chunk_size")]
        chunk_size: usize,
        #[serde(default = "default_poll_interval_secs")]
        poll_interval_secs: u64,
    },
}

fn default_delimiter() -> char {
    ','
}

/// Only used as the tick interval when `PipelineConfig.schedule` (cron) is
/// absent — see `runtime::build_schedule_mode`. Made optional so configs
/// that already set `schedule` don't have to also carry a redundant,
/// unused interval value.
fn default_poll_interval_secs() -> u64 {
    30
}
fn default_chunk_size() -> usize {
    10_000
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformConfig {
    Filter { column: String, value: String },
    Map { rename: HashMap<String, String> },
    Aggregate { group_by: String, sum: String },
}

#[derive(Debug, Deserialize)]
pub struct DestinationConfig {
    #[serde(rename = "type")]
    pub dest_type: String,
    pub connection_string: String,
    pub table: String,
    pub unique_key: Option<String>,
}

/// A config loaded from disk with a resolved pipeline id.
#[derive(Debug)]
pub struct LoadedPipeline {
    pub path: PathBuf,
    pub id: String,
    pub config: PipelineConfig,
}

pub fn load_config(path: &str) -> Result<PipelineConfig, EtlError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| EtlError::ConfigError(format!("Cannot read file {}: {}", path, e)))?;

    let config: PipelineConfig = serde_json::from_str(&content)?;
    validate_schedule(&config)?;
    Ok(config)
}

/// Load all `*.json` pipeline configs from a directory (fail-fast on any bad file).
pub fn load_configs_from_dir(dir: &str) -> Result<Vec<LoadedPipeline>, EtlError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| EtlError::ConfigError(format!("Cannot read config directory {}: {}", dir, e)))?;

    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("json"))
                .unwrap_or(false)
        })
        .collect();

    paths.sort();

    // An empty directory is valid — the engine can start with zero pipelines
    // and have them registered later via the API or by dropping a new
    // `*.json` file into this same directory (see `registry.rs`).
    let mut loaded = Vec::with_capacity(paths.len());
    for path in paths {
        let path_str = path.to_string_lossy();
        let config = load_config(&path_str)?;
        let id = resolve_pipeline_id(&path, &config);
        loaded.push(LoadedPipeline { path, id, config });
    }

    Ok(loaded)
}

pub fn resolve_pipeline_id(path: &Path, config: &PipelineConfig) -> String {
    config.id.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_string()
    })
}

/// Validate that every `depends_on` id refers to a pipeline actually present
/// in `loaded`, that no pipeline depends on itself, and that the dependency
/// graph has no cycles (which would otherwise deadlock every pipeline on the
/// cycle, since each waits forever for the other's success). Called once at
/// startup after all configs are loaded (both single-file and directory
/// modes converge here).
pub fn validate_depends_on(loaded: &[LoadedPipeline]) -> Result<(), EtlError> {
    let entries: Vec<(String, Option<Vec<DependsOnEntry>>)> = loaded
        .iter()
        .map(|p| (p.id.clone(), p.config.depends_on.clone()))
        .collect();
    validate_depends_on_entries(&entries)
}

/// Validate depends_on integrity (existence, no self-dependency, no cycles)
/// as it would be immediately after replacing `id`'s `depends_on` with
/// `new_depends_on`, checked against every other pipeline's *currently
/// registered* config file (re-read fresh from disk via `other_config_paths`,
/// so this reflects the live set rather than whatever was loaded at startup).
///
/// Used to gate hot-reload — whether triggered through the config-editor API
/// or by directly editing a config file on disk — so a bad edit can't
/// silently create an unreachable id or a dependency cycle at runtime. This
/// closes the gap left by `validate_depends_on` only running at startup.
///
/// Best-effort: a pipeline whose own on-disk config is currently
/// unparseable is skipped rather than blocking this reload on an unrelated
/// broken file.
pub fn validate_depends_on_for_reload(
    id: &str,
    new_depends_on: &Option<Vec<DependsOnEntry>>,
    other_config_paths: &[(String, String)],
) -> Result<(), EtlError> {
    let mut entries: Vec<(String, Option<Vec<DependsOnEntry>>)> = Vec::new();
    for (other_id, other_path) in other_config_paths {
        if other_id == id {
            continue;
        }
        if let Ok(other_config) = load_config(other_path) {
            entries.push((other_id.clone(), other_config.depends_on));
        }
    }
    entries.push((id.to_string(), new_depends_on.clone()));
    validate_depends_on_entries(&entries)
}

fn validate_depends_on_entries(
    entries: &[(String, Option<Vec<DependsOnEntry>>)],
) -> Result<(), EtlError> {
    let known_ids: HashSet<&str> = entries.iter().map(|(id, _)| id.as_str()).collect();
    let mut deps_by_id: HashMap<String, Vec<String>> = HashMap::new();

    for (id, depends_on) in entries {
        let deps = depends_on.clone().unwrap_or_default();
        let mut dep_ids = Vec::with_capacity(deps.len());
        for dep in &deps {
            let dep_id = dep.id();
            if dep_id == id {
                return Err(EtlError::ConfigError(format!(
                    "Pipeline '{}' cannot depend on itself",
                    id
                )));
            }
            if !known_ids.contains(dep_id) {
                return Err(EtlError::ConfigError(format!(
                    "Pipeline '{}' depends_on unknown pipeline id '{}'",
                    id, dep_id
                )));
            }
            dep_ids.push(dep_id.to_string());
        }
        deps_by_id.insert(id.clone(), dep_ids);
    }

    detect_dependency_cycle(&deps_by_id)
}

#[derive(Clone, Copy, PartialEq)]
enum VisitState {
    Visiting,
    Done,
}

fn detect_dependency_cycle(deps_by_id: &HashMap<String, Vec<String>>) -> Result<(), EtlError> {
    let mut state: HashMap<&str, VisitState> = HashMap::new();
    let mut path: Vec<&str> = Vec::new();

    for id in deps_by_id.keys() {
        if !state.contains_key(id.as_str()) {
            if let Some(cycle) = visit_for_cycle(id, deps_by_id, &mut state, &mut path) {
                return Err(EtlError::ConfigError(format!(
                    "Dependency cycle detected: {}",
                    cycle.join(" -> ")
                )));
            }
        }
    }
    Ok(())
}

fn visit_for_cycle<'a>(
    id: &'a str,
    deps_by_id: &'a HashMap<String, Vec<String>>,
    state: &mut HashMap<&'a str, VisitState>,
    path: &mut Vec<&'a str>,
) -> Option<Vec<String>> {
    state.insert(id, VisitState::Visiting);
    path.push(id);

    if let Some(deps) = deps_by_id.get(id) {
        for dep in deps {
            match state.get(dep.as_str()) {
                Some(VisitState::Visiting) => {
                    let start_idx = path.iter().position(|&p| p == dep.as_str()).unwrap();
                    let mut cycle: Vec<String> =
                        path[start_idx..].iter().map(|s| s.to_string()).collect();
                    cycle.push(dep.clone());
                    return Some(cycle);
                }
                Some(VisitState::Done) => {}
                None => {
                    if let Some(cycle) = visit_for_cycle(dep, deps_by_id, state, path) {
                        return Some(cycle);
                    }
                }
            }
        }
    }

    path.pop();
    state.insert(id, VisitState::Done);
    None
}

pub fn poll_interval_secs(source: &SourceConfig) -> u64 {
    match source {
        SourceConfig::Postgres {
            poll_interval_secs, ..
        } => *poll_interval_secs,
        SourceConfig::Csv {
            poll_interval_secs, ..
        } => *poll_interval_secs,
        SourceConfig::ClickHouse {
            poll_interval_secs, ..
        } => *poll_interval_secs,
    }
}

pub fn parse_cron_schedule(expr: &str) -> Result<Schedule, EtlError> {
    Schedule::from_str(expr).map_err(|e| {
        EtlError::ConfigError(format!(
            "Invalid cron schedule '{}': {} (expected: sec min hour day month dow)",
            expr, e
        ))
    })
}

fn validate_schedule(config: &PipelineConfig) -> Result<(), EtlError> {
    if let Some(ref expr) = config.schedule {
        parse_cron_schedule(expr)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_load_postgres_config() {
        let config = load_config("config/pipeline.json").expect("Failed to load config");

        match &config.source {
            SourceConfig::Postgres {
                poll_interval_secs, ..
            } => {
                assert_eq!(*poll_interval_secs, 5);
            }
            _ => panic!("Expected Postgres source"),
        }

        assert_eq!(config.transforms.len(), 3);
        assert_eq!(config.destination.table, "orders_summary");
        assert_eq!(config.depends_on, None);
    }

    #[test]
    fn test_load_csv_config() {
        let config = load_config("config/pipeline_csv.json").expect("Failed to load CSV config");

        match &config.source {
            SourceConfig::Csv {
                watch_dir,
                chunk_size,
                ..
            } => {
                assert_eq!(watch_dir, "data/watched");
                assert_eq!(*chunk_size, 10000);
            }
            _ => panic!("Expected CSV source"),
        }

        assert_eq!(config.destination.unique_key, None);
    }

    #[test]
    fn test_load_clickhouse_config() {
        let config = load_config("config/pipeline_clickhouse.json")
            .expect("Failed to load ClickHouse config");

        match &config.source {
            SourceConfig::ClickHouse {
                host,
                database,
                poll_interval_secs,
                ..
            } => {
                assert_eq!(host, "http://localhost:8123");
                assert_eq!(database, "default");
                assert_eq!(*poll_interval_secs, 60);
            }
            _ => panic!("Expected ClickHouse source"),
        }
    }

    #[test]
    fn test_invalid_config() {
        let result = load_config("non_existent_file.json");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cron_rejected() {
        let dir = tempfile_dir("bad_cron");
        write_fixture(
            &dir,
            "bad.json",
            r#"{
              "schedule": "not a cron",
              "source": {
                "type": "csv",
                "watch_dir": "w",
                "processed_dir": "p",
                "poll_interval_secs": 5
              },
              "transforms": [],
              "destination": {
                "type": "postgres",
                "connection_string": "postgres://x",
                "table": "t"
              }
            }"#,
        );
        let result = load_config(&dir.join("bad.json").to_string_lossy());
        assert!(result.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_valid_cron_accepted() {
        let dir = tempfile_dir("good_cron");
        write_fixture(
            &dir,
            "good.json",
            r#"{
              "id": "csv_import",
              "schedule": "0 */10 * * * *",
              "source": {
                "type": "csv",
                "watch_dir": "w",
                "processed_dir": "p",
                "poll_interval_secs": 5
              },
              "transforms": [],
              "destination": {
                "type": "postgres",
                "connection_string": "postgres://x",
                "table": "t"
              }
            }"#,
        );
        let config = load_config(&dir.join("good.json").to_string_lossy()).unwrap();
        assert_eq!(config.id.as_deref(), Some("csv_import"));
        assert!(config.schedule.is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_load_configs_from_dir_and_id_from_filename() {
        let dir = tempfile_dir("multi");
        write_fixture(
            &dir,
            "alpha.json",
            r#"{
              "source": {
                "type": "csv",
                "watch_dir": "w",
                "processed_dir": "p",
                "poll_interval_secs": 5
              },
              "transforms": [],
              "destination": {
                "type": "postgres",
                "connection_string": "postgres://x",
                "table": "t"
              }
            }"#,
        );
        write_fixture(
            &dir,
            "beta.json",
            r#"{
              "id": "custom_beta",
              "source": {
                "type": "csv",
                "watch_dir": "w2",
                "processed_dir": "p2",
                "poll_interval_secs": 10
              },
              "transforms": [],
              "destination": {
                "type": "postgres",
                "connection_string": "postgres://x",
                "table": "t2"
              }
            }"#,
        );

        let loaded = load_configs_from_dir(dir.to_str().unwrap()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "alpha");
        assert_eq!(loaded[1].id, "custom_beta");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_depends_on_parses_optional_field() {
        let dir = tempfile_dir("depends_on");
        write_fixture(
            &dir,
            "downstream.json",
            r#"{
              "id": "downstream",
              "depends_on": ["upstream"],
              "source": {
                "type": "csv",
                "watch_dir": "w",
                "processed_dir": "p",
                "poll_interval_secs": 5
              },
              "transforms": [],
              "destination": {
                "type": "postgres",
                "connection_string": "postgres://x",
                "table": "t"
              }
            }"#,
        );
        let config = load_config(&dir.join("downstream.json").to_string_lossy()).unwrap();
        assert_eq!(
            config.depends_on,
            Some(vec![DependsOnEntry::Simple("upstream".to_string())])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_depends_on_parses_detailed_entry_with_staleness() {
        let dir = tempfile_dir("depends_on_detailed");
        write_fixture(
            &dir,
            "downstream.json",
            r#"{
              "id": "downstream",
              "depends_on": [{"id": "upstream", "max_staleness_secs": 3600}],
              "source": {
                "type": "csv",
                "watch_dir": "w",
                "processed_dir": "p",
                "poll_interval_secs": 5
              },
              "transforms": [],
              "destination": {
                "type": "postgres",
                "connection_string": "postgres://x",
                "table": "t"
              }
            }"#,
        );
        let config = load_config(&dir.join("downstream.json").to_string_lossy()).unwrap();
        assert_eq!(
            config.depends_on,
            Some(vec![DependsOnEntry::Detailed {
                id: "upstream".to_string(),
                max_staleness_secs: Some(3600),
            }])
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_known_id_accepted() {
        let dir = tempfile_dir("deps_valid");
        write_fixture(
            &dir,
            "upstream.json",
            r#"{
              "id": "upstream",
              "source": { "type": "csv", "watch_dir": "w", "processed_dir": "p", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "t" }
            }"#,
        );
        write_fixture(
            &dir,
            "downstream.json",
            r#"{
              "id": "downstream",
              "depends_on": ["upstream"],
              "source": { "type": "csv", "watch_dir": "w2", "processed_dir": "p2", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "t2" }
            }"#,
        );
        let loaded = load_configs_from_dir(dir.to_str().unwrap()).unwrap();
        assert!(validate_depends_on(&loaded).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_unknown_id_rejected() {
        let dir = tempfile_dir("deps_unknown");
        write_fixture(
            &dir,
            "downstream.json",
            r#"{
              "id": "downstream",
              "depends_on": ["does_not_exist"],
              "source": { "type": "csv", "watch_dir": "w", "processed_dir": "p", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "t" }
            }"#,
        );
        let loaded = load_configs_from_dir(dir.to_str().unwrap()).unwrap();
        let result = validate_depends_on(&loaded);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown pipeline id"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_self_dependency_rejected() {
        let dir = tempfile_dir("deps_self");
        write_fixture(
            &dir,
            "loopy.json",
            r#"{
              "id": "loopy",
              "depends_on": ["loopy"],
              "source": { "type": "csv", "watch_dir": "w", "processed_dir": "p", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "t" }
            }"#,
        );
        let loaded = load_configs_from_dir(dir.to_str().unwrap()).unwrap();
        let result = validate_depends_on(&loaded);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot depend on itself"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_direct_cycle_rejected() {
        let dir = tempfile_dir("deps_cycle2");
        write_fixture(
            &dir,
            "a.json",
            r#"{
              "id": "a",
              "depends_on": ["b"],
              "source": { "type": "csv", "watch_dir": "w", "processed_dir": "p", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "t" }
            }"#,
        );
        write_fixture(
            &dir,
            "b.json",
            r#"{
              "id": "b",
              "depends_on": ["a"],
              "source": { "type": "csv", "watch_dir": "w2", "processed_dir": "p2", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "t2" }
            }"#,
        );
        let loaded = load_configs_from_dir(dir.to_str().unwrap()).unwrap();
        let result = validate_depends_on(&loaded);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Dependency cycle detected"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_transitive_cycle_rejected() {
        let dir = tempfile_dir("deps_cycle3");
        write_fixture(
            &dir,
            "a.json",
            r#"{
              "id": "a",
              "depends_on": ["b"],
              "source": { "type": "csv", "watch_dir": "wa", "processed_dir": "pa", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "ta" }
            }"#,
        );
        write_fixture(
            &dir,
            "b.json",
            r#"{
              "id": "b",
              "depends_on": ["c"],
              "source": { "type": "csv", "watch_dir": "wb", "processed_dir": "pb", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "tb" }
            }"#,
        );
        write_fixture(
            &dir,
            "c.json",
            r#"{
              "id": "c",
              "depends_on": ["a"],
              "source": { "type": "csv", "watch_dir": "wc", "processed_dir": "pc", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "tc" }
            }"#,
        );
        let loaded = load_configs_from_dir(dir.to_str().unwrap()).unwrap();
        let result = validate_depends_on(&loaded);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Dependency cycle detected"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_diamond_shape_accepted() {
        // a depends on both b and c; b and c both depend on d. No cycle.
        let dir = tempfile_dir("deps_diamond");
        write_fixture(
            &dir,
            "a.json",
            r#"{
              "id": "a",
              "depends_on": ["b", "c"],
              "source": { "type": "csv", "watch_dir": "wa", "processed_dir": "pa", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "ta" }
            }"#,
        );
        write_fixture(
            &dir,
            "b.json",
            r#"{
              "id": "b",
              "depends_on": ["d"],
              "source": { "type": "csv", "watch_dir": "wb", "processed_dir": "pb", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "tb" }
            }"#,
        );
        write_fixture(
            &dir,
            "c.json",
            r#"{
              "id": "c",
              "depends_on": ["d"],
              "source": { "type": "csv", "watch_dir": "wc", "processed_dir": "pc", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "tc" }
            }"#,
        );
        write_fixture(
            &dir,
            "d.json",
            r#"{
              "id": "d",
              "source": { "type": "csv", "watch_dir": "wd", "processed_dir": "pd", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "td" }
            }"#,
        );
        let loaded = load_configs_from_dir(dir.to_str().unwrap()).unwrap();
        assert!(validate_depends_on(&loaded).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_for_reload_unknown_id_rejected() {
        let dir = tempfile_dir("reload_unknown");
        write_fixture(
            &dir,
            "upstream.json",
            r#"{
              "id": "upstream",
              "source": { "type": "csv", "watch_dir": "w", "processed_dir": "p", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "t" }
            }"#,
        );
        let other_paths = vec![(
            "upstream".to_string(),
            dir.join("upstream.json").to_string_lossy().to_string(),
        )];
        let new_depends_on = Some(vec![DependsOnEntry::Simple("does_not_exist".to_string())]);
        let result = validate_depends_on_for_reload("downstream", &new_depends_on, &other_paths);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown pipeline id"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_for_reload_known_id_accepted() {
        let dir = tempfile_dir("reload_known");
        write_fixture(
            &dir,
            "upstream.json",
            r#"{
              "id": "upstream",
              "source": { "type": "csv", "watch_dir": "w", "processed_dir": "p", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "t" }
            }"#,
        );
        let other_paths = vec![(
            "upstream".to_string(),
            dir.join("upstream.json").to_string_lossy().to_string(),
        )];
        let new_depends_on = Some(vec![DependsOnEntry::Simple("upstream".to_string())]);
        let result = validate_depends_on_for_reload("downstream", &new_depends_on, &other_paths);
        assert!(result.is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_for_reload_new_cycle_rejected() {
        // "a" is already registered and depends on "b". Reloading "b" to
        // depend on "a" would close a 2-cycle that didn't exist at startup.
        let dir = tempfile_dir("reload_cycle");
        write_fixture(
            &dir,
            "a.json",
            r#"{
              "id": "a",
              "depends_on": ["b"],
              "source": { "type": "csv", "watch_dir": "wa", "processed_dir": "pa", "poll_interval_secs": 5 },
              "transforms": [],
              "destination": { "type": "postgres", "connection_string": "postgres://x", "table": "ta" }
            }"#,
        );
        let other_paths = vec![(
            "a".to_string(),
            dir.join("a.json").to_string_lossy().to_string(),
        )];
        let new_depends_on = Some(vec![DependsOnEntry::Simple("a".to_string())]);
        let result = validate_depends_on_for_reload("b", &new_depends_on, &other_paths);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Dependency cycle detected"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_validate_depends_on_for_reload_no_deps_accepted() {
        let result = validate_depends_on_for_reload("solo", &None, &[]);
        assert!(result.is_ok());
    }

    fn tempfile_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "etl_config_test_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_fixture(dir: &Path, name: &str, body: &str) {
        let mut f = std::fs::File::create(dir.join(name)).unwrap();
        f.write_all(body.as_bytes()).unwrap();
    }
}
