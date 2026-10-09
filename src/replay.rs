use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::{SourceConfig, load_config};
use crate::error::EtlError;
use crate::extractor::csv::read_processed_files;
use crate::extractor::s3::S3Extractor;
use crate::pipeline::Pipeline;
use crate::state::PersistentState;

/// `keys` is for dedup-based sources (CSV/S3) — specific already-ingested
/// files/object keys to reprocess. `from`/`until` are for cursor-based
/// sources (Postgres/ClickHouse) — a time range. Exactly one style must be
/// used, matching the pipeline's actual source type; see `run_replay`.
#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    pub from: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub keys: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct ReplaySummary {
    pub rows_loaded: u64,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

/// Standalone extract→transform→load, built fresh from the pipeline's
/// config file and torn down afterward. Never touches the live
/// `PipelineState`/`PersistentState`/scheduler — see the plan's rationale
/// for why that would otherwise risk corrupting the live cursor.
pub async fn run_replay(config_path: &str, req: ReplayRequest) -> Result<ReplaySummary, EtlError> {
    let config = load_config(config_path)?;
    let started_at = Utc::now();

    let rows_loaded = match (&config.source, &req.keys) {
        (SourceConfig::Csv { processed_dir, delimiter, .. }, Some(keys)) => {
            let rows = read_processed_files(processed_dir, keys, *delimiter)?;
            load_rows(&config, rows).await?
        }
        (SourceConfig::S3 { bucket, prefix, format, delimiter, region, endpoint_url, .. }, Some(keys)) => {
            let store = crate::s3_store::build_store(bucket, region.as_deref(), endpoint_url.as_deref())?;
            // S3Extractor's constructor requires a PersistentState/state_path
            // pair for its live `extract()` path, but `read_keys` below never
            // touches either — a throwaway pair is safe here.
            let throwaway_state = Arc::new(Mutex::new(PersistentState::new()));
            let extractor = S3Extractor::new(
                store,
                prefix.clone(),
                format.clone(),
                *delimiter,
                throwaway_state,
                String::new(),
            );
            let rows = extractor.read_keys(keys).await?;
            load_rows(&config, rows).await?
        }
        (SourceConfig::Postgres { .. } | SourceConfig::ClickHouse { .. }, None) => {
            // Reuses the exact same extract→transform→load sequencing as a
            // normal tick (`Pipeline::run`), just bounded by an explicit
            // `until` and never touching `PipelineState`.
            let throwaway_state = Arc::new(Mutex::new(PersistentState::new()));
            let (_interval, extractor) =
                crate::runtime::build_extractor(&config, &throwaway_state, "").await?;
            let transformers = crate::runtime::build_transformers(&config);
            let loader = crate::runtime::build_loader(&config).await?;
            let pipeline = Pipeline::new(extractor, transformers, loader);
            let from = req.from.unwrap_or(DateTime::<Utc>::MIN_UTC);
            pipeline.run_once_bounded(from, req.until).await?
        }
        _ => {
            return Err(EtlError::ConfigError(
                "replay request shape doesn't match this pipeline's source type \
                 (postgres/clickhouse take from/until, csv/s3 take keys)"
                    .to_string(),
            ));
        }
    };

    Ok(ReplaySummary {
        rows_loaded,
        started_at,
        finished_at: Utc::now(),
    })
}

async fn load_rows(config: &crate::config::PipelineConfig, rows: Vec<crate::types::Row>) -> Result<u64, EtlError> {
    let mut current_rows = rows;
    for t in crate::runtime::build_transformers(config) {
        current_rows = t.transform(current_rows)?;
    }
    let rows_loaded = current_rows.len() as u64;

    let loader = crate::runtime::build_loader(config).await?;
    loader.load(current_rows).await?;
    Ok(rows_loaded)
}
