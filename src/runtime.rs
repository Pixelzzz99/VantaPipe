use std::sync::{Arc, Mutex};

use crate::config::{
    DestinationConfig, PipelineConfig, SourceConfig, TransformConfig, poll_interval_secs,
};
use crate::error::EtlError;
use crate::extractor::Extractor;
use crate::extractor::clickhouse::ClickHouseExtractor;
use crate::extractor::csv::CsvExtractor;
use crate::extractor::postgres::PostgresExtractor;
use crate::extractor::s3::S3Extractor;
use crate::loader::Loader;
use crate::loader::clickhouse::ClickHouseLoader;
use crate::loader::postgres::PostgresLoader;
use crate::loader::s3::S3Loader;
use crate::pipeline::{Pipeline, PipelineState};
use crate::scheduler::ScheduleMode;
use crate::state::PersistentState;
use crate::transformer::{
    Transformer, aggregator::AggregateTransformer, custom_js::CustomJsTransformer,
    filter::FilterTransformer, mapper::MapTransformer,
};
use std::time::Duration;

/// Fully constructed pipeline ready for the scheduler.
pub struct BuiltPipeline {
    pub id: String,
    pub config_path: String,
    pub state_path: String,
    pub schedule: ScheduleMode,
    pub schedule_label: String,
    pub pipeline: Pipeline,
    pub pipeline_state: Arc<Mutex<PipelineState>>,
    pub persistent_state: Arc<Mutex<PersistentState>>,
}

pub async fn build_pipeline(
    id: String,
    config_path: String,
    state_path: String,
    config: &PipelineConfig,
) -> Result<BuiltPipeline, EtlError> {
    let persistent_state = Arc::new(Mutex::new(PersistentState::load(&state_path)));
    let pipeline_state = {
        let ps = persistent_state.lock().unwrap();
        log::info!(
            "[{}] Loaded state: {} files processed, last_run: {}",
            id,
            ps.processed_files.len(),
            ps.last_run
        );
        Arc::new(Mutex::new(PipelineState {
            last_run: ps.last_run,
            rows_processed: ps.total_rows_processed,
        }))
    };

    let (interval_secs, extractor) =
        build_extractor(config, &persistent_state, &state_path).await?;
    let transformers = build_transformers(config);
    let loader = build_loader(config).await?;
    let pipeline = Pipeline::new(extractor, transformers, loader);

    let (schedule, schedule_label) = resolve_schedule(config, interval_secs)?;

    Ok(BuiltPipeline {
        id,
        config_path,
        state_path,
        schedule,
        schedule_label,
        pipeline,
        pipeline_state,
        persistent_state,
    })
}

/// Rebuild extractor/loader/schedule while keeping existing persistent/pipeline state.
pub async fn rebuild_pipeline_parts(
    id: &str,
    state_path: &str,
    config: &PipelineConfig,
    persistent_state: Arc<Mutex<PersistentState>>,
) -> Result<(Pipeline, ScheduleMode, String), EtlError> {
    log::info!("[{}] Reloading pipeline from config", id);
    let (interval_secs, extractor) =
        build_extractor(config, &persistent_state, state_path).await?;
    let transformers = build_transformers(config);
    let loader = build_loader(config).await?;
    let pipeline = Pipeline::new(extractor, transformers, loader);
    let (schedule, schedule_label) = resolve_schedule(config, interval_secs)?;
    Ok((pipeline, schedule, schedule_label))
}

fn resolve_schedule(
    config: &PipelineConfig,
    interval_secs: u64,
) -> Result<(ScheduleMode, String), EtlError> {
    if let Some(ref expr) = config.schedule {
        let schedule = crate::config::parse_cron_schedule(expr)?;
        Ok((
            ScheduleMode::Cron(schedule),
            format!("cron: {}", expr),
        ))
    } else {
        Ok((
            ScheduleMode::Interval(interval_secs),
            format!("every {}s", interval_secs),
        ))
    }
}

async fn build_extractor(
    config: &PipelineConfig,
    persistent_state: &Arc<Mutex<PersistentState>>,
    state_path: &str,
) -> Result<(u64, Box<dyn Extractor>), EtlError> {
    let interval = poll_interval_secs(&config.source);

    match &config.source {
        SourceConfig::Postgres {
            connection_string,
            query,
            ..
        } => {
            log::info!("Source: PostgreSQL");
            let e = PostgresExtractor::connect(connection_string, query.clone()).await?;
            Ok((interval, Box::new(e)))
        }
        SourceConfig::Csv {
            watch_dir,
            processed_dir,
            delimiter,
            ..
        } => {
            log::info!("Source: CSV files from {}", watch_dir);
            let e = CsvExtractor::new(
                watch_dir,
                processed_dir,
                *delimiter,
                Arc::clone(persistent_state),
                state_path.to_string(),
            )?;
            Ok((interval, Box::new(e)))
        }
        SourceConfig::ClickHouse {
            host,
            database,
            query,
            username,
            password,
            ..
        } => {
            log::info!("Source: ClickHouse at {} database {}", host, database);
            let e = ClickHouseExtractor::new(
                host.clone(),
                database.clone(),
                query.clone(),
                username.clone(),
                password.clone(),
            )?;
            Ok((interval, Box::new(e)))
        }
        SourceConfig::S3 {
            bucket,
            prefix,
            format,
            delimiter,
            region,
            endpoint_url,
            ..
        } => {
            log::info!("Source: S3 bucket {} prefix {}", bucket, prefix);
            let store = crate::s3_store::build_store(
                bucket,
                region.as_deref(),
                endpoint_url.as_deref(),
            )?;
            let e = S3Extractor::new(
                store,
                prefix.clone(),
                format.clone(),
                *delimiter,
                Arc::clone(persistent_state),
                state_path.to_string(),
            );
            Ok((interval, Box::new(e)))
        }
    }
}

fn build_transformers(config: &PipelineConfig) -> Vec<Box<dyn Transformer>> {
    config
        .transforms
        .iter()
        .map(|tc| -> Box<dyn Transformer> {
            match tc {
                TransformConfig::Filter { column, value } => {
                    Box::new(FilterTransformer::new(column.clone(), value.clone()))
                }
                TransformConfig::Map { rename } => Box::new(MapTransformer::new(rename.clone())),
                TransformConfig::Aggregate { group_by, sum } => {
                    Box::new(AggregateTransformer::new(group_by.clone(), sum.clone()))
                }
                TransformConfig::Custom {
                    script,
                    function,
                    timeout_ms,
                } => Box::new(CustomJsTransformer::new(
                    script.clone(),
                    function.clone(),
                    Duration::from_millis(*timeout_ms),
                )),
            }
        })
        .collect()
}

fn chunk_size_from(config: &PipelineConfig) -> usize {
    match &config.source {
        SourceConfig::Csv { chunk_size, .. } => *chunk_size,
        SourceConfig::ClickHouse { chunk_size, .. } => *chunk_size,
        _ => 10_000,
    }
}

async fn build_loader(config: &PipelineConfig) -> Result<Box<dyn Loader>, EtlError> {
    match &config.destination {
        DestinationConfig::Postgres {
            connection_string,
            table,
            unique_key,
        } => {
            log::info!("Destination: PostgreSQL");
            let l = PostgresLoader::connect(
                connection_string,
                table.clone(),
                chunk_size_from(config),
                unique_key.clone(),
            )
            .await?;
            Ok(Box::new(l))
        }
        DestinationConfig::ClickHouse {
            host,
            database,
            table,
            username,
            password,
        } => {
            log::info!("Destination: ClickHouse at {} database {}", host, database);
            let l = ClickHouseLoader::new(
                host.clone(),
                database.clone(),
                table.clone(),
                username.clone(),
                password.clone(),
            )?;
            Ok(Box::new(l))
        }
        DestinationConfig::S3 {
            bucket,
            prefix,
            format,
            region,
            endpoint_url,
        } => {
            log::info!("Destination: S3 bucket {} prefix {}", bucket, prefix);
            let store = crate::s3_store::build_store(
                bucket,
                region.as_deref(),
                endpoint_url.as_deref(),
            )?;
            let l = S3Loader::new(store, prefix.clone(), format.clone());
            Ok(Box::new(l))
        }
    }
}
