use crate::error::EtlError;
use crate::types::Row;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub mod clickhouse;
pub mod csv;
pub mod postgres;
pub mod s3;

#[async_trait]
pub trait Extractor: Send + Sync {
    /// `until` is `None` on every normal scheduled tick (unbounded upper
    /// end). Only the replay path (`src/replay.rs`) ever passes `Some(_)`,
    /// for cursor-based sources (Postgres/ClickHouse). CSV/S3 ignore both
    /// params — their dedup is by filename/object key, not time.
    async fn extract(
        &self,
        last_run: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Result<Vec<Row>, EtlError>;
}
