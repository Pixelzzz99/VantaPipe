use async_trait::async_trait;
use crate::types::Row;
use crate::error::EtlError;

pub mod clickhouse;
pub mod postgres;
pub mod s3;

#[async_trait]
pub trait Loader: Send + Sync {
    async fn load(&self, rows: Vec<Row>) -> Result<(), EtlError>;
}
