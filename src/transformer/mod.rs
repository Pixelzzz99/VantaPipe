use crate::types::Row;
use crate::error::EtlError;

pub mod filter;
pub mod mapper;
pub mod aggregator;
pub mod custom_js;

pub trait Transformer: Send + Sync{
    fn transform(&self, rows: Vec<Row>) -> Result<Vec<Row>, EtlError>;
}
