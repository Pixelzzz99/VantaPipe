use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row as SqlxRow, Column};
use crate::types::{Row, Value};
use crate::error::EtlError;
use super::Extractor;


pub struct PostgresExtractor {
    pool: PgPool,
    query: String,
}

impl PostgresExtractor {
    pub fn new(pool: PgPool, query: String) -> Self {
        Self { pool, query }
    }

    pub async fn connect(connection_string: &str, query: String) -> Result<Self, EtlError> {
        let pool = PgPool::connect(connection_string).await?;
        Ok(Self::new(pool, query))
    }
}

#[async_trait]
impl Extractor for PostgresExtractor {
    async fn extract(
        &self,
        last_run: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Result<Vec<Row>, EtlError> {
        let sqlx_rows = if wants_until(&self.query) {
            sqlx::query(&self.query)
                .bind(last_run)
                .bind(until.unwrap_or_else(Utc::now))
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query(&self.query)
                .bind(last_run)
                .fetch_all(&self.pool)
                .await?
        };

        let rows = sqlx_rows
            .iter()
            .map(|sqlx_row| convert_row(sqlx_row))
            .collect();


        Ok(rows)
    }
}

/// Whether this query template was authored with a second (`$2`) bound
/// parameter — the convention a pipeline author opts into so the same
/// query serves both normal ticks (`$2` defaults to `Utc::now()`) and a
/// replay's explicit `until` bound.
fn wants_until(query: &str) -> bool {
    query.contains("$2")
}

fn convert_row(sqlx_row: &sqlx::postgres::PgRow) -> Row{
    let mut row = std::collections::HashMap::new();

    for (i, column) in sqlx_row.columns().iter().enumerate() {
        let col_name = column.name().to_string();

        let value = if let Ok(v) = sqlx_row.try_get::<i64, _>(i) {
            Value::Int(v)
        } else if let Ok(v) = sqlx_row.try_get::<f64, _>(i) {
            Value::Float(v)
        } else if let Ok(v) = sqlx_row.try_get::<bool, _>(i) {
            Value::Bool(v)
        } else if let Ok(v) = sqlx_row.try_get::<String, _>(i) {
            Value::Text(v)
        } else {
            Value::Null
        };

        row.insert(col_name, value);
    }
    row
}

#[cfg(test)]
mod tests {
    use super::wants_until;

    #[test]
    fn test_wants_until_detects_second_placeholder() {
        assert!(wants_until("SELECT * FROM t WHERE updated_at > $1 AND updated_at <= $2"));
        assert!(!wants_until("SELECT * FROM t WHERE updated_at > $1"));
    }
}
