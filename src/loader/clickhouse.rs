use super::Loader;
use crate::error::EtlError;
use crate::types::{Row, row_to_json};
use async_trait::async_trait;
use reqwest::Client;
use std::time::Duration;

pub struct ClickHouseLoader {
    client: Client,
    host: String,
    database: String,
    table: String,
    username: String,
    password: String,
}

impl ClickHouseLoader {
    pub fn new(
        host: String,
        database: String,
        table: String,
        username: String,
        password: String,
    ) -> Result<Self, EtlError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|e| {
                EtlError::ConnectionError(format!("Failed to build HTTP client: {}", e))
            })?;

        Ok(Self {
            client,
            host,
            database,
            table,
            username,
            password,
        })
    }
}

/// `INSERT INTO {table} FORMAT JSONEachRow` body: one JSON object per
/// line. Factored out as a plain function (no I/O) so it's unit-testable
/// without a live ClickHouse instance.
fn build_insert_body(table: &str, rows: &[Row]) -> String {
    let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", table);
    for row in rows {
        body.push_str(&row_to_json(row).to_string());
        body.push('\n');
    }
    body
}

#[async_trait]
impl Loader for ClickHouseLoader {
    async fn load(&self, rows: Vec<Row>) -> Result<(), EtlError> {
        if rows.is_empty() {
            return Ok(());
        }

        let body = build_insert_body(&self.table, &rows);

        let mut request = self
            .client
            .post(&self.host)
            .query(&[("database", &self.database)])
            .body(body);

        if !self.username.is_empty() {
            request = request.basic_auth(&self.username, Some(&self.password));
        }

        let response = request
            .send()
            .await
            .map_err(|e| EtlError::ConnectionError(format!("ClickHouse HTTP error: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(EtlError::LoadError(format!(
                "ClickHouse insert failed ({}): {}",
                status,
                body.trim()
            )));
        }

        log::info!("Loaded {} rows into {}", rows.len(), self.table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, make_row};

    #[test]
    fn test_build_insert_body_shape() {
        let rows = vec![make_row(vec![
            ("id", Value::Int(1)),
            ("name", Value::Text("Alice".to_string())),
        ])];
        let body = build_insert_body("my_table", &rows);
        let mut lines = body.lines();
        assert_eq!(lines.next().unwrap(), "INSERT INTO my_table FORMAT JSONEachRow");
        let row_line = lines.next().unwrap();
        assert!(row_line.contains("\"id\":1"));
        assert!(row_line.contains("\"name\":\"Alice\""));
        assert!(lines.next().is_none());
    }

    #[test]
    fn test_build_insert_body_multiple_rows() {
        let rows = vec![
            make_row(vec![("id", Value::Int(1))]),
            make_row(vec![("id", Value::Int(2))]),
        ];
        let body = build_insert_body("t", &rows);
        // header + 2 row lines + trailing newline from the loop
        assert_eq!(body.lines().count(), 3);
    }

    #[test]
    fn test_build_insert_body_empty_rows() {
        let body = build_insert_body("t", &[]);
        assert_eq!(body, "INSERT INTO t FORMAT JSONEachRow\n");
    }
}
