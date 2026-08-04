use super::Extractor;
use crate::error::EtlError;
use crate::types::{Row, Value};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::Value as JsonValue;

pub struct ClickHouseExtractor {
    client: Client,
    host: String,
    database: String,
    query_template: String,
    username: String,
    password: String,
}

impl ClickHouseExtractor {
    pub fn new(
        host: String,
        database: String,
        query_template: String,
        username: String,
        password: String,
    ) -> Result<Self, EtlError> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .map_err(|e| {
                EtlError::ConnectionError(format!("Failed to build HTTP client: {}", e))
            })?;

        Ok(Self {
            client,
            host,
            database,
            query_template,
            username,
            password,
        })
    }

    fn build_query(&self, last_run: DateTime<Utc>) -> String {
        let formatted = last_run.format("%Y-%m-%d %H:%M:%S").to_string();
        self.query_template.replace("{last_run}", &formatted)
    }

    async fn execute_query(&self, sql: &str) -> Result<Vec<Row>, EtlError> {
        let sql_with_format = if sql.contains("FORMAT") {
            sql.to_string()
        } else {
            format!("{} FORMAT JSONEachRow", sql)
        };

        let mut request = self
            .client
            .post(&self.host)
            .query(&[("database", &self.database)])
            .body(sql_with_format);

        if !self.username.is_empty() {
            request = request.basic_auth(&self.username, Some(&self.password));
        }

        let response = request
            .send()
            .await
            .map_err(|e| EtlError::ConnectionError(format!("Clickhouse HTTP Error: {}", e)))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| EtlError::QueryError(format!("Failed to read response: {}", e)))?;

        if !status.is_success() {
            return Err(EtlError::QueryError(format!(
                "Clickhouse error ({}): {}",
                status,
                body.trim()
            )));
        }

        let row: Vec<Row> = body
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| parse_json_row(line))
            .collect::<Result<Vec<Row>, EtlError>>()?;

        Ok(row)
    }
}

fn parse_json_row(line: &str) -> Result<Row, EtlError> {
    let json: serde_json::Map<String, JsonValue> = serde_json::from_str(line)
        .map_err(|e| EtlError::QueryError(format!("JSON parse error '{}': {}", line, e)))?;

    let row: Row = json
        .into_iter()
        .map(|(key, json_val)| (key, convert_json_value(json_val)))
        .collect();

    Ok(row)
}

fn convert_json_value(json_val: JsonValue) -> Value {
    match json_val {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(b),
        JsonValue::String(s) => Value::Text(s),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        other => Value::Text(other.to_string()),
    }
}

#[async_trait]
impl Extractor for ClickHouseExtractor {
    async fn extract(&self, last_run: DateTime<Utc>) -> Result<Vec<Row>, EtlError> {
        let sql = self.build_query(last_run);
        log::info!("ClickHouse query: {}", sql);

        let rows = self.execute_query(&sql).await?;
        log::info!("ClickHouse returned {} rows", rows.len());

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_extractor() -> ClickHouseExtractor {
        ClickHouseExtractor::new(
            "http://localhost:8123".to_string(),
            "default".to_string(),
            "SELECT * FROM orders WHERE updated_at > '{last_run}'".to_string(),
            "default".to_string(),
            "".to_string(),
        )
        .unwrap()
    }

    #[test]
    fn test_convert_json_int() {
        let val = JsonValue::Number(serde_json::Number::from(42i64));
        assert_eq!(convert_json_value(val), Value::Int(42));
    }

    #[test]
    fn test_convert_json_float() {
        let val = JsonValue::Number(serde_json::Number::from_f64(3.14).unwrap());
        assert_eq!(convert_json_value(val), Value::Float(3.14));
    }

    #[test]
    fn test_convert_json_string() {
        assert_eq!(
            convert_json_value(JsonValue::String("hello".to_string())),
            Value::Text("hello".to_string())
        )
    }

    #[test]
    fn test_convert_json_bool() {
        assert_eq!(convert_json_value(JsonValue::Bool(true)), Value::Bool(true));
        assert_eq!(
            convert_json_value(JsonValue::Bool(false)),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_convert_json_null() {
        assert_eq!(convert_json_value(JsonValue::Null), Value::Null);
    }

    #[test]
    fn test_parse_json_row() {
        let line = r#"{"id":1, "name": "Alice", "amount": 100.5, "active": true }"#;
        let row = parse_json_row(line).unwrap();

        assert_eq!(row.get("id"), Some(&Value::Int(1)));
        assert_eq!(row.get("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(row.get("amount"), Some(&Value::Float(100.5)));
        assert_eq!(row.get("active"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_parse_json_row_with_null() {
        let line = r#"{"id": 2, "name": null, "amount": 0}"#;
        let row = parse_json_row(line).unwrap();

        assert_eq!(row.get("id"), Some(&Value::Int(2)));
        assert_eq!(row.get("name"), Some(&Value::Null));
    }

    #[test]
    fn test_parse_invalid_json() {
        let result = parse_json_row("not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_query() {
        let extractor = make_extractor();
        let last_run = chrono::DateTime::parse_from_rfc3339("2026-01-15T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let query = extractor.build_query(last_run);
        assert!(query.contains("2026-01-15 10:30:00"));
        assert!(!query.contains("{last_run}"));
    }

    #[test]
    fn test_format_added_if_missing() {
        let extractor = make_extractor();
        assert!(!extractor.query_template.contains("FORAMT"));
    }
}
