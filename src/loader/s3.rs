use super::Loader;
use crate::error::EtlError;
use crate::types::{Row, row_to_json};
use async_trait::async_trait;
use object_store::{ObjectStore, PutPayload, path::Path as ObjPath};
use std::sync::Arc;

pub struct S3Loader {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    format: String,
}

impl S3Loader {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String, format: String) -> Self {
        Self {
            store,
            prefix,
            format,
        }
    }
}

/// One object per `load()` call — key is `{prefix}/{unix millis}.{ext}`,
/// so concurrent/successive loads never collide. Factored out as a plain
/// function (no I/O) so the naming/serialization logic is unit-testable.
fn build_object(prefix: &str, format: &str, rows: &[Row]) -> (String, Vec<u8>) {
    let millis = chrono::Utc::now().timestamp_millis();
    let ext = if format == "csv" { "csv" } else { "jsonl" };
    let key = format!("{}/{}.{}", prefix.trim_end_matches('/'), millis, ext);

    let body = if format == "csv" {
        serialize_csv(rows)
    } else {
        serialize_jsonl(rows)
    };

    (key, body)
}

fn serialize_jsonl(rows: &[Row]) -> Vec<u8> {
    let mut out = String::new();
    for row in rows {
        out.push_str(&row_to_json(row).to_string());
        out.push('\n');
    }
    out.into_bytes()
}

fn serialize_csv(rows: &[Row]) -> Vec<u8> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    let mut columns: Vec<String> = Vec::new();
    for row in rows {
        let mut keys: Vec<&String> = row.keys().collect();
        keys.sort();
        for k in keys {
            if !columns.contains(k) {
                columns.push(k.clone());
            }
        }
    }
    let _ = wtr.write_record(&columns);
    for row in rows {
        let record: Vec<String> = columns
            .iter()
            .map(|c| row.get(c).map(value_to_csv_cell).unwrap_or_default())
            .collect();
        let _ = wtr.write_record(&record);
    }
    wtr.into_inner().unwrap_or_default()
}

/// Plain cell text — not JSON. `value_to_json(v).to_string()` would wrap
/// `Text` in extra `"..."` quotes (it's JSON serialization), which the CSV
/// writer then quotes *again* since the cell starts with a literal `"`.
fn value_to_csv_cell(v: &crate::types::Value) -> String {
    use crate::types::Value;
    match v {
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Text(s) => s.clone(),
        Value::Null => String::new(),
    }
}

#[async_trait]
impl Loader for S3Loader {
    async fn load(&self, rows: Vec<Row>) -> Result<(), EtlError> {
        if rows.is_empty() {
            return Ok(());
        }

        let (key, body) = build_object(&self.prefix, &self.format, &rows);

        self.store
            .put(&ObjPath::from(key.as_str()), PutPayload::from(body))
            .await
            .map_err(|e| EtlError::LoadError(format!("Failed to PUT {}: {}", key, e)))?;

        log::info!("Loaded {} rows into s3://{}", rows.len(), key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, make_row};

    #[test]
    fn test_build_object_jsonl_key_and_body() {
        let rows = vec![make_row(vec![("id", Value::Int(1))])];
        let (key, body) = build_object("exports", "jsonl", &rows);
        assert!(key.starts_with("exports/"));
        assert!(key.ends_with(".jsonl"));
        let text = String::from_utf8(body).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(text.contains("\"id\":1"));
    }

    #[test]
    fn test_build_object_csv_key_and_body() {
        let rows = vec![make_row(vec![
            ("id", Value::Int(1)),
            ("name", Value::Text("Alice".to_string())),
        ])];
        let (key, body) = build_object("exports/", "csv", &rows);
        assert!(key.starts_with("exports/"));
        assert!(key.ends_with(".csv"));
        let text = String::from_utf8(body).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next().unwrap(), "id,name");
        assert_eq!(lines.next().unwrap(), "1,Alice");
    }

    #[tokio::test]
    async fn test_s3_loader_round_trip_via_in_memory_store() {
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjPath;

        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let loader = S3Loader::new(Arc::clone(&store), "exports".to_string(), "jsonl".to_string());

        let rows = vec![make_row(vec![
            ("id", Value::Int(1)),
            ("name", Value::Text("Alice".to_string())),
        ])];
        loader.load(rows).await.unwrap();

        // Exactly one object landed under the prefix, and it round-trips.
        let mut stream = store.list(Some(&ObjPath::from("exports")));
        use futures_util::StreamExt;
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            keys.push(meta.unwrap().location);
        }
        assert_eq!(keys.len(), 1);

        let body = store.get(&keys[0]).await.unwrap().bytes().await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("\"id\":1"));
        assert!(text.contains("\"name\":\"Alice\""));
    }
}
