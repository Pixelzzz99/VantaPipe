use super::Extractor;
use crate::error::EtlError;
use crate::state::PersistentState;
use crate::types::{Row, Value, json_to_row};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use object_store::{ObjectStore, path::Path as ObjPath};
use std::sync::{Arc, Mutex};

pub struct S3Extractor {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    format: String,
    delimiter: u8,
    state: Arc<Mutex<PersistentState>>,
    state_path: String,
}

impl S3Extractor {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        format: String,
        delimiter: char,
        state: Arc<Mutex<PersistentState>>,
        state_path: String,
    ) -> Self {
        Self {
            store,
            prefix,
            format,
            delimiter: delimiter as u8,
            state,
            state_path,
        }
    }

    /// Objects under `prefix` not already marked processed in
    /// `PersistentState` — same dedup mechanism `CsvExtractor` already
    /// uses (`mark_file_processed`/`is_file_processed`), just keyed by S3
    /// object key instead of a local filename.
    async fn find_new_keys(&self) -> Result<Vec<String>, EtlError> {
        let prefix_path = ObjPath::from(self.prefix.as_str());
        let mut stream = self.store.list(Some(&prefix_path));
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| {
                EtlError::ConnectionError(format!("Failed to list s3://{}: {}", self.prefix, e))
            })?;
            keys.push(meta.location.to_string());
        }
        keys.sort();

        let state = self.state.lock().unwrap();
        Ok(keys
            .into_iter()
            .filter(|k| !state.is_file_processed(k))
            .collect())
    }

    /// Re-GET specific keys directly, bypassing `find_new_keys`/dedup
    /// entirely — no `is_file_processed` check, no `mark_file_processed`,
    /// no `.save()`. Used by the replay path (`src/replay.rs`); safe to run
    /// alongside the live worker since nothing here mutates shared state
    /// and S3 objects aren't moved/deleted on read.
    pub async fn read_keys(&self, keys: &[String]) -> Result<Vec<Row>, EtlError> {
        let mut all_rows = Vec::new();
        for key in keys {
            let rows = self.read_object(key).await?;
            all_rows.extend(rows);
        }
        Ok(all_rows)
    }

    async fn read_object(&self, key: &str) -> Result<Vec<Row>, EtlError> {
        let result = self
            .store
            .get(&ObjPath::from(key))
            .await
            .map_err(|e| EtlError::ConnectionError(format!("Failed to GET {}: {}", key, e)))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| EtlError::QueryError(format!("Failed to read {}: {}", key, e)))?;

        match self.format.as_str() {
            "csv" => parse_csv_bytes(&bytes, self.delimiter, key),
            _ => parse_jsonl_bytes(&bytes, key),
        }
    }
}

fn parse_csv_bytes(bytes: &[u8], delimiter: u8, key: &str) -> Result<Vec<Row>, EtlError> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .from_reader(bytes);

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| EtlError::QueryError(format!("{}: {}", key, e)))?
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result.map_err(|e| EtlError::QueryError(format!("{}: {}", key, e)))?;
        let row: Row = headers
            .iter()
            .zip(record.iter())
            .map(|(header, value)| (header.clone(), parse_csv_value(value)))
            .collect();
        rows.push(row);
    }
    Ok(rows)
}

fn parse_csv_value(s: &str) -> Value {
    if s.is_empty() {
        return Value::Null;
    }
    if let Ok(n) = s.parse::<i64>() {
        return Value::Int(n);
    }
    if let Ok(n) = s.parse::<f64>() {
        return Value::Float(n);
    }
    match s.to_lowercase().as_str() {
        "true" | "yes" => Value::Bool(true),
        "false" | "no" => Value::Bool(false),
        _ => Value::Text(s.to_string()),
    }
}

fn parse_jsonl_bytes(bytes: &[u8], key: &str) -> Result<Vec<Row>, EtlError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| EtlError::QueryError(format!("{} is not valid UTF-8: {}", key, e)))?;
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let json: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| EtlError::QueryError(format!("{}: invalid JSON line: {}", key, e)))?;
        rows.push(json_to_row(json));
    }
    Ok(rows)
}

#[async_trait]
impl Extractor for S3Extractor {
    async fn extract(
        &self,
        _last_run: DateTime<Utc>,
        _until: Option<DateTime<Utc>>,
    ) -> Result<Vec<Row>, EtlError> {
        let keys = self.find_new_keys().await?;
        if keys.is_empty() {
            log::info!("No new objects under s3://{}", self.prefix);
            return Ok(vec![]);
        }

        log::info!("Found {} new object(s) under s3://{}", keys.len(), self.prefix);
        let mut all_rows = Vec::new();

        for key in &keys {
            log::info!("Reading: {}", key);
            let rows = self.read_object(key).await?;
            log::info!("Read {} rows from {}", rows.len(), key);
            all_rows.extend(rows);

            let mut state = self.state.lock().unwrap();
            state.mark_file_processed(key);
            state.last_run = Utc::now();
            let snapshot = state.clone();
            drop(state);
            snapshot.save(&self.state_path)?;
        }

        Ok(all_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::PersistentState;
    use object_store::memory::InMemory;

    fn temp_state_path(name: &str) -> String {
        std::env::temp_dir()
            .join(format!("etl_s3_extractor_test_{}_{}.json", name, std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    async fn put_jsonl(store: &InMemory, key: &str, lines: &[&str]) {
        let body = lines.join("\n");
        store
            .put(&ObjPath::from(key), body.into_bytes().into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_s3_extractor_reads_jsonl_and_dedupes_on_second_poll() {
        let store = InMemory::new();
        put_jsonl(
            &store,
            "incoming/orders-1.jsonl",
            &[r#"{"id": 1, "status": "active"}"#, r#"{"id": 2, "status": "inactive"}"#],
        )
        .await;

        let state_path = temp_state_path("dedup");
        let extractor = S3Extractor::new(
            Arc::new(store),
            "incoming".to_string(),
            "jsonl".to_string(),
            ',',
            Arc::new(Mutex::new(PersistentState::new())),
            state_path.clone(),
        );

        let rows = extractor.extract(Utc::now(), None).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&Value::Int(1)));

        // Second poll: same object, nothing new.
        let rows2 = extractor.extract(Utc::now(), None).await.unwrap();
        assert!(rows2.is_empty(), "expected the already-processed object to be skipped");

        let _ = std::fs::remove_file(&state_path);
    }

    #[tokio::test]
    async fn test_s3_extractor_reads_csv() {
        let store = InMemory::new();
        store
            .put(
                &ObjPath::from("incoming/orders.csv"),
                "id,name\n1,Alice\n2,Bob".to_string().into_bytes().into(),
            )
            .await
            .unwrap();

        let state_path = temp_state_path("csv");
        let extractor = S3Extractor::new(
            Arc::new(store),
            "incoming".to_string(),
            "csv".to_string(),
            ',',
            Arc::new(Mutex::new(PersistentState::new())),
            state_path.clone(),
        );

        let rows = extractor.extract(Utc::now(), None).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("name"), Some(&Value::Text("Alice".to_string())));

        let _ = std::fs::remove_file(&state_path);
    }

    #[tokio::test]
    async fn test_read_keys_bypasses_dedup_state() {
        let store = InMemory::new();
        put_jsonl(&store, "incoming/orders-1.jsonl", &[r#"{"id": 1}"#]).await;

        let state_path = temp_state_path("replay");
        let state = Arc::new(Mutex::new(PersistentState::new()));
        {
            let mut s = state.lock().unwrap();
            s.mark_file_processed("incoming/orders-1.jsonl");
        }
        let extractor = S3Extractor::new(
            Arc::new(store),
            "incoming".to_string(),
            "jsonl".to_string(),
            ',',
            Arc::clone(&state),
            state_path.clone(),
        );

        // Already marked processed above — a normal extract() would skip
        // it, but read_keys() must re-read it regardless.
        let rows = extractor
            .read_keys(&["incoming/orders-1.jsonl".to_string()])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::Int(1)));

        // Dedup state untouched by read_keys — still exactly what we set above.
        assert!(state.lock().unwrap().is_file_processed("incoming/orders-1.jsonl"));

        let _ = std::fs::remove_file(&state_path);
    }
}
