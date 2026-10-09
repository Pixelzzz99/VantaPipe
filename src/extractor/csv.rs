use super::Extractor;
use crate::error::EtlError;
use crate::state::PersistentState;
use crate::types::{Row, Value};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct CsvExtractor {
    watch_dir: PathBuf,
    processed_dir: PathBuf,
    delimiter: u8,
    state: Arc<Mutex<PersistentState>>,
    state_path: String,
}

impl CsvExtractor {
    pub fn new(
        watch_dir: &str,
        processed_dir: &str,
        delimiter: char,
        state: Arc<Mutex<PersistentState>>,
        state_path: String,
    ) -> Result<Self, EtlError> {
        std::fs::create_dir_all(watch_dir)
            .map_err(|e| EtlError::ConfigError(format!("Cannot create watch directory: {}", e)))?;
        std::fs::create_dir_all(processed_dir).map_err(|e| {
            EtlError::ConfigError(format!("Cannot create processed directory: {}", e))
        })?;
        Ok(Self {
            watch_dir: PathBuf::from(watch_dir),
            processed_dir: PathBuf::from(processed_dir),
            delimiter: delimiter as u8,
            state,
            state_path,
        })
    }

    fn find_new_csv_files(&self) -> Result<Vec<PathBuf>, EtlError> {
        let state = self.state.lock().unwrap();

        let mut files: Vec<PathBuf> = std::fs::read_dir(&self.watch_dir)
            .map_err(|e| EtlError::ConfigError(format!("Cannot read watch_dir: {}", e)))?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("csv"))
                    .unwrap_or(false)
            })
            .filter(|path| {
                let filename = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("");
                !state.is_file_processed(&filename)
            })
            .collect();

        files.sort();
        Ok(files)
    }

    fn read_csv_file(&self, path: &Path) -> Result<Vec<Row>, EtlError> {
        read_csv_file(path, self.delimiter)
    }

    fn move_to_processed(&self, file_path: &Path) -> Result<(), EtlError> {
        let file_name = file_path
            .file_name()
            .ok_or_else(|| EtlError::ConfigError("Invalid file name".to_string()))?;
        let dest_path = self.processed_dir.join(file_name);
        std::fs::rename(file_path, dest_path).map_err(|e| {
            EtlError::LoadError(format!(
                "Cannot move {} to processed: {}",
                file_path.display(),
                e
            ))
        })?;
        Ok(())
    }
}

fn read_csv_file(path: &Path, delimiter: u8) -> Result<Vec<Row>, EtlError> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .from_path(path)
        .map_err(|e| {
            EtlError::QueryError(format!("Cannot open CSV file: {}: {}", path.display(), e))
        })?;

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| EtlError::QueryError(e.to_string()))?
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut rows = Vec::new();

    for result in reader.records() {
        let record =
            result.map_err(|e| EtlError::QueryError(format!("CSV parse error: {}", e)))?;

        let row: Row = headers
            .iter()
            .zip(record.iter())
            .map(|(header, value)| (header.clone(), parse_csv_value(value)))
            .collect();

        rows.push(row);
    }

    Ok(rows)
}

/// Re-read specific already-processed files directly from `processed_dir`
/// (where `move_to_processed` archives them) — used by the replay path
/// (`src/replay.rs`). Deliberately bypasses `CsvExtractor`/`find_new_csv_files`
/// entirely: no dedup check, no file move, no `PersistentState` touched, so
/// it can't race the live worker.
pub fn read_processed_files(
    processed_dir: &str,
    filenames: &[String],
    delimiter: char,
) -> Result<Vec<Row>, EtlError> {
    let dir = Path::new(processed_dir);
    let mut all_rows = Vec::new();
    for filename in filenames {
        let path = dir.join(filename);
        let rows = read_csv_file(&path, delimiter as u8)?;
        all_rows.extend(rows);
    }
    Ok(all_rows)
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
        "true" | "yes" => return Value::Bool(true),
        "false" | "no" => return Value::Bool(false),
        _ => {}
    }

    Value::Text(s.to_string())
}

#[async_trait]
impl Extractor for CsvExtractor {
    async fn extract(
        &self,
        _last_run: DateTime<Utc>,
        _until: Option<DateTime<Utc>>,
    ) -> Result<Vec<Row>, EtlError> {
        let files = self.find_new_csv_files()?;
        if files.is_empty() {
            log::info!("No new CSV files in {:?}", self.watch_dir);
            return Ok(vec![]);
        }

        log::info!("Found {} new CSV file(s)", files.len());
        let mut all_rows = Vec::new();

        for path in &files {
            let filename = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("unknown");

            log::info!("Reading: {}", filename);
            let rows = self.read_csv_file(path)?;
            log::info!("Read {} rows from {}", rows.len(), filename);

            all_rows.extend(rows);

            self.move_to_processed(path)?;

            {
                let mut state = self.state.lock().unwrap();
                state.mark_file_processed(filename);
                state.last_run = Utc::now();
            }

            let state_snapshot = self.state.lock().unwrap().clone();
            state_snapshot.save(&self.state_path)?;
        }
        Ok(all_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_int() {
        assert_eq!(parse_csv_value("42"), Value::Int(42));
        assert_eq!(parse_csv_value("-10"), Value::Int(-10));
    }

    #[test]
    fn test_parse_float() {
        assert_eq!(parse_csv_value("3.14"), Value::Float(3.14));
        assert_eq!(parse_csv_value("-0.001"), Value::Float(-0.001));
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(parse_csv_value("true"), Value::Bool(true));
        assert_eq!(parse_csv_value("no"), Value::Bool(false));
    }

    #[test]
    fn test_parse_text() {
        assert_eq!(parse_csv_value("active"), Value::Text("active".to_string()));
    }

    #[test]
    fn test_parse_null() {
        assert_eq!(parse_csv_value(""), Value::Null);
    }

    #[test]
    fn test_read_csv_file() {
        let tmp = "/tmp/elt_test_read.csv";
        std::fs::write(tmp, "id,amount,status\n1,100.0,active\n2,200.5,inactive\n").unwrap();

        let state = Arc::new(Mutex::new(PersistentState::new()));
        let extractor = CsvExtractor::new(
            "/tmp",
            "/tmp/processed_test",
            ',',
            state,
            "/tmp/etl_state_csv_test.json".to_string(),
        )
        .unwrap();

        let rows = extractor.read_csv_file(Path::new(tmp)).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(rows[0].get("amount"), Some(&Value::Float(100.0)));
        assert_eq!(
            rows[0].get("status"),
            Some(&Value::Text("active".to_string()))
        );

        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_read_processed_files() {
        let dir = std::env::temp_dir()
            .join(format!("etl_csv_replay_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("orders-1.csv"),
            "id,amount\n1,100.0\n2,200.5\n",
        )
        .unwrap();

        let rows = read_processed_files(
            dir.to_str().unwrap(),
            &["orders-1.csv".to_string()],
            ',',
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&Value::Int(1)));

        std::fs::remove_dir_all(&dir).ok();
    }
}
