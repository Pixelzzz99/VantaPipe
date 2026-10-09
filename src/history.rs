use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

const DEFAULT_CAPACITY: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RunOutcome {
    Success,
    Empty,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub pipeline_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: RunOutcome,
    pub rows: u64,
    pub error: Option<String>,
    #[serde(default)]
    pub error_kind: Option<String>,
    /// `Some("replay")` for a run started via `POST .../replay` (see
    /// `src/replay.rs`), `None`/absent for an ordinary scheduled or
    /// manually-triggered tick.
    #[serde(default)]
    pub trigger: Option<String>,
}

#[derive(Clone)]
pub struct RunHistoryStore {
    inner: Arc<RwLock<HashMap<String, VecDeque<RunRecord>>>>,
    history_dir: PathBuf,
    capacity: usize,
}

impl RunHistoryStore {
    pub fn new(history_dir: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&history_dir);
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            history_dir,
            capacity: DEFAULT_CAPACITY,
        }
    }

    pub fn load_pipeline(&self, pipeline_id: &str) {
        let path = self.jsonl_path(pipeline_id);
        let Ok(content) = std::fs::read_to_string(&path) else {
            return;
        };
        let mut records = VecDeque::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(rec) = serde_json::from_str::<RunRecord>(line) {
                records.push_back(rec);
            }
        }
        while records.len() > self.capacity {
            records.pop_front();
        }
        if let Ok(mut inner) = self.inner.write() {
            inner.insert(pipeline_id.to_string(), records);
        }
    }

    pub fn record_finished(&self, record: RunRecord) {
        let pipeline_id = record.pipeline_id.clone();
        if let Ok(mut inner) = self.inner.write() {
            let q = inner.entry(pipeline_id.clone()).or_default();
            q.push_back(record.clone());
            while q.len() > self.capacity {
                q.pop_front();
            }
        }
        self.append_jsonl(&pipeline_id, &record);
    }

    pub fn for_pipeline(&self, pipeline_id: &str) -> Vec<RunRecord> {
        let inner = self.inner.read().unwrap();
        inner
            .get(pipeline_id)
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn all(&self) -> Vec<RunRecord> {
        let inner = self.inner.read().unwrap();
        let mut all: Vec<_> = inner.values().flat_map(|q| q.iter().cloned()).collect();
        all.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        all
    }

    fn jsonl_path(&self, pipeline_id: &str) -> PathBuf {
        self.history_dir.join(format!("{}.jsonl", pipeline_id))
    }

    fn append_jsonl(&self, pipeline_id: &str, record: &RunRecord) {
        use std::io::Write;
        let path = self.jsonl_path(pipeline_id);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        else {
            log::warn!("Cannot open history file {}", path.display());
            return;
        };
        if let Ok(line) = serde_json::to_string(record) {
            let _ = writeln!(file, "{}", line);
        }
    }
}

pub fn history_dir_from_state_arg(state_arg: &str, legacy_state_file: bool) -> PathBuf {
    if legacy_state_file {
        let p = Path::new(state_arg);
        p.parent()
            .unwrap_or_else(|| Path::new("."))
            .join("history")
    } else {
        Path::new(state_arg).join("history")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_trim_and_persist() {
        let dir = std::env::temp_dir().join(format!("etl_hist_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = RunHistoryStore {
            inner: Arc::new(RwLock::new(HashMap::new())),
            history_dir: dir.clone(),
            capacity: 3,
        };

        for i in 0..5 {
            store.record_finished(RunRecord {
                pipeline_id: "p1".into(),
                started_at: Utc::now(),
                finished_at: Some(Utc::now()),
                status: RunOutcome::Success,
                rows: i,
                error: None,
                error_kind: None,
                trigger: None,
            });
        }

        let recs = store.for_pipeline("p1");
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].rows, 2);
        assert_eq!(recs[2].rows, 4);

        let store2 = RunHistoryStore::new(dir.clone());
        store2.load_pipeline("p1");
        // file has 5 lines; load keeps last capacity
        assert!(store2.for_pipeline("p1").len() <= 5);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_legacy_record_without_error_kind_deserializes() {
        let line = r#"{"pipeline_id":"p1","started_at":"2024-01-01T00:00:00Z","finished_at":null,"status":"success","rows":0,"error":null}"#;
        let rec: RunRecord = serde_json::from_str(line).expect("legacy record should deserialize");
        assert_eq!(rec.error_kind, None);
        assert_eq!(rec.trigger, None);
    }

    #[test]
    fn test_trigger_survives_jsonl_round_trip() {
        let dir = std::env::temp_dir().join(format!("etl_hist_trigger_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = RunHistoryStore::new(dir.clone());

        store.record_finished(RunRecord {
            pipeline_id: "p1".into(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
            status: RunOutcome::Success,
            rows: 3,
            error: None,
            error_kind: None,
            trigger: Some("replay".into()),
        });

        let store2 = RunHistoryStore::new(dir.clone());
        store2.load_pipeline("p1");
        let recs = store2.for_pipeline("p1");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].trigger.as_deref(), Some("replay"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_error_kind_survives_jsonl_round_trip() {
        let dir = std::env::temp_dir().join(format!("etl_hist_kind_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = RunHistoryStore::new(dir.clone());

        store.record_finished(RunRecord {
            pipeline_id: "p1".into(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
            status: RunOutcome::Error,
            rows: 0,
            error: Some("boom".into()),
            error_kind: Some("connection".into()),
            trigger: None,
        });

        let store2 = RunHistoryStore::new(dir.clone());
        store2.load_pipeline("p1");
        let recs = store2.for_pipeline("p1");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].error_kind.as_deref(), Some("connection"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
