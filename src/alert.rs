use serde_json::{Value, json};
use std::time::Duration;

/// Fire-and-forget POST — alerts must never block or slow down a tick.
/// A delivery failure is logged and dropped; this is a notification
/// channel, not a guaranteed-delivery system (see plan: no retry).
pub fn send_async(webhook_url: &str, payload: Value) {
    let url = webhook_url.to_string();
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let result = client
            .post(&url)
            .timeout(Duration::from_secs(5))
            .json(&payload)
            .send()
            .await;
        if let Err(e) = result {
            log::warn!("Failed to send alert webhook to {}: {}", url, e);
        }
    });
}

/// `"text"` is Slack incoming-webhook compatible (`{"text": "..."}` is all
/// Slack needs); the rest are structured fields for any other consumer.
pub fn error_payload(pipeline_id: &str, message: &str, kind: &str) -> Value {
    json!({
        "text": format!("🔴 [{}] error ({}): {}", pipeline_id, kind, message),
        "pipeline": pipeline_id,
        "status": "error",
        "error_kind": kind,
        "message": message,
    })
}

pub fn blocked_payload(pipeline_id: &str, reason: &str) -> Value {
    json!({
        "text": format!("🟣 [{}] blocked: {}", pipeline_id, reason),
        "pipeline": pipeline_id,
        "status": "blocked",
        "reason": reason,
    })
}

pub fn recovered_payload(pipeline_id: &str) -> Value {
    json!({
        "text": format!("✅ [{}] recovered", pipeline_id),
        "pipeline": pipeline_id,
        "status": "recovered",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_payload_shape() {
        let p = error_payload("demo", "boom", "query");
        assert_eq!(p["pipeline"], "demo");
        assert_eq!(p["status"], "error");
        assert_eq!(p["error_kind"], "query");
        assert_eq!(p["message"], "boom");
        assert!(p["text"].as_str().unwrap().contains("demo"));
        assert!(p["text"].as_str().unwrap().contains("boom"));
    }

    #[test]
    fn test_blocked_payload_shape() {
        let p = blocked_payload("demo", "waiting on 'upstream'");
        assert_eq!(p["pipeline"], "demo");
        assert_eq!(p["status"], "blocked");
        assert_eq!(p["reason"], "waiting on 'upstream'");
        assert!(p["text"].as_str().unwrap().contains("waiting on 'upstream'"));
    }

    #[test]
    fn test_recovered_payload_shape() {
        let p = recovered_payload("demo");
        assert_eq!(p["pipeline"], "demo");
        assert_eq!(p["status"], "recovered");
        assert!(p["text"].as_str().unwrap().contains("demo"));
    }
}
