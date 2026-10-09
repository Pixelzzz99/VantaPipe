use super::Transformer;
use crate::error::EtlError;
use crate::types::{Row, json_to_row, row_to_json};
use rquickjs::{Context, Function, Runtime, Value as JsValue};
use std::time::{Duration, Instant};

/// Runs a user-authored JS function over the whole row batch, via an
/// embedded QuickJS engine (`rquickjs`) — no external interpreter/process.
/// The script file is re-read on every `transform()` call rather than
/// cached, so edits take effect on the pipeline's next tick with no
/// reload needed (see `config::TransformConfig::Custom`).
pub struct CustomJsTransformer {
    script_path: String,
    function_name: String,
    timeout: Duration,
}

impl CustomJsTransformer {
    pub fn new(script_path: String, function_name: String, timeout: Duration) -> Self {
        Self {
            script_path,
            function_name,
            timeout,
        }
    }

    fn run(&self, rows: Vec<Row>) -> Result<Vec<Row>, EtlError> {
        let source = std::fs::read_to_string(&self.script_path).map_err(|e| {
            EtlError::TransformError(format!(
                "Cannot read script '{}': {}",
                self.script_path, e
            ))
        })?;

        let input_rows: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
        let input_json = serde_json::to_string(&input_rows).map_err(|e| {
            EtlError::TransformError(format!("Failed to serialize rows to JSON: {}", e))
        })?;

        let runtime = Runtime::new().map_err(|e| {
            EtlError::TransformError(format!("Failed to start QuickJS runtime: {}", e))
        })?;

        let deadline = Instant::now() + self.timeout;
        runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() > deadline)));

        let context = Context::full(&runtime).map_err(|e| {
            EtlError::TransformError(format!("Failed to create QuickJS context: {}", e))
        })?;

        let script_path = self.script_path.clone();
        let function_name = self.function_name.clone();

        let output_json: String = context.with(|ctx| -> Result<String, EtlError> {
            ctx.eval::<(), _>(source.into_bytes()).map_err(|e| {
                EtlError::TransformError(format!("Script error in '{}': {}", script_path, e))
            })?;

            let globals = ctx.globals();
            let func: Function = globals.get(function_name.as_str()).map_err(|_| {
                EtlError::TransformError(format!(
                    "Function '{}' not found in '{}'",
                    function_name, script_path
                ))
            })?;

            let input_value = ctx.json_parse(input_json).map_err(|e| {
                EtlError::TransformError(format!("Failed to hand rows to script: {}", e))
            })?;

            let result: JsValue = func.call((input_value,)).map_err(|e| {
                if Instant::now() > deadline {
                    EtlError::TransformError(format!(
                        "Script '{}' ({}) exceeded its {:?} timeout",
                        function_name, script_path, self.timeout
                    ))
                } else {
                    EtlError::TransformError(format!(
                        "Script '{}' ({}) threw: {}",
                        function_name, script_path, e
                    ))
                }
            })?;

            let out = ctx
                .json_stringify(result)
                .map_err(|e| {
                    EtlError::TransformError(format!(
                        "Failed to read result from '{}': {}",
                        script_path, e
                    ))
                })?
                .ok_or_else(|| {
                    EtlError::TransformError(format!(
                        "Function '{}' in '{}' returned undefined — expected an array of rows",
                        function_name, script_path
                    ))
                })?;

            out.to_string().map_err(|e| {
                EtlError::TransformError(format!(
                    "Failed to read result from '{}': {}",
                    script_path, e
                ))
            })
        })?;

        let output_rows: Vec<serde_json::Value> =
            serde_json::from_str(&output_json).map_err(|e| {
                EtlError::TransformError(format!(
                    "Function '{}' in '{}' must return an array of rows: {}",
                    self.function_name, self.script_path, e
                ))
            })?;

        Ok(output_rows.into_iter().map(json_to_row).collect())
    }
}

impl Transformer for CustomJsTransformer {
    fn transform(&self, rows: Vec<Row>) -> Result<Vec<Row>, EtlError> {
        self.run(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use std::io::Write;

    fn write_script(name: &str, body: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "etl_custom_js_test_{}_{}",
            name,
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.js", name));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn sample_rows() -> Vec<Row> {
        vec![
            [
                ("id".to_string(), Value::Int(1)),
                ("status".to_string(), Value::Text("active".to_string())),
                ("amount".to_string(), Value::Float(10.25)),
            ]
            .into_iter()
            .collect(),
            [
                ("id".to_string(), Value::Int(2)),
                ("status".to_string(), Value::Text("inactive".to_string())),
                ("amount".to_string(), Value::Float(20.0)),
            ]
            .into_iter()
            .collect(),
        ]
    }

    #[test]
    fn test_custom_js_transform_reshapes_rows() {
        let path = write_script(
            "reshape",
            r#"
            function transform(rows) {
                return rows
                    .filter(r => r.status === 'active')
                    .map(r => ({ id: r.id, doubled: r.amount * 2 }));
            }
            "#,
        );
        let t = CustomJsTransformer::new(path, "transform".to_string(), Duration::from_secs(5));
        let result = t.transform(sample_rows()).expect("should succeed");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(result[0].get("doubled"), Some(&Value::Float(20.5)));
    }

    #[test]
    fn test_custom_js_transform_custom_function_name() {
        let path = write_script(
            "custom_name",
            r#"function reshape(rows) { return rows; }"#,
        );
        let t = CustomJsTransformer::new(path, "reshape".to_string(), Duration::from_secs(5));
        let result = t.transform(sample_rows()).expect("should succeed");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_custom_js_transform_missing_function_errors() {
        let path = write_script("missing_fn", r#"function notTransform(rows) { return rows; }"#);
        let t = CustomJsTransformer::new(path, "transform".to_string(), Duration::from_secs(5));
        let err = t.transform(sample_rows()).unwrap_err();
        assert_eq!(err.kind(), "transform");
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_custom_js_transform_script_throws_errors() {
        let path = write_script(
            "throws",
            r#"function transform(rows) { throw new Error("boom"); }"#,
        );
        let t = CustomJsTransformer::new(path, "transform".to_string(), Duration::from_secs(5));
        let err = t.transform(sample_rows()).unwrap_err();
        assert_eq!(err.kind(), "transform");
    }

    #[test]
    fn test_custom_js_transform_missing_script_file_errors() {
        let t = CustomJsTransformer::new(
            "/nonexistent/path/does_not_exist.js".to_string(),
            "transform".to_string(),
            Duration::from_secs(5),
        );
        let err = t.transform(sample_rows()).unwrap_err();
        assert_eq!(err.kind(), "transform");
        assert!(err.to_string().contains("Cannot read script"));
    }

    #[test]
    fn test_custom_js_transform_infinite_loop_times_out() {
        let path = write_script("infinite", r#"function transform(rows) { while (true) {} }"#);
        let timeout = Duration::from_millis(200);
        let t = CustomJsTransformer::new(path, "transform".to_string(), timeout);

        let start = Instant::now();
        let err = t.transform(sample_rows()).unwrap_err();
        let elapsed = start.elapsed();

        assert_eq!(err.kind(), "transform");
        assert!(
            elapsed < timeout * 4,
            "expected the timeout to actually interrupt execution, took {:?}",
            elapsed
        );
    }
}
