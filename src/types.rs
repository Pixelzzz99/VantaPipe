use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null
}

pub type Row = HashMap<String, Value>;

/// `Value` -> `serde_json::Value`. Used to hand rows to anything that
/// speaks JSON — currently the custom JS transform boundary
/// (`transformer::custom_js`).
pub fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Int(i) => serde_json::Value::from(*i),
        Value::Float(f) => serde_json::Value::from(*f),
        Value::Text(s) => serde_json::Value::from(s.clone()),
        Value::Bool(b) => serde_json::Value::from(*b),
        Value::Null => serde_json::Value::Null,
    }
}

/// `serde_json::Value` -> `Value`. Shared by the ClickHouse extractor
/// (`extractor::clickhouse`) and the custom JS transform boundary
/// (`transformer::custom_js`) — both need to turn arbitrary JSON back into
/// our row representation the same way.
pub fn json_to_value(json_val: serde_json::Value) -> Value {
    match json_val {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::String(s) => Value::Text(s),
        serde_json::Value::Number(n) => {
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

pub fn row_to_json(row: &Row) -> serde_json::Value {
    serde_json::Value::Object(
        row.iter()
            .map(|(k, v)| (k.clone(), value_to_json(v)))
            .collect(),
    )
}

pub fn json_to_row(json_val: serde_json::Value) -> Row {
    match json_val {
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(k, v)| (k, json_to_value(v)))
            .collect(),
        _ => Row::new(),
    }
}

/*
 * Помощная функция для создания строки (Row) из вектора пар (ключ, значение).
 * Как работает:
 *   - Принимает вектор кортежей, где каждый кортеж содержит строку (ключ) и значение типа Value. 
 *   - Преобразует каждый ключ в String и собирает пары в HashMap, который представляет собой
 *   строку (Row).
 */
#[cfg(test)]
pub fn make_row(pairs: Vec<(&str, Value)>) -> Row {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_variants(){
        let int_value = Value::Int(42);
        let float_value = Value::Float(3.14);
        let text_value = Value::Text("Hello".to_string());
        let bool_value = Value::Bool(true);
        let null_value = Value::Null;

        match int_value{
            Value::Int(i) => assert_eq!(i, 42),
            _ => panic!("Expected Int variant"),
        }

        assert_eq!(int_value, Value::Int(42));
        assert_eq!(float_value, Value::Float(3.14));
        assert_eq!(text_value, Value::Text("Hello".to_string()));
        assert_eq!(bool_value, Value::Bool(true));
        assert_eq!(null_value, Value::Null);
    }

    #[test]
    fn test_make_row(){
        let row = make_row(vec![
            ("id", Value::Int(1)),
            ("name", Value::Text("Alice".to_string())),
            ("is_active", Value::Bool(true)),
        ]);

        assert_eq!(row.get("id"), Some(&Value::Int(1)));
        assert_eq!(row.get("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(row.get("is_active"), Some(&Value::Bool(true)));
        assert_eq!(row.len(), 3);

    }
}
