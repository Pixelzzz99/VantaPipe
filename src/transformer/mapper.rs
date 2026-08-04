use std::collections::HashMap;
use crate::types::Row;
use crate::error::EtlError;
use super::Transformer;

pub struct MapTransformer {
    pub rename: HashMap<String, String>
}

impl MapTransformer {
    pub fn new(rename: HashMap<String, String>) -> Self {
        Self { rename }
    }
}

impl Transformer for MapTransformer {
    fn transform(&self, rows: Vec<Row>) -> Result<Vec<Row>, EtlError> {
        let result = rows.into_iter()
            .map(|row| {
                row.into_iter()
            .map(|(key, value)| {
                let new_key = self.rename
                    .get(&key)
                    .cloned()
                    .unwrap_or(key);
                (new_key, value)
            }).collect::<Row>()
            }).collect::<Vec<Row>>();
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, make_row};

    #[test]
    fn test_rename_columns(){
        let mut rename = HashMap::new();
        rename.insert("user_id".to_string(), "client_id".to_string());
        rename.insert("amount".to_string(), "total".to_string());

        let mapper = MapTransformer::new(rename);
        let rows = vec![make_row(vec![
            ("user_id", Value::Int(1)),
            ("amount", Value::Float(99.9)),
            ("status", Value::Text("active".to_string())),
        ])];

        let result = mapper.transform(rows).unwrap();
        let row = &result[0];

        assert!(row.contains_key("client_id"));
        assert!(row.contains_key("total"));
        assert!(row.contains_key("status"));
        assert!(!row.contains_key("user_id"));
        assert!(!row.contains_key("amount"));
    }
}
