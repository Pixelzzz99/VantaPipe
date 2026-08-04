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
