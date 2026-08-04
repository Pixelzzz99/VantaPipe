use std::fmt;

#[derive(Debug)]
pub enum EtlError {
    ConnectionError(String),
    QueryError(String),
    ConfigError(String),
    LoadError(String),
}

impl fmt::Display for EtlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EtlError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            EtlError::QueryError(msg) => write!(f, "Query error: {}", msg),
            EtlError::ConfigError(msg) => write!(f, "Config error: {}", msg),
            EtlError::LoadError(msg) => write!(f, "Load error: {}", msg),
        }
    }
}

impl EtlError {
    pub fn kind(&self) -> &'static str {
        match self {
            EtlError::ConnectionError(_) => "connection",
            EtlError::QueryError(_) => "query",
            EtlError::ConfigError(_) => "config",
            EtlError::LoadError(_) => "load",
        }
    }
}

impl std::error::Error for EtlError {}


impl From<sqlx::Error> for EtlError {
    fn from(e: sqlx::Error) -> Self {
        EtlError::QueryError(e.to_string())
    }
}

impl From<serde_json::Error> for EtlError {
    fn from(e:serde_json::Error) -> Self {
        EtlError::ConfigError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_etl_error_display() {
        let error = EtlError::ConnectionError("Failed to connect to database".to_string());
        assert_eq!(format!("{}", error), "Connection error: Failed to connect to database");
    }

    #[test]
    fn test_all_variants_display(){
        let errors = vec![
            EtlError::ConnectionError('c'.to_string()),
            EtlError::QueryError('q'.to_string()),
            EtlError::ConfigError('g'.to_string()),
            EtlError::LoadError('l'.to_string()),
        ];

        for err in errors {
            assert!(!err.to_string().is_empty(), "Error message should not be empty");
        }
    }

    #[test]
    fn test_error_kind() {
        assert_eq!(EtlError::ConnectionError("x".to_string()).kind(), "connection");
        assert_eq!(EtlError::QueryError("x".to_string()).kind(), "query");
        assert_eq!(EtlError::ConfigError("x".to_string()).kind(), "config");
        assert_eq!(EtlError::LoadError("x".to_string()).kind(), "load");
    }
}
