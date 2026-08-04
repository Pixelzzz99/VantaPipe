use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use std::env;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct BasicAuthConfig {
    pub username: String,
    pub password: String,
}

impl BasicAuthConfig {
    /// Load from ETL_AUTH_USER / ETL_AUTH_PASS. `Ok(None)` means auth is
    /// disabled (both unset) — the default, so local/dev usage is unaffected.
    /// `Err` means a partial configuration (only one of the two set), which
    /// is treated as a startup error rather than silently running
    /// half-protected.
    pub fn from_env() -> Result<Option<Self>, String> {
        Self::from_values(env::var("ETL_AUTH_USER").ok(), env::var("ETL_AUTH_PASS").ok())
    }

    fn from_values(user: Option<String>, pass: Option<String>) -> Result<Option<Self>, String> {
        match (user, pass) {
            (None, None) => Ok(None),
            (Some(username), Some(password)) => Ok(Some(Self { username, password })),
            (Some(_), None) => Err(
                "ETL_AUTH_USER is set but ETL_AUTH_PASS is not — set both or neither".to_string(),
            ),
            (None, Some(_)) => Err(
                "ETL_AUTH_PASS is set but ETL_AUTH_USER is not — set both or neither".to_string(),
            ),
        }
    }
}

/// Constant-time byte comparison, to avoid leaking credential content or
/// length via response-timing side channels.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Check an `Authorization` header value against `config`. Expects
/// `Basic <base64(username:password)>`.
pub fn verify_basic_auth(header_value: Option<&str>, config: &BasicAuthConfig) -> bool {
    let Some(header_value) = header_value else {
        return false;
    };
    let Some(encoded) = header_value.strip_prefix("Basic ") else {
        return false;
    };
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(decoded) = String::from_utf8(decoded) else {
        return false;
    };
    let Some((user, pass)) = decoded.split_once(':') else {
        return false;
    };
    constant_time_eq(user.as_bytes(), config.username.as_bytes())
        && constant_time_eq(pass.as_bytes(), config.password.as_bytes())
}

fn unauthorized_response() -> Response {
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"etl-engine\""),
    );
    response
}

/// Axum middleware gating every route behind HTTP Basic Auth. A no-op pass-
/// through when `config` is `None` (auth disabled).
pub async fn require_basic_auth(
    State(config): State<Arc<Option<BasicAuthConfig>>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let Some(config) = config.as_ref() else {
        return next.run(req).await;
    };

    let header_value = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    if verify_basic_auth(header_value, config) {
        next.run(req).await
    } else {
        unauthorized_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_values_both_unset_disables_auth() {
        assert_eq!(BasicAuthConfig::from_values(None, None), Ok(None));
    }

    #[test]
    fn test_from_values_both_set_enables_auth() {
        let result = BasicAuthConfig::from_values(
            Some("admin".to_string()),
            Some("secret".to_string()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.username, "admin");
        assert_eq!(result.password, "secret");
    }

    #[test]
    fn test_from_values_only_user_set_is_error() {
        assert!(BasicAuthConfig::from_values(Some("admin".to_string()), None).is_err());
    }

    #[test]
    fn test_from_values_only_pass_set_is_error() {
        assert!(BasicAuthConfig::from_values(None, Some("secret".to_string())).is_err());
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"short", b"longer"));
        assert!(constant_time_eq(b"", b""));
    }

    fn config() -> BasicAuthConfig {
        BasicAuthConfig {
            username: "admin".to_string(),
            password: "s3cr3t".to_string(),
        }
    }

    fn basic_header(user: &str, pass: &str) -> String {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", user, pass));
        format!("Basic {}", encoded)
    }

    #[test]
    fn test_verify_basic_auth_correct_credentials() {
        let header = basic_header("admin", "s3cr3t");
        assert!(verify_basic_auth(Some(&header), &config()));
    }

    #[test]
    fn test_verify_basic_auth_wrong_password() {
        let header = basic_header("admin", "wrong");
        assert!(!verify_basic_auth(Some(&header), &config()));
    }

    #[test]
    fn test_verify_basic_auth_wrong_username() {
        let header = basic_header("someone_else", "s3cr3t");
        assert!(!verify_basic_auth(Some(&header), &config()));
    }

    #[test]
    fn test_verify_basic_auth_missing_header() {
        assert!(!verify_basic_auth(None, &config()));
    }

    #[test]
    fn test_verify_basic_auth_malformed_header() {
        assert!(!verify_basic_auth(Some("not-basic-at-all"), &config()));
        assert!(!verify_basic_auth(Some("Basic not-valid-base64!!!"), &config()));
        assert!(!verify_basic_auth(
            Some(&format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("no-colon-here")
            )),
            &config()
        ));
    }
}
