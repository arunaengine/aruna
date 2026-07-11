use std::collections::{HashMap, HashSet};

use aruna_core::structs::SourceConnectorKind;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceConnectorValidationRules {
    pub required_public_keys: &'static [&'static str],
    pub allowed_public_keys: &'static [&'static str],
    pub required_secret_keys: &'static [&'static str],
    pub allowed_secret_keys: &'static [&'static str],
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("connector name must not be empty")]
    EmptyName,
    #[error("connector kind `{kind}` is not supported")]
    UnsupportedConnectorKind { kind: SourceConnectorKind },
    #[error("missing required public config key `{key}` for connector kind `{kind}`")]
    MissingRequiredPublicKey {
        kind: SourceConnectorKind,
        key: String,
    },
    #[error("public config key `{key}` is not allowed for connector kind `{kind}`")]
    UnknownPublicKey {
        kind: SourceConnectorKind,
        key: String,
    },
    #[error("missing required secret config key `{key}` for connector kind `{kind}`")]
    MissingRequiredSecretKey {
        kind: SourceConnectorKind,
        key: String,
    },
    #[error("secret config key `{key}` is not allowed for connector kind `{kind}`")]
    UnknownSecretKey {
        kind: SourceConnectorKind,
        key: String,
    },
    #[error("public config key `{key}` must not be empty")]
    EmptyPublicValue { key: String },
    #[error("secret config key `{key}` must not be empty")]
    EmptySecretValue { key: String },
    #[error("connector endpoint `{endpoint}` is not a valid URL")]
    InvalidEndpointUrl { endpoint: String },
    #[error("connector endpoint scheme `{scheme}` is not allowed for connector kind `{kind}`")]
    InvalidEndpointScheme {
        kind: SourceConnectorKind,
        scheme: String,
    },
    #[error("connector endpoint port must not be `0`")]
    InvalidEndpointPort,
}

pub fn validate_connector_input(
    name: &str,
    kind: SourceConnectorKind,
    public_config: &HashMap<String, String>,
    secret_config: &HashMap<String, String>,
) -> Result<(), ValidationError> {
    if name.trim().is_empty() {
        return Err(ValidationError::EmptyName);
    }

    if kind == SourceConnectorKind::ArunaNative {
        return Err(ValidationError::UnsupportedConnectorKind { kind });
    }

    for (key, value) in public_config {
        if value.trim().is_empty() {
            return Err(ValidationError::EmptyPublicValue { key: key.clone() });
        }
    }

    for (key, value) in secret_config {
        if value.trim().is_empty() {
            return Err(ValidationError::EmptySecretValue { key: key.clone() });
        }
    }

    let rules = rules_for_kind(kind);
    let allowed_public: HashSet<_> = rules.allowed_public_keys.iter().copied().collect();
    let allowed_secret: HashSet<_> = rules.allowed_secret_keys.iter().copied().collect();

    for key in public_config.keys() {
        if !allowed_public.contains(key.as_str()) {
            return Err(ValidationError::UnknownPublicKey {
                kind,
                key: key.clone(),
            });
        }
    }

    for key in secret_config.keys() {
        if !allowed_secret.contains(key.as_str()) {
            return Err(ValidationError::UnknownSecretKey {
                kind,
                key: key.clone(),
            });
        }
    }

    for key in rules.required_public_keys {
        if !public_config.contains_key(*key) {
            return Err(ValidationError::MissingRequiredPublicKey {
                kind,
                key: (*key).to_string(),
            });
        }
    }

    for key in rules.required_secret_keys {
        if !secret_config.contains_key(*key) {
            return Err(ValidationError::MissingRequiredSecretKey {
                kind,
                key: (*key).to_string(),
            });
        }
    }

    if let Some(endpoint) = public_config.get("endpoint") {
        validate_endpoint(kind, endpoint)?;
    }

    Ok(())
}

/// Static endpoint validation for create/replace: scheme per kind and explicit
/// port `0`. Fast UX feedback only — the egress enforcement layer (which is
/// allowlist-aware) is the SSRF security boundary, so denied IP literals are not
/// rejected here to keep allowlisted internal endpoints creatable.
fn validate_endpoint(kind: SourceConnectorKind, endpoint: &str) -> Result<(), ValidationError> {
    let url = url::Url::parse(endpoint).map_err(|_| ValidationError::InvalidEndpointUrl {
        endpoint: endpoint.to_string(),
    })?;

    let allowed_schemes: &[&str] = match kind {
        SourceConnectorKind::Ftp => &["ftp"],
        _ => &["http", "https"],
    };
    if !allowed_schemes
        .iter()
        .any(|scheme| scheme.eq_ignore_ascii_case(url.scheme()))
    {
        return Err(ValidationError::InvalidEndpointScheme {
            kind,
            scheme: url.scheme().to_string(),
        });
    }

    if url.port() == Some(0) {
        return Err(ValidationError::InvalidEndpointPort);
    }

    Ok(())
}

pub const fn rules_for_kind(kind: SourceConnectorKind) -> SourceConnectorValidationRules {
    match kind {
        SourceConnectorKind::Http => SourceConnectorValidationRules {
            required_public_keys: &["endpoint"],
            allowed_public_keys: &["endpoint", "root"],
            required_secret_keys: &[],
            allowed_secret_keys: &["username", "password", "token"],
        },
        // Static credentials are mandatory: a credential-less S3 client would
        // fall back to ambient discovery (env/profile/IMDS), an SSRF vector.
        SourceConnectorKind::S3 => SourceConnectorValidationRules {
            required_public_keys: &["bucket", "endpoint"],
            allowed_public_keys: &["bucket", "endpoint", "region", "root"],
            required_secret_keys: &["access_key_id", "secret_access_key"],
            allowed_secret_keys: &["access_key_id", "secret_access_key"],
        },
        SourceConnectorKind::Webdav => SourceConnectorValidationRules {
            required_public_keys: &["endpoint"],
            allowed_public_keys: &["endpoint", "root"],
            required_secret_keys: &[],
            allowed_secret_keys: &["username", "password", "token"],
        },
        SourceConnectorKind::Ftp => SourceConnectorValidationRules {
            required_public_keys: &["endpoint"],
            allowed_public_keys: &["endpoint", "root"],
            required_secret_keys: &[],
            allowed_secret_keys: &["user", "password"],
        },
        SourceConnectorKind::ArunaNative => SourceConnectorValidationRules {
            required_public_keys: &["endpoint"],
            allowed_public_keys: &["endpoint", "realm_id", "default_node_id"],
            required_secret_keys: &[],
            allowed_secret_keys: &["bearer_token", "access_key", "secret_key"],
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_name() {
        let err = validate_connector_input(
            "  ",
            SourceConnectorKind::Http,
            &HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            &HashMap::new(),
        )
        .unwrap_err();

        assert_eq!(err, ValidationError::EmptyName);
    }

    #[test]
    fn rejects_unknown_public_key() {
        let err = validate_connector_input(
            "http",
            SourceConnectorKind::Http,
            &HashMap::from([
                ("endpoint".to_string(), "https://example.org".to_string()),
                ("bucket".to_string(), "nope".to_string()),
            ]),
            &HashMap::new(),
        )
        .unwrap_err();

        assert_eq!(
            err,
            ValidationError::UnknownPublicKey {
                kind: SourceConnectorKind::Http,
                key: "bucket".to_string(),
            }
        );
    }

    #[test]
    fn rejects_missing_required_public_key() {
        let err = validate_connector_input(
            "s3",
            SourceConnectorKind::S3,
            &HashMap::from([("bucket".to_string(), "reads".to_string())]),
            &HashMap::new(),
        )
        .unwrap_err();

        assert_eq!(
            err,
            ValidationError::MissingRequiredPublicKey {
                kind: SourceConnectorKind::S3,
                key: "endpoint".to_string(),
            }
        );
    }

    #[test]
    fn requires_s3_credentials() {
        let public = HashMap::from([
            ("bucket".to_string(), "reads".to_string()),
            ("endpoint".to_string(), "https://s3.example.org".to_string()),
        ]);

        let err = validate_connector_input("s3", SourceConnectorKind::S3, &public, &HashMap::new())
            .unwrap_err();
        assert_eq!(
            err,
            ValidationError::MissingRequiredSecretKey {
                kind: SourceConnectorKind::S3,
                key: "access_key_id".to_string(),
            }
        );

        let err = validate_connector_input(
            "s3",
            SourceConnectorKind::S3,
            &public,
            &HashMap::from([("access_key_id".to_string(), "ak".to_string())]),
        )
        .unwrap_err();
        assert_eq!(
            err,
            ValidationError::MissingRequiredSecretKey {
                kind: SourceConnectorKind::S3,
                key: "secret_access_key".to_string(),
            }
        );

        validate_connector_input(
            "s3",
            SourceConnectorKind::S3,
            &public,
            &HashMap::from([
                ("access_key_id".to_string(), "ak".to_string()),
                ("secret_access_key".to_string(), "sk".to_string()),
            ]),
        )
        .unwrap();
    }

    #[test]
    fn rejects_unknown_secret_key() {
        let err = validate_connector_input(
            "webdav",
            SourceConnectorKind::Webdav,
            &HashMap::from([(
                "endpoint".to_string(),
                "https://dav.example.org".to_string(),
            )]),
            &HashMap::from([("session_token".to_string(), "nope".to_string())]),
        )
        .unwrap_err();

        assert_eq!(
            err,
            ValidationError::UnknownSecretKey {
                kind: SourceConnectorKind::Webdav,
                key: "session_token".to_string(),
            }
        );
    }

    #[test]
    fn accepts_valid_ftp_config() {
        validate_connector_input(
            "ftp",
            SourceConnectorKind::Ftp,
            &HashMap::from([
                (
                    "endpoint".to_string(),
                    "ftp://ftp.example.org:21".to_string(),
                ),
                ("root".to_string(), "/datasets".to_string()),
            ]),
            &HashMap::from([
                ("user".to_string(), "alice".to_string()),
                ("password".to_string(), "secret".to_string()),
            ]),
        )
        .unwrap();
    }

    #[test]
    fn rejects_wrong_scheme() {
        let err = validate_connector_input(
            "http",
            SourceConnectorKind::Http,
            &HashMap::from([("endpoint".to_string(), "ftp://example.org".to_string())]),
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(matches!(err, ValidationError::InvalidEndpointScheme { .. }));
    }

    #[test]
    fn rejects_zero_port() {
        let err = validate_connector_input(
            "http",
            SourceConnectorKind::Http,
            &HashMap::from([("endpoint".to_string(), "http://example.org:0".to_string())]),
            &HashMap::new(),
        )
        .unwrap_err();
        assert_eq!(err, ValidationError::InvalidEndpointPort);
    }

    #[test]
    fn accepts_loopback_endpoint() {
        // Denied IP literals are enforced by the egress layer, not create-time
        // validation, so allowlisted internal endpoints stay creatable.
        validate_connector_input(
            "http",
            SourceConnectorKind::Http,
            &HashMap::from([("endpoint".to_string(), "http://127.0.0.1:9000".to_string())]),
            &HashMap::new(),
        )
        .unwrap();
    }

    #[test]
    fn accepts_http_endpoint() {
        validate_connector_input(
            "http",
            SourceConnectorKind::Http,
            &HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            &HashMap::new(),
        )
        .unwrap();
    }

    #[test]
    fn rejects_unsupported_aruna_native_connector_kind() {
        let err = validate_connector_input(
            "native",
            SourceConnectorKind::ArunaNative,
            &HashMap::from([(
                "endpoint".to_string(),
                "https://aruna.example.org".to_string(),
            )]),
            &HashMap::new(),
        )
        .unwrap_err();

        assert_eq!(
            err,
            ValidationError::UnsupportedConnectorKind {
                kind: SourceConnectorKind::ArunaNative,
            }
        );
    }
}
