use std::collections::{HashMap, HashSet};

use aruna_core::structs::SourceConnectorKind;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceConnectorValidationRules {
    pub required_public_keys: &'static [&'static str],
    pub allowed_public_keys: &'static [&'static str],
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
    #[error("secret config key `{key}` is not allowed for connector kind `{kind}`")]
    UnknownSecretKey {
        kind: SourceConnectorKind,
        key: String,
    },
    #[error("public config key `{key}` must not be empty")]
    EmptyPublicValue { key: String },
    #[error("secret config key `{key}` must not be empty")]
    EmptySecretValue { key: String },
    #[error("public config key `{key}` must be `true` or `false`")]
    InvalidBoolValue { key: String },
    #[error("credentials must not be set when `skip_signature` is enabled")]
    CredentialsWithSkipSignature,
    #[error("signed s3 connectors require `{ACCESS_KEY_ID}` and `{SECRET_ACCESS_KEY}`")]
    MissingCredentials,
}

pub const S3_SKIP_SIGNATURE: &str = "skip_signature";
const ACCESS_KEY_ID: &str = "access_key_id";
const SECRET_ACCESS_KEY: &str = "secret_access_key";

pub fn validate_connector_input(
    name: &str,
    kind: SourceConnectorKind,
    public_config: &HashMap<String, String>,
    secret_config: &HashMap<String, String>,
) -> Result<(), ValidationError> {
    if name.trim().is_empty() {
        return Err(ValidationError::EmptyName);
    }

    // ftp is refused because opendal cannot constrain its passive data address.
    if matches!(
        kind,
        SourceConnectorKind::Ftp | SourceConnectorKind::ArunaNative
    ) {
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

    let mut anonymous = false;
    if let Some(value) = public_config.get(S3_SKIP_SIGNATURE) {
        if value != "true" && value != "false" {
            return Err(ValidationError::InvalidBoolValue {
                key: S3_SKIP_SIGNATURE.to_string(),
            });
        }
        anonymous = value == "true";
        if anonymous && !secret_config.is_empty() {
            return Err(ValidationError::CredentialsWithSkipSignature);
        }
    }

    // Without static keys a signed connector makes reqsign walk the node's own
    // ambient credential chain against a tenant-chosen endpoint.
    if kind == SourceConnectorKind::S3
        && !anonymous
        && !(secret_config.contains_key(ACCESS_KEY_ID)
            && secret_config.contains_key(SECRET_ACCESS_KEY))
    {
        return Err(ValidationError::MissingCredentials);
    }

    Ok(())
}

pub const fn rules_for_kind(kind: SourceConnectorKind) -> SourceConnectorValidationRules {
    match kind {
        SourceConnectorKind::Http => SourceConnectorValidationRules {
            required_public_keys: &["endpoint"],
            allowed_public_keys: &["endpoint", "root"],
            allowed_secret_keys: &["username", "password", "token"],
        },
        SourceConnectorKind::S3 => SourceConnectorValidationRules {
            required_public_keys: &["bucket", "endpoint"],
            allowed_public_keys: &["bucket", "endpoint", "region", "root", S3_SKIP_SIGNATURE],
            allowed_secret_keys: &["access_key_id", "secret_access_key"],
        },
        SourceConnectorKind::Webdav => SourceConnectorValidationRules {
            required_public_keys: &["endpoint"],
            allowed_public_keys: &["endpoint", "root"],
            allowed_secret_keys: &["username", "password", "token"],
        },
        SourceConnectorKind::Ftp => SourceConnectorValidationRules {
            required_public_keys: &["endpoint"],
            allowed_public_keys: &["endpoint", "root"],
            allowed_secret_keys: &["user", "password"],
        },
        SourceConnectorKind::ArunaNative => SourceConnectorValidationRules {
            required_public_keys: &["endpoint"],
            allowed_public_keys: &["endpoint", "realm_id", "default_node_id"],
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
    fn rejects_ftp_kind() {
        // An otherwise well-formed ftp connector must still fail registration.
        let err = validate_connector_input(
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
        .unwrap_err();

        assert_eq!(
            err,
            ValidationError::UnsupportedConnectorKind {
                kind: SourceConnectorKind::Ftp,
            }
        );
    }

    #[test]
    fn accepts_skip_signature() {
        // A public bucket needs no credentials at all.
        validate_connector_input(
            "public-s3",
            SourceConnectorKind::S3,
            &HashMap::from([
                ("bucket".to_string(), "ngi-igenomes".to_string()),
                (
                    "endpoint".to_string(),
                    "https://s3.amazonaws.com".to_string(),
                ),
                (S3_SKIP_SIGNATURE.to_string(), "true".to_string()),
            ]),
            &HashMap::new(),
        )
        .unwrap();
    }

    #[test]
    fn skip_forbids_credentials() {
        // Unsigned requests would silently ignore the stored secrets.
        let err = validate_connector_input(
            "public-s3",
            SourceConnectorKind::S3,
            &HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
                (S3_SKIP_SIGNATURE.to_string(), "true".to_string()),
            ]),
            &HashMap::from([("access_key_id".to_string(), "AKIA".to_string())]),
        )
        .unwrap_err();

        assert_eq!(err, ValidationError::CredentialsWithSkipSignature);
    }

    #[test]
    fn signed_requires_credentials() {
        // Both an absent and an explicitly disabled `skip_signature` sign requests.
        for skip in [None, Some("false")] {
            let mut public = HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
            ]);
            if let Some(skip) = skip {
                public.insert(S3_SKIP_SIGNATURE.to_string(), skip.to_string());
            }

            let err = validate_connector_input(
                "signed-s3",
                SourceConnectorKind::S3,
                &public,
                &HashMap::new(),
            )
            .unwrap_err();

            assert_eq!(err, ValidationError::MissingCredentials);
        }
    }

    #[test]
    fn requires_both_keys() {
        let err = validate_connector_input(
            "signed-s3",
            SourceConnectorKind::S3,
            &HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
            ]),
            &HashMap::from([("access_key_id".to_string(), "AKIA".to_string())]),
        )
        .unwrap_err();

        assert_eq!(err, ValidationError::MissingCredentials);
    }

    #[test]
    fn accepts_signed_s3() {
        validate_connector_input(
            "signed-s3",
            SourceConnectorKind::S3,
            &HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
            ]),
            &HashMap::from([
                ("access_key_id".to_string(), "AKIA".to_string()),
                ("secret_access_key".to_string(), "secret".to_string()),
            ]),
        )
        .unwrap();
    }

    #[test]
    fn rejects_bad_skip() {
        let err = validate_connector_input(
            "public-s3",
            SourceConnectorKind::S3,
            &HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
                (S3_SKIP_SIGNATURE.to_string(), "yes".to_string()),
            ]),
            &HashMap::new(),
        )
        .unwrap_err();

        assert_eq!(
            err,
            ValidationError::InvalidBoolValue {
                key: S3_SKIP_SIGNATURE.to_string(),
            }
        );
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
