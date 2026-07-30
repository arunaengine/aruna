use aruna_core::structs::{GroupBackendKind, ensure_confined_relative_path};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use thiserror::Error;

/// Config keys are matched against a closed allowlist of canonical spellings.
/// A denylist is unenforceable: opendal lowercases keys, accepts a wide serde
/// alias set, and silently drops what it does not know.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupBackendRules {
    pub required_public: &'static [&'static str],
    pub allowed_public: &'static [&'static str],
    pub allowed_secret: &'static [&'static str],
    /// Every one of these secrets is mandatory.
    pub required_secret: &'static [&'static str],
    /// At least one of these secrets is mandatory.
    pub one_of_secret: &'static [&'static str],
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GroupBackendError {
    #[error("backend name must not be empty")]
    EmptyName,
    #[error("config key `{key}` must not be empty")]
    EmptyValue { key: String },
    #[error("config key `{key}` is set twice")]
    DuplicateKey { key: String },
    #[error("public config key `{key}` is not allowed for backend kind `{kind}`")]
    UnknownPublicKey { kind: GroupBackendKind, key: String },
    #[error("secret config key `{key}` is not allowed for backend kind `{kind}`")]
    UnknownSecretKey { kind: GroupBackendKind, key: String },
    #[error("missing required public config key `{key}` for backend kind `{kind}`")]
    MissingPublicKey { kind: GroupBackendKind, key: String },
    #[error("backend kind `{kind}` requires secret `{key}`")]
    MissingSecret { kind: GroupBackendKind, key: String },
    #[error("backend kind `{kind}` requires one of the secrets `{keys}`")]
    MissingEitherSecret {
        kind: GroupBackendKind,
        keys: String,
    },
    #[error("endpoint `{0}` must be an https url")]
    InsecureEndpoint(String),
    #[error("root `{0}` must be a relative path without parent components")]
    UnsafeRoot(String),
    #[error("`force_path_style` must be `true` or `false`")]
    InvalidBool,
}

/// The normalized, storable configs: keys lowercased so the record matches what
/// opendal will actually read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedConfig {
    pub public: HashMap<String, String>,
    pub secret: HashMap<String, String>,
}

pub const fn rules_for_kind(kind: GroupBackendKind) -> GroupBackendRules {
    match kind {
        // `role_arn`, `session_token` and every `disable_*` toggle are absent on
        // purpose: they hand the request back to the node's ambient chain.
        GroupBackendKind::S3 => GroupBackendRules {
            required_public: &["endpoint", "bucket"],
            allowed_public: &["endpoint", "bucket", "region", "root", "force_path_style"],
            allowed_secret: &["access_key_id", "secret_access_key"],
            required_secret: &["access_key_id", "secret_access_key"],
            one_of_secret: &[],
        },
        // `credential_path` and `service_account` are absent: both resolve
        // node-local identity rather than the tenant's.
        GroupBackendKind::Gcs => GroupBackendRules {
            required_public: &["bucket"],
            allowed_public: &["bucket", "root", "endpoint"],
            allowed_secret: &["credential"],
            required_secret: &["credential"],
            one_of_secret: &[],
        },
        // Azure exposes no switch to disable ambient discovery, so a static
        // credential is the only thing keeping that chain unreachable. Opendal
        // pushes the shared-key provider only when `account_name` is set too.
        GroupBackendKind::Azblob => GroupBackendRules {
            required_public: &["endpoint", "container", "account_name"],
            allowed_public: &["endpoint", "container", "root", "account_name"],
            allowed_secret: &["account_key", "sas_token"],
            required_secret: &[],
            one_of_secret: &["account_key", "sas_token"],
        },
        // `authority_host` is absent: it redirects the OAuth token request to a
        // tenant-chosen host.
        GroupBackendKind::Azdls => GroupBackendRules {
            required_public: &["endpoint", "filesystem", "account_name"],
            allowed_public: &["endpoint", "filesystem", "root", "account_name"],
            allowed_secret: &["account_key", "sas_token"],
            required_secret: &[],
            one_of_secret: &["account_key", "sas_token"],
        },
        GroupBackendKind::B2 => GroupBackendRules {
            required_public: &["bucket", "bucket_id"],
            allowed_public: &["bucket", "bucket_id", "root"],
            allowed_secret: &["application_key_id", "application_key"],
            required_secret: &["application_key_id", "application_key"],
            one_of_secret: &[],
        },
    }
}

pub fn validate_backend_input(
    name: &str,
    kind: GroupBackendKind,
    public_config: &HashMap<String, String>,
    secret_config: &HashMap<String, String>,
) -> Result<NormalizedConfig, GroupBackendError> {
    if name.trim().is_empty() {
        return Err(GroupBackendError::EmptyName);
    }

    let rules = rules_for_kind(kind);
    let public = normalize(public_config, rules.allowed_public, |key| {
        GroupBackendError::UnknownPublicKey { kind, key }
    })?;
    let secret = normalize(secret_config, rules.allowed_secret, |key| {
        GroupBackendError::UnknownSecretKey { kind, key }
    })?;

    for key in rules.required_public {
        if !public.contains_key(*key) {
            return Err(GroupBackendError::MissingPublicKey {
                kind,
                key: (*key).to_string(),
            });
        }
    }
    for key in rules.required_secret {
        if !secret.contains_key(*key) {
            return Err(GroupBackendError::MissingSecret {
                kind,
                key: (*key).to_string(),
            });
        }
    }
    if !rules.one_of_secret.is_empty()
        && !rules
            .one_of_secret
            .iter()
            .any(|key| secret.contains_key(*key))
    {
        return Err(GroupBackendError::MissingEitherSecret {
            kind,
            keys: rules.one_of_secret.join("`, `"),
        });
    }

    if let Some(endpoint) = public.get("endpoint")
        && !endpoint.starts_with("https://")
    {
        return Err(GroupBackendError::InsecureEndpoint(endpoint.clone()));
    }
    if let Some(root) = public.get("root")
        && ensure_confined_relative_path(Path::new(root.trim_start_matches('/'))).is_err()
    {
        return Err(GroupBackendError::UnsafeRoot(root.clone()));
    }
    if let Some(value) = public.get("force_path_style")
        && value.parse::<bool>().is_err()
    {
        return Err(GroupBackendError::InvalidBool);
    }

    Ok(NormalizedConfig { public, secret })
}

/// Lowercases keys before matching, so no alias trap slips through on casing,
/// and rejects two spellings of one key rather than letting opendal pick.
fn normalize(
    config: &HashMap<String, String>,
    allowed: &'static [&'static str],
    unknown: impl Fn(String) -> GroupBackendError,
) -> Result<HashMap<String, String>, GroupBackendError> {
    let allowed: HashSet<_> = allowed.iter().copied().collect();
    let mut normalized = HashMap::with_capacity(config.len());
    for (key, value) in config {
        let key = key.trim().to_ascii_lowercase();
        if !allowed.contains(key.as_str()) {
            return Err(unknown(key));
        }
        if value.trim().is_empty() {
            return Err(GroupBackendError::EmptyValue { key });
        }
        if normalized.insert(key.clone(), value.clone()).is_some() {
            return Err(GroupBackendError::DuplicateKey { key });
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::{GroupBackendError, validate_backend_input};
    use aruna_core::structs::GroupBackendKind;
    use std::collections::HashMap;

    fn config(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn s3_public() -> HashMap<String, String> {
        config(&[("endpoint", "https://s3.example.com"), ("bucket", "data")])
    }

    fn s3_secret() -> HashMap<String, String> {
        config(&[("access_key_id", "id"), ("secret_access_key", "key")])
    }

    #[test]
    fn accepts_minimal_s3() {
        let normalized =
            validate_backend_input("tenant", GroupBackendKind::S3, &s3_public(), &s3_secret())
                .unwrap();

        assert_eq!(normalized.public, s3_public());
        assert_eq!(normalized.secret, s3_secret());
    }

    #[test]
    fn rejects_alias_traps() {
        // Every one of these is an opendal alias for an ambient-credential path.
        let traps: [(GroupBackendKind, &str, &str); 10] = [
            (GroupBackendKind::S3, "token", "t"),
            (GroupBackendKind::S3, "aws_session_token", "t"),
            (GroupBackendKind::S3, "session_token", "t"),
            (GroupBackendKind::S3, "role_arn", "arn:aws:iam::1:role/x"),
            (GroupBackendKind::S3, "skip_signature", "true"),
            (GroupBackendKind::S3, "disable_config_load", "false"),
            (
                GroupBackendKind::Gcs,
                "google_application_credentials",
                "/x",
            ),
            (GroupBackendKind::Gcs, "credential_path", "/x"),
            (GroupBackendKind::Gcs, "service_account", "a@b.iam"),
            (GroupBackendKind::Azdls, "authority_host", "https://evil"),
        ];

        for (kind, key, value) in traps {
            let mut public = kind_public(kind);
            public.insert(key.to_string(), value.to_string());
            let error =
                validate_backend_input("tenant", kind, &public, &kind_secret(kind)).unwrap_err();
            assert!(
                matches!(error, GroupBackendError::UnknownPublicKey { .. }),
                "{key} on {kind} was not rejected: {error}"
            );

            let mut secret = kind_secret(kind);
            secret.insert(key.to_string(), value.to_string());
            let error =
                validate_backend_input("tenant", kind, &kind_public(kind), &secret).unwrap_err();
            assert!(
                matches!(error, GroupBackendError::UnknownSecretKey { .. }),
                "{key} secret on {kind} was not rejected: {error}"
            );
        }
    }

    #[test]
    fn rejects_uppercase_alias() {
        // Opendal lowercases keys, so the allowlist has to as well.
        let mut public = s3_public();
        public.insert("AWS_SESSION_TOKEN".to_string(), "t".to_string());

        let error = validate_backend_input("tenant", GroupBackendKind::S3, &public, &s3_secret())
            .unwrap_err();

        assert_eq!(
            error,
            GroupBackendError::UnknownPublicKey {
                kind: GroupBackendKind::S3,
                key: "aws_session_token".to_string(),
            }
        );
    }

    #[test]
    fn requires_static_credentials() {
        // Without these the provider chain falls back to the node's identity.
        for kind in GroupBackendKind::ALL {
            let error = validate_backend_input("tenant", kind, &kind_public(kind), &HashMap::new())
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    GroupBackendError::MissingSecret { .. }
                        | GroupBackendError::MissingEitherSecret { .. }
                ),
                "{kind} accepted an empty secret config: {error}"
            );
        }
    }

    #[test]
    fn accepts_either_secret() {
        for kind in [GroupBackendKind::Azblob, GroupBackendKind::Azdls] {
            for key in ["account_key", "sas_token"] {
                validate_backend_input(
                    "tenant",
                    kind,
                    &kind_public(kind),
                    &config(&[(key, "value")]),
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn requires_account_name() {
        // Opendal derives the name only for `*.core.windows.net`-style hosts, so
        // without it a tenant endpoint gets the node's ambient token instead.
        for kind in [GroupBackendKind::Azblob, GroupBackendKind::Azdls] {
            let mut public = kind_public(kind);
            public.remove("account_name");
            public.insert(
                "endpoint".to_string(),
                "https://collector.attacker.example".to_string(),
            );

            let error =
                validate_backend_input("tenant", kind, &public, &kind_secret(kind)).unwrap_err();

            assert_eq!(
                error,
                GroupBackendError::MissingPublicKey {
                    kind,
                    key: "account_name".to_string(),
                }
            );
        }
    }

    #[test]
    fn rejects_bad_endpoint() {
        let mut public = s3_public();
        public.insert("endpoint".to_string(), "http://s3.example.com".to_string());

        let error = validate_backend_input("tenant", GroupBackendKind::S3, &public, &s3_secret())
            .unwrap_err();

        assert!(matches!(error, GroupBackendError::InsecureEndpoint(_)));
    }

    #[test]
    fn rejects_escaping_root() {
        let mut public = s3_public();
        public.insert("root".to_string(), "../elsewhere".to_string());

        let error = validate_backend_input("tenant", GroupBackendKind::S3, &public, &s3_secret())
            .unwrap_err();

        assert!(matches!(error, GroupBackendError::UnsafeRoot(_)));
    }

    #[test]
    fn rejects_empty_name() {
        let error = validate_backend_input("  ", GroupBackendKind::S3, &s3_public(), &s3_secret())
            .unwrap_err();

        assert_eq!(error, GroupBackendError::EmptyName);
    }

    fn kind_public(kind: GroupBackendKind) -> HashMap<String, String> {
        match kind {
            GroupBackendKind::S3 => s3_public(),
            GroupBackendKind::Gcs => config(&[("bucket", "data")]),
            GroupBackendKind::Azblob => config(&[
                ("endpoint", "https://acct.blob.core.windows.net"),
                ("container", "data"),
                ("account_name", "acct"),
            ]),
            GroupBackendKind::Azdls => config(&[
                ("endpoint", "https://acct.dfs.core.windows.net"),
                ("filesystem", "data"),
                ("account_name", "acct"),
            ]),
            GroupBackendKind::B2 => config(&[("bucket", "data"), ("bucket_id", "abc")]),
        }
    }

    fn kind_secret(kind: GroupBackendKind) -> HashMap<String, String> {
        match kind {
            GroupBackendKind::S3 => s3_secret(),
            GroupBackendKind::Gcs => config(&[("credential", "base64json")]),
            GroupBackendKind::Azblob | GroupBackendKind::Azdls => config(&[("account_key", "key")]),
            GroupBackendKind::B2 => {
                config(&[("application_key_id", "id"), ("application_key", "key")])
            }
        }
    }
}
