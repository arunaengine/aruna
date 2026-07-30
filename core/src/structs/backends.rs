use crate::errors::ConversionError;
use crate::structs::{
    Backend, BackendConfig, BackendRef, BlobTimeoutConfig, NodeRoutingRule, RoutingTarget,
    validate_node_rules, validate_storage_class,
};
use ipnet::IpNet;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use ulid::Ulid;

/// TOML schema of the node's backends file. Shared by the node and the doctor
/// so both read the operator's file through one parser.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendsFile {
    #[serde(default)]
    pub backend: BTreeMap<String, BackendEntry>,
    #[serde(default)]
    pub routing: Vec<RoutingEntry>,
    #[serde(default)]
    pub egress: EgressEntry,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendEntry {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub class: Option<String>,
    /// Whether tenant-authored rules may target this entry's class.
    #[serde(default = "tenants_allowed")]
    pub allow_tenants: bool,
    /// Operator allowance for user data on this backend. Stored and reported
    /// only; enforcement belongs to the quota arc.
    #[serde(default)]
    pub quota_bytes: Option<u64>,
    #[serde(default)]
    pub default: bool,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub bucket_prefix: Option<String>,
    #[serde(default)]
    pub max_bucket_size: Option<u64>,
    #[serde(default)]
    pub multipart_bucket: Option<String>,
    #[serde(default)]
    pub force_path_style: Option<bool>,
}

fn tenants_allowed() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingEntry {
    #[serde(default)]
    pub group: Option<Ulid>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub key_prefix: Option<String>,
    pub target: TargetEntry,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetEntry {
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default)]
    pub class: Option<String>,
}

/// Node-local narrowing. It can only add denies and withdraw group backends;
/// there is no syntax that widens the compiled policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EgressEntry {
    pub serve_group_backends: bool,
    pub deny: Vec<String>,
}

impl Default for EgressEntry {
    fn default() -> Self {
        Self {
            serve_group_backends: true,
            deny: Vec::new(),
        }
    }
}

/// One registered backend after the file, credentials and process-level
/// timeouts have been merged.
#[derive(Clone, Debug)]
pub struct NodeBackendEntry {
    pub name: String,
    pub config: BackendConfig,
    pub class: Option<String>,
    pub allow_tenants: bool,
    pub quota_bytes: Option<u64>,
}

/// The whole storage configuration of one node.
#[derive(Clone, Debug)]
pub struct NodeBackendsConfig {
    pub backends: Vec<NodeBackendEntry>,
    pub default_name: String,
    pub rules: Vec<NodeRoutingRule>,
    pub serve_group_backends: bool,
    pub extra_deny: Vec<IpNet>,
}

impl NodeBackendsConfig {
    /// The zero-config shape used when no backends file is present.
    pub fn single(config: BackendConfig) -> Self {
        Self {
            backends: vec![NodeBackendEntry {
                name: BackendRef::DEFAULT_NODE_NAME.to_string(),
                config,
                class: None,
                allow_tenants: true,
                quota_bytes: None,
            }],
            default_name: BackendRef::DEFAULT_NODE_NAME.to_string(),
            rules: Vec::new(),
            serve_group_backends: true,
            extra_deny: Vec::new(),
        }
    }
}

/// Looks up the credentials of one backend. Credentials never live in the file,
/// so the caller supplies them from the environment or, later, a vault.
pub type CredentialLookup<'a> = &'a dyn Fn(&str) -> Option<(String, String)>;

impl BackendsFile {
    pub fn parse(text: &str) -> Result<Self, ConversionError> {
        toml::from_str(text).map_err(|error| ConversionError::FromStrError(error.to_string()))
    }

    /// Merges file, credentials and timeouts into the node's storage config.
    /// Rejects a missing or ambiguous default, unknown backend types, invalid
    /// classes and rules naming nothing.
    pub fn resolve(
        &self,
        credentials: CredentialLookup<'_>,
        timeouts: BlobTimeoutConfig,
    ) -> Result<NodeBackendsConfig, ConversionError> {
        if self.backend.is_empty() {
            return Err(ConversionError::FromStrError(
                "backends file declares no backend".to_string(),
            ));
        }
        let defaults: Vec<&String> = self
            .backend
            .iter()
            .filter(|(_, entry)| entry.default)
            .map(|(name, _)| name)
            .collect();
        let [default_name] = defaults.as_slice() else {
            return Err(ConversionError::FromStrError(format!(
                "backends file must mark exactly one backend as default, found {}",
                defaults.len()
            )));
        };
        let default_name = (*default_name).clone();

        let mut backends = Vec::new();
        let mut classes = BTreeMap::new();
        for (name, entry) in &self.backend {
            if let Some(class) = entry.class.as_deref() {
                validate_storage_class(class)
                    .map_err(|error| ConversionError::FromStrError(error.to_string()))?;
                if classes.insert(class.to_string(), name.clone()).is_some() {
                    return Err(ConversionError::FromStrError(format!(
                        "storage class `{class}` is claimed by more than one backend"
                    )));
                }
            }
            if entry.quota_bytes == Some(0) {
                return Err(ConversionError::FromStrError(format!(
                    "backend `{name}` sets quota_bytes = 0, which allows no user data"
                )));
            }
            backends.push(NodeBackendEntry {
                name: name.clone(),
                config: entry.to_config(credentials(name), timeouts)?,
                class: entry.class.clone(),
                allow_tenants: entry.allow_tenants,
                quota_bytes: entry.quota_bytes,
            });
        }

        let rules = self
            .routing
            .iter()
            .map(|entry| entry.to_rule(&self.backend, &classes))
            .collect::<Result<Vec<_>, _>>()?;
        validate_node_rules(&rules)
            .map_err(|error| ConversionError::FromStrError(error.to_string()))?;

        let extra_deny = self
            .egress
            .deny
            .iter()
            .map(|net| {
                IpNet::from_str(net)
                    .map_err(|error| ConversionError::FromStrError(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(NodeBackendsConfig {
            backends,
            default_name,
            rules,
            serve_group_backends: self.egress.serve_group_backends,
            extra_deny,
        })
    }
}

impl BackendEntry {
    pub fn to_config(
        &self,
        credentials: Option<(String, String)>,
        timeouts: BlobTimeoutConfig,
    ) -> Result<BackendConfig, ConversionError> {
        let backend_type = Backend::from_str(&self.kind)?;
        let mut service_config = HashMap::new();
        if backend_type == Backend::S3 {
            let endpoint = self.endpoint.as_ref().ok_or_else(|| {
                ConversionError::FromStrError("s3 backend requires an endpoint".to_string())
            })?;
            service_config.insert("endpoint".to_string(), endpoint.clone());
            let (access_key_id, secret_access_key) = credentials.ok_or_else(|| {
                ConversionError::FromStrError(
                    "s3 backend requires credentials in the environment".to_string(),
                )
            })?;
            service_config.insert("access_key_id".to_string(), access_key_id);
            service_config.insert("secret_access_key".to_string(), secret_access_key);
            if let Some(region) = self.region.as_ref() {
                service_config.insert("region".to_string(), region.clone());
            }
            if let Some(bucket) = self
                .bucket
                .as_ref()
                .filter(|value| !value.trim().is_empty())
            {
                service_config.insert("bucket".to_string(), bucket.trim().to_string());
            }
            service_config.insert(
                "force_path_style".to_string(),
                self.force_path_style.unwrap_or(true).to_string(),
            );
        }

        Ok(BackendConfig {
            backend_type,
            root: self.root.clone().unwrap_or_default(),
            service_config,
            bucket_prefix: self.bucket_prefix.clone(),
            max_bucket_size: self.max_bucket_size,
            multipart_bucket: self.multipart_bucket.clone(),
            timeouts,
        })
    }
}

impl RoutingEntry {
    fn to_rule(
        &self,
        backends: &BTreeMap<String, BackendEntry>,
        classes: &BTreeMap<String, String>,
    ) -> Result<NodeRoutingRule, ConversionError> {
        let target = match (&self.target.backend, &self.target.class) {
            (Some(name), None) => {
                if !backends.contains_key(name) {
                    return Err(ConversionError::FromStrError(format!(
                        "routing rule names unregistered backend `{name}`"
                    )));
                }
                RoutingTarget::Backend(BackendRef::Node(name.clone()))
            }
            (None, Some(class)) => {
                validate_storage_class(class)
                    .map_err(|error| ConversionError::FromStrError(error.to_string()))?;
                if !classes.contains_key(class) {
                    return Err(ConversionError::FromStrError(format!(
                        "routing rule names unregistered storage class `{class}`"
                    )));
                }
                RoutingTarget::Class(class.clone())
            }
            _ => {
                return Err(ConversionError::FromStrError(
                    "routing target must name exactly one of backend or class".to_string(),
                ));
            }
        };
        Ok(NodeRoutingRule {
            group: self.group,
            bucket: self.bucket.clone(),
            key_prefix: self.key_prefix.clone(),
            target,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BackendsFile;
    use crate::structs::{BackendRef, BlobTimeoutConfig, RoutingTarget};

    const FILE: &str = r#"
[backend.default]
type = "filesystem"
root = "/data/blob"
default = true

[backend.cold]
type = "s3"
class = "cold"
allow_tenants = false
quota_bytes = 10995116277760
root = ""
endpoint = "https://s3.example.org"
region = "eu-central-1"
bucket_prefix = "aruna-cold-"
max_bucket_size = 100000
multipart_bucket = "cold-parts"
force_path_style = true

[[routing]]
bucket = "raw-archive"
target = { class = "cold" }

[[routing]]
key_prefix = "archive/"
target = { backend = "cold" }

[egress]
serve_group_backends = false
deny = ["203.0.113.0/24"]
"#;

    fn secrets(_: &str) -> Option<(String, String)> {
        Some(("key".to_string(), "secret".to_string()))
    }

    #[test]
    fn resolves_full_file() {
        let parsed = BackendsFile::parse(FILE).unwrap();

        let config = parsed
            .resolve(&secrets, BlobTimeoutConfig::default())
            .unwrap();

        assert_eq!(config.default_name, "default");
        assert_eq!(config.backends.len(), 2);
        let cold = config
            .backends
            .iter()
            .find(|entry| entry.name == "cold")
            .unwrap();
        assert_eq!(cold.class.as_deref(), Some("cold"));
        assert!(!cold.allow_tenants);
        assert_eq!(cold.quota_bytes, Some(10_995_116_277_760));
        assert!(
            config
                .backends
                .iter()
                .find(|entry| entry.name == "default")
                .is_some_and(|entry| entry.allow_tenants && entry.quota_bytes.is_none())
        );
        assert_eq!(
            cold.config
                .service_config
                .get("access_key_id")
                .map(String::as_str),
            Some("key")
        );
        assert_eq!(config.rules.len(), 2);
        assert_eq!(
            config.rules[0].target,
            RoutingTarget::Class("cold".to_string())
        );
        assert_eq!(
            config.rules[1].target,
            RoutingTarget::Backend(BackendRef::Node("cold".to_string()))
        );
        assert!(!config.serve_group_backends);
        assert_eq!(config.extra_deny.len(), 1);
    }

    #[test]
    fn rejects_two_defaults() {
        let file = BackendsFile::parse(
            r#"
[backend.a]
type = "filesystem"
default = true
[backend.b]
type = "filesystem"
default = true
"#,
        )
        .unwrap();

        assert!(
            file.resolve(&secrets, BlobTimeoutConfig::default())
                .is_err()
        );
    }

    #[test]
    fn rejects_no_default() {
        let file = BackendsFile::parse(
            r#"
[backend.a]
type = "filesystem"
"#,
        )
        .unwrap();

        assert!(
            file.resolve(&secrets, BlobTimeoutConfig::default())
                .is_err()
        );
    }

    #[test]
    fn rejects_duplicate_class() {
        let file = BackendsFile::parse(
            r#"
[backend.a]
type = "filesystem"
class = "cold"
default = true
[backend.b]
type = "filesystem"
class = "cold"
"#,
        )
        .unwrap();

        assert!(
            file.resolve(&secrets, BlobTimeoutConfig::default())
                .is_err()
        );
    }

    #[test]
    fn rejects_unknown_target() {
        let file = BackendsFile::parse(
            r#"
[backend.a]
type = "filesystem"
default = true

[[routing]]
target = { class = "glacier" }
"#,
        )
        .unwrap();

        assert!(
            file.resolve(&secrets, BlobTimeoutConfig::default())
                .is_err()
        );
    }

    #[test]
    fn rejects_tied_rules() {
        // Both rules match the same write with the same specificity, so file
        // order would silently decide where the bytes land.
        let file = BackendsFile::parse(
            r#"
[backend.a]
type = "filesystem"
default = true

[[routing]]
bucket = "raw"
target = { backend = "a" }

[[routing]]
group = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
target = { backend = "a" }
"#,
        )
        .unwrap();

        let error = file
            .resolve(&secrets, BlobTimeoutConfig::default())
            .unwrap_err()
            .to_string();

        assert!(error.contains("bucket=raw"), "{error}");
        assert!(
            error.contains("group=01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            "{error}"
        );
    }

    #[test]
    fn rejects_missing_secrets() {
        let file = BackendsFile::parse(
            r#"
[backend.a]
type = "s3"
endpoint = "https://s3.example.org"
default = true
"#,
        )
        .unwrap();

        assert!(
            file.resolve(&|_| None, BlobTimeoutConfig::default())
                .is_err()
        );
    }

    #[test]
    fn rejects_zero_quota() {
        let file = BackendsFile::parse(
            r#"
[backend.a]
type = "filesystem"
default = true
quota_bytes = 0
"#,
        )
        .unwrap();

        assert!(
            file.resolve(&secrets, BlobTimeoutConfig::default())
                .is_err()
        );
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(
            BackendsFile::parse(
                r#"
[backend.a]
type = "filesystem"
secret_access_key = "leaked"
"#
            )
            .is_err()
        );
    }
}
