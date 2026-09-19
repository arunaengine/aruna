//! Parses operator settings from one explicit environment source without any other input.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use aruna_api::s3::server::S3ServerTimeouts;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::OidcProviderConfig;
use aruna_core::structs::storage::backends::{BackendsFile, NodeBackendsConfig};
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BlobTimeoutConfig};
use aruna_core::structs::storage::node_info::{
    CLASS_LABEL_PREFIX, KIND_LABEL_KEY, LOCATION_LABEL_KEY,
};
use aruna_net::IrohRuntimeConfig;
use aruna_operations::metadata::MetadataSearchStorage;
use aruna_storage::FjallPersistPolicy;

use crate::config::{PortalArtifactConfig, PortalConfig, RateLimitSettings, SetupError};

const OPS_SOCKET_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3002);
const BOOTSTRAP_TIMEOUT_SECS: u64 = 120;
const SYNC_TIMEOUT_SECS: u64 = 60;

/// The operator-input source. Production reads the process environment (with
/// any `.env` already loaded into it); tests pass an explicit map.
pub trait SettingsEnv {
    fn var(&self, key: &str) -> Option<String>;
}

pub struct ProcessEnv;

impl SettingsEnv for ProcessEnv {
    fn var(&self, key: &str) -> Option<String> {
        dotenvy::var(key).ok()
    }
}

impl SettingsEnv for BTreeMap<String, String> {
    fn var(&self, key: &str) -> Option<String> {
        self.get(key).cloned()
    }
}

impl SettingsEnv for HashMap<String, String> {
    fn var(&self, key: &str) -> Option<String> {
        self.get(key).cloned()
    }
}

/// Environment-derived configuration. Reading it performs no network or storage
/// I/O and touches no global state, so a test can parse an explicit map.
pub struct Settings {
    pub storage_path: String,
    pub metadata_storage_path: String,
    pub metadata_search_storage: MetadataSearchStorage,
    pub fjall_persist_policy: FjallPersistPolicy,
    pub sync_storage_path: PathBuf,
    pub blob_root: String,
    pub blob_backends: NodeBackendsConfig,
    pub blob_bucket_prefix: Option<String>,
    pub blob_bucket_size: Option<u64>,
    pub blob_multipart_bucket: Option<String>,
    pub connect_timeout_secs: u64,
    pub io_timeout_secs: u64,
    pub transfer_idle_secs: u64,
    pub bootstrap_timeout_secs: u64,
    pub sync_timeout_secs: u64,
    pub request_timeout_secs: u64,
    pub connection_idle_secs: u64,
    pub stream_lifetime_secs: u64,
    pub http_socket_addr: SocketAddr,
    pub ops_socket_addr: SocketAddr,
    pub max_body_size: usize,
    pub cors_allowed_origins: Vec<String>,
    pub desktop_cors: bool,
    pub mcp_enabled: bool,
    pub portal_csp_origins: Vec<String>,
    pub p2p_addr: SocketAddr,
    pub additional_relay_urls: Vec<String>,
    pub max_uni_streams: Option<u64>,
    pub max_bidi_streams: Option<u64>,
    pub document_sync_runtime: IrohRuntimeConfig,
    pub metadata_replication_factor: u32,
    /// `None` when `S3_HOST`/`S3_ADDRESS` are unset or empty: no S3 listener.
    pub s3_host: Option<String>,
    pub api_public_url: Option<String>,
    pub s3_public_url: Option<String>,
    pub trusted_proxies: Vec<ipnet::IpNet>,
    pub rocrate_limits: RoCrateLimits,
    pub rate_limits: RateLimitSettings,
    pub s3_address: Option<String>,
    pub onboarding_secret: Option<String>,
    pub oidc_providers: Vec<OidcProviderConfig>,
    pub realm_description: String,
    pub portal: PortalConfig,
    pub assistant_proxy: bool,
    pub node_labels: BTreeMap<String, String>,
    pub node_location: Option<String>,
    pub node_weight: Option<u32>,
}

impl std::fmt::Debug for Settings {
    /// Redacts the onboarding secret: parsed settings may be printed in an
    /// error path, and the secret is not diagnostic output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("storage_path", &self.storage_path)
            .field("http_socket_addr", &self.http_socket_addr)
            .field("onboarding_secret", &self.onboarding_secret.is_some())
            .finish_non_exhaustive()
    }
}

/// Parses the process environment.
pub fn read_settings() -> Result<Settings, SetupError> {
    read_settings_from(&ProcessEnv)
}

/// Parses one explicit operator-input source.
pub fn read_settings_from(env: &dyn SettingsEnv) -> Result<Settings, SetupError> {
    let storage_path = required_var(env, "STORAGE_PATH")?;
    let metadata_storage_path = env
        .var("CRAQLE_STORAGE_PATH")
        .unwrap_or_else(|| format!("{storage_path}/craqle"));
    let metadata_search_storage = search_storage_env(env)?;
    let fjall_persist_policy = persist_policy_env(env)?;
    let sync_storage_path = env
        .var("DOCUMENT_SYNC_STORAGE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("{storage_path}/document-sync")));
    let document_sync_runtime = load_sync_config(env)?;
    let blob_root = env
        .var("BLOB_ROOT")
        .unwrap_or_else(|| format!("{storage_path}/blobstore"));
    let blob_bucket_prefix = env.var("BLOB_BUCKET_PREFIX");

    let max_uni_streams = env
        .var("MAX_CONCURRENT_UNI_STREAMS")
        .map(|value| value.parse::<u64>())
        .transpose()?;
    let max_bidi_streams = env
        .var("MAX_CONCURRENT_BIDI_STREAMS")
        .map(|value| value.parse::<u64>())
        .transpose()?;

    let blob_bucket_size = env
        .var("BLOB_MAX_BUCKET_SIZE")
        .map(|value| value.parse::<u64>())
        .transpose()?
        .or(Some(100_000));
    let blob_multipart_bucket = env
        .var("BLOB_MULTIPART_BUCKET")
        .filter(|value| !value.trim().is_empty())
        .or(Some("uploaded-parts".to_string()));
    let connect_timeout_secs = env
        .var("BLOB_CONTROL_PLANE_CONNECT_TIMEOUT_SECS")
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(30);
    let io_timeout_secs = env
        .var("BLOB_CONTROL_PLANE_IO_TIMEOUT_SECS")
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(30);
    let transfer_idle_secs = env
        .var("BLOB_TRANSFER_IDLE_TIMEOUT_SECS")
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(30 * 60);
    let bootstrap_timeout_secs = positive_u64_env(
        env,
        "ONBOARDING_BOOTSTRAP_TIMEOUT_SECS",
        BOOTSTRAP_TIMEOUT_SECS,
    )?;
    let sync_timeout_secs = positive_u64_env(
        env,
        "ONBOARDING_DOCUMENT_SYNC_TIMEOUT_SECS",
        SYNC_TIMEOUT_SECS,
    )?;
    let s3_timeouts = S3ServerTimeouts::default();
    let request_timeout_secs = positive_u64_env(
        env,
        "S3_INITIAL_REQUEST_TIMEOUT_SECS",
        s3_timeouts.initial_request.as_secs(),
    )?;
    let connection_idle_secs = positive_u64_env(
        env,
        "S3_CONNECTION_IDLE_TIMEOUT_SECS",
        s3_timeouts.connection_idle.as_secs(),
    )?;
    let stream_lifetime_secs = positive_u64_env(
        env,
        "S3_STREAM_LIFETIME_TIMEOUT_SECS",
        s3_timeouts.stream_lifetime.as_secs(),
    )?;
    let blob_timeouts = BlobTimeoutConfig {
        control_connect_timeout: Duration::from_secs(connect_timeout_secs),
        control_io_timeout: Duration::from_secs(io_timeout_secs),
        transfer_idle_timeout: Duration::from_secs(transfer_idle_secs),
    };
    let blob_backends = load_backends_config(
        env,
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root.clone(),
            service_config: HashMap::new(),
            bucket_prefix: blob_bucket_prefix.clone(),
            max_bucket_size: blob_bucket_size,
            multipart_bucket: blob_multipart_bucket.clone(),
            timeouts: blob_timeouts,
        },
        blob_timeouts,
    )?;
    let http_socket_addr = SocketAddr::from_str(&required_var(env, "SOCKET_ADDRESS")?)?;
    let ops_socket_addr = env
        .var("OPS_SOCKET_ADDRESS")
        .filter(|value| !value.trim().is_empty())
        .map(|value| SocketAddr::from_str(value.trim()))
        .transpose()?
        .unwrap_or(OPS_SOCKET_ADDR);
    let max_body_size = env
        .var("MAX_HTTP_BODY_SIZE")
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(aruna_api::server::MAX_BODY_SIZE);
    let cors_allowed_origins = parse_list_env(env, "CORS_ALLOWED_ORIGINS");
    let desktop_cors = desktop_cors_env(env);
    let mcp_enabled = mcp_env(env)?;
    let portal_csp_origins = parse_list_env(env, "PORTAL_CSP_EXTRA_ORIGINS");
    let p2p_addr = SocketAddr::from_str(
        &env.var("P2P_SOCKET_ADDRESS")
            .unwrap_or_else(|| http_socket_addr.to_string()),
    )?;
    let additional_relay_urls = parse_list_env(env, "P2P_ADDITIONAL_RELAY_URLS");
    validate_relay_urls("P2P_ADDITIONAL_RELAY_URLS", &additional_relay_urls)?;
    let metadata_replication_factor = env
        .var("METADATA_REPLICATION_FACTOR")
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(3)
        .max(1);
    let api_public_url = optional_public_url(env, "API_PUBLIC_URL")?;
    let s3_public_url = optional_public_url(env, "S3_PUBLIC_URL")?;
    let trusted_proxies = trusted_proxies_env(env)?;
    let rocrate_limits = rocrate_limits_env(env)?;
    let rate_limits = rate_limits_env(env)?;
    // Devices may omit both S3 values, but supplying only one is always invalid.
    // The node identity later decides whether the listener is required.
    let (s3_host, s3_address) = match (
        optional_nonempty_env(env, "S3_HOST")?,
        optional_nonempty_env(env, "S3_ADDRESS")?,
    ) {
        (Some(host), Some(address)) => {
            SocketAddr::from_str(&address)?;
            (Some(host), Some(address))
        }
        (None, None) => (None, None),
        (Some(_), None) => return Err(SetupError::MissingConfigValue("S3_ADDRESS")),
        (None, Some(_)) => return Err(SetupError::MissingConfigValue("S3_HOST")),
    };
    let node_labels = parse_node_labels(env)?;
    let node_location = env
        .var("ARUNA_NODE_LOCATION")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let node_weight = env
        .var("ARUNA_NODE_WEIGHT")
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().parse::<u32>())
        .transpose()?;
    let realm_description = env
        .var("REALM_DESCRIPTION")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Aruna Realm".to_string());
    let onboarding_secret = env
        .var("ONBOARDING_SECRET")
        .filter(|value| !value.trim().is_empty());
    let oidc_providers = load_oidc_providers(env)?;
    let portal = portal_config_env(env)?;
    let assistant_proxy = assistant_proxy_env(env)?;
    // The SPA is served from another origin than the API, so it cannot reach a
    // relative API path and needs the advertised one.
    if matches!(portal, PortalConfig::Artifact { .. }) && api_public_url.is_none() {
        return Err(SetupError::MissingConfigValue("API_PUBLIC_URL"));
    }

    Ok(Settings {
        storage_path,
        metadata_storage_path,
        metadata_search_storage,
        fjall_persist_policy,
        sync_storage_path,
        blob_root,
        blob_backends,
        blob_bucket_prefix,
        blob_bucket_size,
        blob_multipart_bucket,
        connect_timeout_secs,
        io_timeout_secs,
        transfer_idle_secs,
        bootstrap_timeout_secs,
        sync_timeout_secs,
        request_timeout_secs,
        connection_idle_secs,
        stream_lifetime_secs,
        http_socket_addr,
        ops_socket_addr,
        max_body_size,
        cors_allowed_origins,
        desktop_cors,
        mcp_enabled,
        portal_csp_origins,
        p2p_addr,
        additional_relay_urls,
        max_uni_streams,
        max_bidi_streams,
        document_sync_runtime,
        metadata_replication_factor,
        s3_host,
        api_public_url,
        s3_public_url,
        trusted_proxies,
        rocrate_limits,
        rate_limits,
        s3_address,
        onboarding_secret,
        oidc_providers,
        realm_description,
        portal,
        assistant_proxy,
        node_labels,
        node_location,
        node_weight,
    })
}

fn required_var(env: &dyn SettingsEnv, key: &'static str) -> Result<String, SetupError> {
    env.var(key).ok_or(SetupError::MissingConfigValue(key))
}

/// Environment token a backend name maps to. The mapping is lossy, so callers
/// must reject two names that share a token before reading any credential.
fn credential_token(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Per-backend S3 credentials. The backends file never holds secrets; this is
/// the one lookup a node-local vault will replace later.
fn backend_credentials(env: &dyn SettingsEnv, name: &str) -> Option<(String, String)> {
    let upper = credential_token(name);
    let key = env.var(&format!("BLOB_BACKEND_{upper}_ACCESS_KEY_ID"))?;
    let secret = env.var(&format!("BLOB_BACKEND_{upper}_SECRET_ACCESS_KEY"))?;
    Some((key, secret))
}

/// Refuses a file where two backends would read the same credential variables,
/// which would let one backend authenticate with another's account.
fn reject_token_clashes(names: impl Iterator<Item = String>) -> Result<(), SetupError> {
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for name in names {
        let token = credential_token(&name);
        if let Some(other) = seen.insert(token.clone(), name.clone()) {
            return Err(invalid_config_value(
                "BLOB_BACKENDS_PATH",
                format!("{other}, {name}"),
                format!("both backends resolve to BLOB_BACKEND_{token}_* credentials"),
            ));
        }
    }
    Ok(())
}

/// Reads the operator's backends file, or synthesises the implicit single
/// filesystem backend so a zero-config node keeps working.
fn load_backends_config(
    env: &dyn SettingsEnv,
    implicit: BackendConfig,
    timeouts: BlobTimeoutConfig,
) -> Result<NodeBackendsConfig, SetupError> {
    let Some(path) = env.var("BLOB_BACKENDS_PATH") else {
        return Ok(NodeBackendsConfig::single(implicit));
    };
    let text = std::fs::read_to_string(&path)?;
    let file = BackendsFile::parse(&text)?;
    reject_token_clashes(file.backend.keys().cloned())?;
    Ok(file.resolve(&|name| backend_credentials(env, name), timeouts)?)
}

pub(crate) fn normalize_env_value(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}

pub(crate) fn invalid_config_value(
    key: &'static str,
    value: impl Into<String>,
    message: impl std::fmt::Display,
) -> SetupError {
    SetupError::InvalidConfigValue {
        key,
        value: value.into(),
        message: message.to_string(),
    }
}

fn search_storage_env(env: &dyn SettingsEnv) -> Result<MetadataSearchStorage, SetupError> {
    const KEY: &str = "CRAQLE_SEARCH_STORAGE";
    let Some(value) = env.var(KEY) else {
        return Ok(MetadataSearchStorage::Disk);
    };

    match normalize_env_value(&value).as_str() {
        "disk" => Ok(MetadataSearchStorage::Disk),
        "memory" | "in_memory" | "ram" => Ok(MetadataSearchStorage::Memory),
        _ => Err(invalid_config_value(
            KEY,
            value,
            "expected one of: disk, memory",
        )),
    }
}

fn persist_policy_env(env: &dyn SettingsEnv) -> Result<FjallPersistPolicy, SetupError> {
    const KEY: &str = "ARUNA_FJALL_PERSIST_MODE";
    let Some(value) = env.var(KEY) else {
        return Ok(FjallPersistPolicy::default());
    };

    value
        .parse::<FjallPersistPolicy>()
        .map_err(|message| invalid_config_value(KEY, value, message))
}

fn positive_u64_env(
    env: &dyn SettingsEnv,
    key: &'static str,
    default: u64,
) -> Result<u64, SetupError> {
    let Some(value) = optional_nonempty_env(env, key)? else {
        return Ok(default);
    };
    let parsed = value
        .parse::<u64>()
        .map_err(|error| invalid_config_value(key, &value, error))?;
    if parsed == 0 {
        return Err(invalid_config_value(
            key,
            value,
            "must be greater than zero",
        ));
    }
    Ok(parsed)
}

fn positive_u32_env(
    env: &dyn SettingsEnv,
    key: &'static str,
    default: u32,
) -> Result<u32, SetupError> {
    let Some(value) = optional_nonempty_env(env, key)? else {
        return Ok(default);
    };
    let parsed = value
        .parse::<u32>()
        .map_err(|error| invalid_config_value(key, &value, error))?;
    if parsed == 0 {
        return Err(invalid_config_value(
            key,
            value,
            "must be greater than zero",
        ));
    }
    Ok(parsed)
}

fn rocrate_limits_env(env: &dyn SettingsEnv) -> Result<RoCrateLimits, SetupError> {
    let defaults = RoCrateLimits::default();
    let limits = RoCrateLimits {
        direct_upload_bytes: positive_u64_env(
            env,
            "ROCRATE_DIRECT_UPLOAD_BYTES",
            defaults.direct_upload_bytes,
        )?,
        import_source_bytes: positive_u64_env(
            env,
            "ROCRATE_IMPORT_SOURCE_BYTES",
            defaults.import_source_bytes,
        )?,
        expanded_import_bytes: positive_u64_env(
            env,
            "ROCRATE_EXPANDED_IMPORT_BYTES",
            defaults.expanded_import_bytes,
        )?,
        export_artifact_bytes: positive_u64_env(
            env,
            "ROCRATE_EXPORT_ARTIFACT_BYTES",
            defaults.export_artifact_bytes,
        )?,
        max_entries: positive_u64_env(env, "ROCRATE_MAX_ENTRIES", defaults.max_entries)?,
        metadata_bytes: positive_u64_env(env, "ROCRATE_METADATA_BYTES", defaults.metadata_bytes)?,
        key_bytes: positive_u64_env(env, "ROCRATE_KEY_BYTES", defaults.key_bytes)?,
        upload_retention_ms: positive_u64_env(
            env,
            "ROCRATE_UPLOAD_RETENTION_MS",
            defaults.upload_retention_ms,
        )?,
        artifact_retention_ms: positive_u64_env(
            env,
            "ROCRATE_JOB_RETENTION_MS",
            defaults.artifact_retention_ms,
        )?,
        max_active_jobs: positive_u32_env(
            env,
            "ROCRATE_MAX_ACTIVE_JOBS",
            defaults.max_active_jobs,
        )?,
        holder_ttl_ms: positive_u64_env(env, "ROCRATE_HOLDER_TTL_MS", defaults.holder_ttl_ms)?,
        holder_refresh_ms: positive_u64_env(
            env,
            "ROCRATE_HOLDER_REFRESH_MS",
            defaults.holder_refresh_ms,
        )?,
    };
    if limits.holder_refresh_ms >= limits.holder_ttl_ms {
        return Err(invalid_config_value(
            "ROCRATE_HOLDER_REFRESH_MS",
            limits.holder_refresh_ms.to_string(),
            "must be less than ROCRATE_HOLDER_TTL_MS",
        ));
    }
    Ok(limits)
}

fn rate_limits_env(env: &dyn SettingsEnv) -> Result<RateLimitSettings, SetupError> {
    let defaults = RateLimitSettings::default();
    Ok(RateLimitSettings {
        ip_per_minute: positive_u32_env(env, "RATE_LIMIT_IP_PER_MINUTE", defaults.ip_per_minute)?,
        ip_burst: positive_u32_env(env, "RATE_LIMIT_IP_BURST", defaults.ip_burst)?,
        principal_per_minute: positive_u32_env(
            env,
            "RATE_LIMIT_PRINCIPAL_PER_MINUTE",
            defaults.principal_per_minute,
        )?,
        principal_burst: positive_u32_env(
            env,
            "RATE_LIMIT_PRINCIPAL_BURST",
            defaults.principal_burst,
        )?,
        s3_max_connections: positive_u32_env(
            env,
            "S3_MAX_CONNECTIONS",
            defaults.s3_max_connections,
        )?,
        s3_max_requests: positive_u32_env(
            env,
            "S3_MAX_CONCURRENT_REQUESTS",
            defaults.s3_max_requests,
        )?,
    })
}

fn portal_config_env(env: &dyn SettingsEnv) -> Result<PortalConfig, SetupError> {
    const MODE_KEY: &str = "PORTAL_MODE";
    let Some(mode) = env.var(MODE_KEY) else {
        return Ok(PortalConfig::Disabled);
    };

    match normalize_env_value(&mode).as_str() {
        "disabled" => Ok(PortalConfig::Disabled),
        "artifact" => {
            let artifact_url = optional_nonempty_env(env, "PORTAL_ARTIFACT_URL")?;
            if let Some(artifact_url) = &artifact_url {
                reqwest::Url::parse(artifact_url).map_err(|error| {
                    invalid_config_value("PORTAL_ARTIFACT_URL", artifact_url, error)
                })?;
            }
            let artifact_sha256 = optional_nonempty_env(env, "PORTAL_ARTIFACT_SHA256")?
                .map(|value| normalize_sha256_env("PORTAL_ARTIFACT_SHA256", &value))
                .transpose()?;
            let portal_dir = PathBuf::from(required_nonempty_env(env, "PORTAL_DIR")?);
            const ADDRESS_KEY: &str = "PORTAL_SOCKET_ADDRESS";
            let address = required_nonempty_env(env, ADDRESS_KEY)?;
            let socket_addr = SocketAddr::from_str(address.trim())
                .map_err(|error| invalid_config_value(ADDRESS_KEY, address, error))?;
            Ok(PortalConfig::Artifact {
                artifact: PortalArtifactConfig {
                    artifact_url,
                    artifact_sha256,
                    portal_dir,
                },
                socket_addr,
            })
        }
        _ => Err(invalid_config_value(
            MODE_KEY,
            mode,
            "expected one of: disabled, artifact",
        )),
    }
}

fn optional_nonempty_env(
    env: &dyn SettingsEnv,
    key: &'static str,
) -> Result<Option<String>, SetupError> {
    match env.var(key) {
        Some(value) if value.trim().is_empty() => Ok(None),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

fn required_nonempty_env(env: &dyn SettingsEnv, key: &'static str) -> Result<String, SetupError> {
    match env.var(key) {
        Some(value) if !value.trim().is_empty() => Ok(value),
        Some(value) => Err(invalid_config_value(key, value, "must not be empty")),
        None => Err(SetupError::MissingConfigValue(key)),
    }
}

/// `TRUSTED_PROXIES` is a comma-separated list of CIDR networks or bare IPs
/// whose `x-forwarded-*` headers the node honors. Empty trusts no proxy.
fn trusted_proxies_env(env: &dyn SettingsEnv) -> Result<Vec<ipnet::IpNet>, SetupError> {
    let Some(value) = optional_nonempty_env(env, "TRUSTED_PROXIES")? else {
        return Ok(Vec::new());
    };
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .parse::<ipnet::IpNet>()
                .or_else(|_| entry.parse::<std::net::IpAddr>().map(ipnet::IpNet::from))
                .map_err(|error| invalid_config_value("TRUSTED_PROXIES", entry, error))
        })
        .collect()
}

fn optional_public_url(
    env: &dyn SettingsEnv,
    key: &'static str,
) -> Result<Option<String>, SetupError> {
    let value = optional_nonempty_env(env, key)?;
    if let Some(url) = &value {
        validate_public_url(key, url)?;
    }
    Ok(value)
}

pub(crate) fn validate_public_url(key: &'static str, value: &str) -> Result<(), SetupError> {
    let url =
        reqwest::Url::parse(value).map_err(|error| invalid_config_value(key, value, error))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(invalid_config_value(
            key,
            value,
            "expected an absolute HTTP or HTTPS URL with a host",
        ));
    }
    Ok(())
}

fn normalize_sha256_env(key: &'static str, value: &str) -> Result<String, SetupError> {
    let trimmed = value.trim();
    if trimmed.len() != 64 || !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(invalid_config_value(
            key,
            value,
            "expected 64 hex characters",
        ));
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn parse_list_env(env: &dyn SettingsEnv, key: &str) -> Vec<String> {
    env.var(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// `DESKTOP_CORS` opts out of the always-on desktop origin admission.
fn desktop_cors_env(env: &dyn SettingsEnv) -> bool {
    !matches!(
        env.var("DESKTOP_CORS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "false" | "0" | "no"
    )
}

fn assistant_proxy_env(env: &dyn SettingsEnv) -> Result<bool, SetupError> {
    let Some(value) = optional_nonempty_env(env, "ASSISTANT_PROXY")? else {
        return Ok(true);
    };
    match normalize_env_value(&value).as_str() {
        "enabled" => Ok(true),
        "disabled" => Ok(false),
        _ => Err(invalid_config_value(
            "ASSISTANT_PROXY",
            value,
            "expected enabled or disabled",
        )),
    }
}

fn mcp_env(env: &dyn SettingsEnv) -> Result<bool, SetupError> {
    const KEY: &str = "MCP";
    match env.var(KEY).as_deref().map(normalize_env_value) {
        None => Ok(true),
        Some(value) if value == "enabled" => Ok(true),
        Some(value) if value == "disabled" => Ok(false),
        Some(value) => Err(invalid_config_value(
            KEY,
            value,
            "expected one of: enabled, disabled",
        )),
    }
}

/// Parses the placement-map initialization/onboarding input `ARUNA_NODE_LABELS`
/// in `k=v,k2=v2` form. Rejects malformed pairs and every derived-only label,
/// which the owning node stamps for itself.
fn parse_node_labels(env: &dyn SettingsEnv) -> Result<BTreeMap<String, String>, SetupError> {
    const KEY: &str = "ARUNA_NODE_LABELS";
    let raw = env.var(KEY).unwrap_or_default();
    let mut labels = BTreeMap::new();
    for pair in raw
        .split(',')
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
    {
        let (label_key, label_value) = pair
            .split_once('=')
            .ok_or_else(|| invalid_config_value(KEY, pair, "expected key=value"))?;
        let label_key = label_key.trim();
        let label_value = label_value.trim();
        if label_key.is_empty() {
            return Err(invalid_config_value(KEY, pair, "empty label key"));
        }
        if label_key == KIND_LABEL_KEY {
            return Err(invalid_config_value(
                KEY,
                pair,
                format!("{KIND_LABEL_KEY} is a reserved derived label"),
            ));
        }
        if label_key == LOCATION_LABEL_KEY {
            return Err(invalid_config_value(
                KEY,
                pair,
                format!("{LOCATION_LABEL_KEY} is derived from ARUNA_NODE_LOCATION"),
            ));
        }
        if label_key.starts_with(CLASS_LABEL_PREFIX) {
            return Err(invalid_config_value(
                KEY,
                pair,
                format!("{CLASS_LABEL_PREFIX}* labels are derived from the backends file"),
            ));
        }
        labels.insert(label_key.to_string(), label_value.to_string());
    }
    Ok(labels)
}

fn load_sync_config(env: &dyn SettingsEnv) -> Result<IrohRuntimeConfig, SetupError> {
    let default = IrohRuntimeConfig::default();
    Ok(IrohRuntimeConfig {
        connect_timeout: duration_secs_env(
            env,
            "DOCUMENT_SYNC_CONNECT_TIMEOUT_SECS",
            default.connect_timeout,
        )?,
        sync_io_timeout: duration_secs_env(
            env,
            "DOCUMENT_SYNC_IO_TIMEOUT_SECS",
            default.sync_io_timeout,
        )?,
        resync_interval: duration_secs_env(
            env,
            "DOCUMENT_SYNC_RESYNC_INTERVAL_SECS",
            default.resync_interval,
        )?,
        resync_initial_backoff: duration_secs_env(
            env,
            "DOCUMENT_SYNC_RESYNC_INITIAL_BACKOFF_SECS",
            default.resync_initial_backoff,
        )?,
        resync_max_backoff: duration_secs_env(
            env,
            "DOCUMENT_SYNC_RESYNC_MAX_BACKOFF_SECS",
            default.resync_max_backoff,
        )?,
        full_sweep_interval: duration_secs_env(
            env,
            "DOCUMENT_SYNC_FULL_SWEEP_INTERVAL_SECS",
            default.full_sweep_interval,
        )?,
        full_sweep_time_of_day: duration_secs_env(
            env,
            "DOCUMENT_SYNC_FULL_SWEEP_TIME_OF_DAY_SECS",
            default.full_sweep_time_of_day,
        )?,
    })
}

fn duration_secs_env(
    env: &dyn SettingsEnv,
    key: &'static str,
    default: Duration,
) -> Result<Duration, SetupError> {
    let Some(value) = env.var(key) else {
        return Ok(default);
    };
    let seconds = value
        .parse::<u64>()
        .map_err(|error| invalid_config_value(key, value, error))?;
    Ok(Duration::from_secs(seconds))
}

fn load_oidc_providers(env: &dyn SettingsEnv) -> Result<Vec<OidcProviderConfig>, SetupError> {
    let Some(provider_ids) = env.var("OIDC_PROVIDER_IDS") else {
        return Ok(Vec::new());
    };

    provider_ids
        .split(',')
        .map(str::trim)
        .filter(|provider_id| !provider_id.is_empty())
        .map(|provider_id| {
            let env_prefix = provider_id.to_ascii_uppercase().replace('-', "_");
            Ok(OidcProviderConfig {
                id: provider_id.to_string(),
                issuer: required_dynamic_var(env, &format!("OIDC_{env_prefix}_ISSUER"))?,
                audience: required_dynamic_var(env, &format!("OIDC_{env_prefix}_AUDIENCE"))?,
                discovery_url: required_dynamic_var(
                    env,
                    &format!("OIDC_{env_prefix}_DISCOVERY_URL"),
                )?,
            })
        })
        .collect()
}

fn required_dynamic_var(env: &dyn SettingsEnv, key: &str) -> Result<String, SetupError> {
    env.var(key).ok_or_else(|| {
        SetupError::ValueNotFound(dotenvy::Error::EnvVar(std::env::VarError::NotPresent))
    })
}

/// Rejects a relay list entry that is not a valid relay URL.
/// Accepts what iroh accepts as a relay: stored realm configs are read with the
/// same check, so a stricter one could stop a node from starting on its own config.
pub(crate) fn validate_relay_urls(key: &'static str, urls: &[String]) -> Result<(), SetupError> {
    for url in urls {
        url.parse::<iroh::RelayUrl>()
            .map_err(|error| invalid_config_value(key, url, error))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_api::s3::server::S3ServerTimeouts;

    /// A minimal valid operator environment; each case overrides what it
    /// exercises. Parsing never reads the process environment.
    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        let mut env = BTreeMap::from([
            (
                "STORAGE_PATH".to_string(),
                "/tmp/aruna-settings".to_string(),
            ),
            ("SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("PORTAL_MODE".to_string(), "disabled".to_string()),
            ("ARUNA_FJALL_PERSIST_MODE".to_string(), "buffer".to_string()),
        ]);
        for (key, value) in pairs {
            env.insert((*key).to_string(), (*value).to_string());
        }
        env
    }

    fn parse(pairs: &[(&str, &str)]) -> Result<Settings, SetupError> {
        read_settings_from(&env(pairs))
    }

    fn invalid_key(error: SetupError) -> &'static str {
        match error {
            SetupError::InvalidConfigValue { key, .. } => key,
            other => panic!("expected InvalidConfigValue, got {other:?}"),
        }
    }

    #[test]
    fn missing_path_named() {
        let mut raw = env(&[]);
        raw.remove("STORAGE_PATH");
        assert!(matches!(
            read_settings_from(&raw),
            Err(SetupError::MissingConfigValue("STORAGE_PATH"))
        ));
    }

    #[test]
    fn rocrate_limits_parse() {
        let settings = parse(&[]).unwrap();
        assert_eq!(settings.rocrate_limits, RoCrateLimits::default());

        let values = [1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 11];
        let pairs: Vec<(&str, String)> = [
            "ROCRATE_DIRECT_UPLOAD_BYTES",
            "ROCRATE_IMPORT_SOURCE_BYTES",
            "ROCRATE_EXPANDED_IMPORT_BYTES",
            "ROCRATE_EXPORT_ARTIFACT_BYTES",
            "ROCRATE_MAX_ENTRIES",
            "ROCRATE_METADATA_BYTES",
            "ROCRATE_KEY_BYTES",
            "ROCRATE_UPLOAD_RETENTION_MS",
            "ROCRATE_JOB_RETENTION_MS",
            "ROCRATE_MAX_ACTIVE_JOBS",
            "ROCRATE_HOLDER_TTL_MS",
            "ROCRATE_HOLDER_REFRESH_MS",
        ]
        .into_iter()
        .zip(values)
        .map(|(key, value)| (key, value.to_string()))
        .collect();
        let pairs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect();
        let settings = parse(&pairs).unwrap();
        assert_eq!(
            settings.rocrate_limits,
            RoCrateLimits {
                direct_upload_bytes: 1,
                import_source_bytes: 2,
                expanded_import_bytes: 3,
                export_artifact_bytes: 4,
                max_entries: 5,
                metadata_bytes: 6,
                key_bytes: 7,
                upload_retention_ms: 8,
                artifact_retention_ms: 9,
                max_active_jobs: 10,
                holder_ttl_ms: 12,
                holder_refresh_ms: 11,
            }
        );
    }

    #[test]
    fn rocrate_limits_rejected() {
        assert_eq!(
            invalid_key(parse(&[("ROCRATE_MAX_ENTRIES", "0")]).unwrap_err()),
            "ROCRATE_MAX_ENTRIES"
        );
        let error = parse(&[
            ("ROCRATE_HOLDER_TTL_MS", "5"),
            ("ROCRATE_HOLDER_REFRESH_MS", "5"),
        ])
        .unwrap_err();
        assert_eq!(invalid_key(error), "ROCRATE_HOLDER_REFRESH_MS");
    }

    #[test]
    fn timeout_overrides() {
        let settings = parse(&[
            ("ONBOARDING_BOOTSTRAP_TIMEOUT_SECS", "240"),
            ("ONBOARDING_DOCUMENT_SYNC_TIMEOUT_SECS", "300"),
            ("S3_INITIAL_REQUEST_TIMEOUT_SECS", "45"),
            ("S3_CONNECTION_IDLE_TIMEOUT_SECS", "90"),
            ("S3_STREAM_LIFETIME_TIMEOUT_SECS", "600"),
        ])
        .unwrap();
        assert_eq!(settings.bootstrap_timeout_secs, 240);
        assert_eq!(settings.sync_timeout_secs, 300);
        assert_eq!(settings.request_timeout_secs, 45);
        assert_eq!(settings.connection_idle_secs, 90);
        assert_eq!(settings.stream_lifetime_secs, 600);

        let settings = parse(&[]).unwrap();
        let defaults = S3ServerTimeouts::default();
        assert_eq!(settings.bootstrap_timeout_secs, 120);
        assert_eq!(settings.sync_timeout_secs, 60);
        assert_eq!(
            settings.request_timeout_secs,
            defaults.initial_request.as_secs()
        );
        assert_eq!(
            settings.connection_idle_secs,
            defaults.connection_idle.as_secs()
        );
        assert_eq!(
            settings.stream_lifetime_secs,
            defaults.stream_lifetime.as_secs()
        );
    }

    #[test]
    fn listener_pair_required() {
        let settings = parse(&[("S3_HOST", ""), ("S3_ADDRESS", "")]).unwrap();
        assert!(settings.s3_host.is_none());
        assert!(settings.s3_address.is_none());

        assert!(matches!(
            parse(&[("S3_HOST", "127.0.0.1:0")]).unwrap_err(),
            SetupError::MissingConfigValue("S3_ADDRESS")
        ));
        assert!(matches!(
            parse(&[("S3_ADDRESS", "127.0.0.1:0")]).unwrap_err(),
            SetupError::MissingConfigValue("S3_HOST")
        ));

        let settings = parse(&[("S3_HOST", "127.0.0.1:0"), ("S3_ADDRESS", "127.0.0.1:0")]).unwrap();
        assert_eq!(settings.s3_host.as_deref(), Some("127.0.0.1:0"));
        assert_eq!(settings.s3_address.as_deref(), Some("127.0.0.1:0"));
    }

    #[test]
    fn public_url_schemes() {
        for value in ["http://localhost:1337", "https://s3.example.test/base/"] {
            assert!(parse(&[("API_PUBLIC_URL", value)]).is_ok());
        }
        for value in [
            "file:///tmp/s3",
            "mailto:admin@example.test",
            "ftp://s3.example.test",
            "https://",
            "/relative/path",
        ] {
            let error = parse(&[("API_PUBLIC_URL", value)]).unwrap_err();
            assert_eq!(invalid_key(error), "API_PUBLIC_URL");
        }
    }

    #[test]
    fn persist_policy_validation() {
        assert_eq!(
            parse(&[]).unwrap().fjall_persist_policy,
            FjallPersistPolicy::default()
        );
        assert!(parse(&[("ARUNA_FJALL_PERSIST_MODE", "sync_all")]).is_ok());
        let error = parse(&[("ARUNA_FJALL_PERSIST_MODE", "nonsense")]).unwrap_err();
        assert_eq!(invalid_key(error), "ARUNA_FJALL_PERSIST_MODE");
    }

    #[test]
    fn assistant_proxy_conventions() {
        assert!(parse(&[]).unwrap().assistant_proxy);
        assert!(
            parse(&[("ASSISTANT_PROXY", "enabled")])
                .unwrap()
                .assistant_proxy
        );
        assert!(
            !parse(&[("ASSISTANT_PROXY", "disabled")])
                .unwrap()
                .assistant_proxy
        );
        assert_eq!(
            invalid_key(parse(&[("ASSISTANT_PROXY", "maybe")]).unwrap_err()),
            "ASSISTANT_PROXY"
        );
    }

    #[test]
    fn node_labels_validation() {
        assert!(parse(&[]).unwrap().node_labels.is_empty());
        let settings = parse(&[("ARUNA_NODE_LABELS", "a=1, b = two")]).unwrap();
        assert_eq!(
            settings.node_labels,
            BTreeMap::from([
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "two".to_string())
            ])
        );
        for raw in [
            "aruna-engine.org/kind=server",
            "aruna-engine.org/location=lab",
            "aruna-engine.org/storage-class/gold=yes",
            "a",
            "=x",
        ] {
            assert_eq!(
                invalid_key(parse(&[("ARUNA_NODE_LABELS", raw)]).unwrap_err()),
                "ARUNA_NODE_LABELS",
                "labels `{raw}` must be rejected"
            );
        }
    }

    #[test]
    fn oidc_providers_parse() {
        assert!(parse(&[]).unwrap().oidc_providers.is_empty());
        let settings = parse(&[
            ("OIDC_PROVIDER_IDS", "keycloak"),
            ("OIDC_KEYCLOAK_ISSUER", "https://issuer.example"),
            ("OIDC_KEYCLOAK_AUDIENCE", "aruna"),
            (
                "OIDC_KEYCLOAK_DISCOVERY_URL",
                "https://issuer.example/.well-known",
            ),
        ])
        .unwrap();
        assert_eq!(settings.oidc_providers.len(), 1);
        assert_eq!(settings.oidc_providers[0].id, "keycloak");

        assert!(matches!(
            parse(&[
                ("OIDC_PROVIDER_IDS", "keycloak"),
                ("OIDC_KEYCLOAK_ISSUER", "https://issuer.example"),
            ])
            .unwrap_err(),
            SetupError::ValueNotFound(_)
        ));
    }

    #[test]
    fn portal_artifact_requirements() {
        assert!(matches!(parse(&[]).unwrap().portal, PortalConfig::Disabled));
        assert_eq!(
            invalid_key(parse(&[("PORTAL_MODE", "carrier-pigeon")]).unwrap_err()),
            "PORTAL_MODE"
        );

        let base = [
            ("PORTAL_MODE", "artifact"),
            ("PORTAL_ARTIFACT_URL", "https://portal.example/app.tar.gz"),
            ("PORTAL_DIR", "/var/lib/aruna/portal"),
            ("PORTAL_SOCKET_ADDRESS", "127.0.0.1:8080"),
            ("API_PUBLIC_URL", "https://api.example"),
        ];
        let settings = parse(&base).unwrap();
        let PortalConfig::Artifact {
            socket_addr,
            artifact,
        } = settings.portal
        else {
            panic!("artifact portal must parse");
        };
        assert_eq!(socket_addr.to_string(), "127.0.0.1:8080");
        assert_eq!(
            artifact.artifact_url.as_deref(),
            Some("https://portal.example/app.tar.gz")
        );

        let mut without_dir = base.to_vec();
        without_dir.retain(|(key, _)| *key != "PORTAL_DIR");
        assert!(matches!(
            parse(&without_dir).unwrap_err(),
            SetupError::MissingConfigValue("PORTAL_DIR")
        ));

        let mut without_address = base.to_vec();
        without_address.retain(|(key, _)| *key != "PORTAL_SOCKET_ADDRESS");
        assert!(matches!(
            parse(&without_address).unwrap_err(),
            SetupError::MissingConfigValue("PORTAL_SOCKET_ADDRESS")
        ));

        let mut without_public = base.to_vec();
        without_public.retain(|(key, _)| *key != "API_PUBLIC_URL");
        assert!(matches!(
            parse(&without_public).unwrap_err(),
            SetupError::MissingConfigValue("API_PUBLIC_URL")
        ));
    }

    #[test]
    fn relay_urls_validate() {
        assert!(
            parse(&[(
                "P2P_ADDITIONAL_RELAY_URLS",
                "https://relay-a.example, https://relay-b.example"
            )])
            .is_ok()
        );
        assert_eq!(
            invalid_key(parse(&[("P2P_ADDITIONAL_RELAY_URLS", "not-a-url")]).unwrap_err()),
            "P2P_ADDITIONAL_RELAY_URLS"
        );
        // A relay URL iroh accepts must stay valid, as it was before the settings split.
        assert!(validate_relay_urls("realm_relay", &["wss://relay.example".to_string()]).is_ok());
    }

    #[test]
    fn duplicate_tokens_rejected() {
        assert!(reject_token_clashes(["hot", "cold"].into_iter().map(str::to_string)).is_ok());
        let error =
            reject_token_clashes(["hot-store", "hot_store"].into_iter().map(str::to_string))
                .unwrap_err();
        assert_eq!(invalid_key(error), "BLOB_BACKENDS_PATH");
    }

    #[test]
    fn node_labels_order() {
        let labels = super::parse_node_labels(&env(&[("ARUNA_NODE_LABELS", "b=2,a=1")])).unwrap();
        assert_eq!(
            labels.keys().cloned().collect::<Vec<_>>(),
            vec!["a".to_string(), "b".to_string()]
        );
    }
}
