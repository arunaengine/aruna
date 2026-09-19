//! Reads and parses every compute setting from one operator input source into typed values.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use aruna_core::compute::ResourceEnvelope;

use super::{BackendSettings, ComputeBuildError, ComputeSettings};
use crate::config::Config;

/// One selected compute backend with its already-parsed settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SelectedBackend {
    None,
    Docker,
    Apptainer,
    Kubernetes,
}

/// Parses the operator's backend choice. Pure so the accepted spellings are
/// testable without touching the process environment.
pub(super) fn selected_backend(selected: &str) -> Result<SelectedBackend, ComputeBuildError> {
    match selected.trim() {
        // A supervisor that turns compute off writes a disabling value rather
        // than unsetting the key, which an inherited environment would refill.
        "none" | "off" | "" => Ok(SelectedBackend::None),
        "docker" => Ok(SelectedBackend::Docker),
        "apptainer" => Ok(SelectedBackend::Apptainer),
        "kubernetes" => Ok(SelectedBackend::Kubernetes),
        other => Err(ComputeBuildError::Invalid(format!(
            "unknown ARUNA_COMPUTE_EXECUTOR `{other}`"
        ))),
    }
}

/// Reads every compute setting once. Backend-specific settings are parsed only
/// for the selected backend, so an invalid value for an unused backend never
/// fails a start.
pub(super) fn collect(
    env: &dyn crate::settings::SettingsEnv,
) -> Result<ComputeSettings, ComputeBuildError> {
    let selected = env
        .var("ARUNA_COMPUTE_EXECUTOR")
        .unwrap_or_else(|| "none".to_string());
    let backend = match selected_backend(&selected)? {
        SelectedBackend::None => BackendSettings::None,
        SelectedBackend::Docker => BackendSettings::Docker(collect_docker(env)?),
        SelectedBackend::Apptainer => BackendSettings::Apptainer(collect_apptainer(env)?),
        SelectedBackend::Kubernetes => BackendSettings::Kubernetes(collect_kubernetes(env)?),
    };
    let has_backend = !matches!(backend, BackendSettings::None);
    Ok(ComputeSettings {
        backend,
        optional: parse_bool(env.var("ARUNA_COMPUTE_OPTIONAL").as_deref()),
        local_only: parse_bool(env.var("ARUNA_COMPUTE_LOCAL_ONLY").as_deref()),
        s3_url: parse_nonempty(env.var("ARUNA_COMPUTE_S3_URL").as_deref()),
        envelope: match has_backend {
            true => Some(compute_envelope(env)?),
            false => None,
        },
    })
}

/// The session subnet an operator configured, else the default. Read once,
/// inside the one collection boundary.
fn session_subnet(env: &dyn crate::settings::SettingsEnv) -> String {
    env.var("ARUNA_COMPUTE_DOCKER_SESSION_SUBNET")
        .unwrap_or_else(|| aruna_compute::executor::config::DEFAULT_SESSION_SUBNET.to_string())
}

fn collect_docker(
    env: &dyn crate::settings::SettingsEnv,
) -> Result<super::DockerSettings, ComputeBuildError> {
    Ok(super::DockerSettings {
        disk_bytes: parse_disk_limit(env.var("ARUNA_COMPUTE_DOCKER_DISK_BYTES").as_deref())?,
        session_subnet: session_subnet(env),
        pull_deadline: parse_duration(
            "ARUNA_COMPUTE_DOCKER_PULL_DEADLINE",
            env.var("ARUNA_COMPUTE_DOCKER_PULL_DEADLINE").as_deref(),
            300,
        )?,
        keep_failed: parse_bool(env.var("ARUNA_COMPUTE_KEEP_FAILED").as_deref()),
        state_root: parse_path(env.var("ARUNA_COMPUTE_STATE_ROOT").as_deref()),
    })
}

fn collect_apptainer(
    env: &dyn crate::settings::SettingsEnv,
) -> Result<super::ApptainerSettings, ComputeBuildError> {
    let cgroup_root = env
        .var("ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT")
        .map(PathBuf::from)
        .ok_or_else(|| {
            ComputeBuildError::Invalid(
                "Apptainer executor requires ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT".to_string(),
            )
        })?;
    let state_root = env
        .var("ARUNA_COMPUTE_APPTAINER_STATE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./compute-state/apptainer"));
    let sif_cache = env
        .var("ARUNA_COMPUTE_APPTAINER_SIF_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./compute-state/sif"));
    Ok(super::ApptainerSettings {
        cgroup_root,
        state_root,
        sif_cache,
        stop_grace: parse_duration(
            "ARUNA_COMPUTE_STOP_GRACE",
            env.var("ARUNA_COMPUTE_STOP_GRACE").as_deref(),
            10,
        )?,
        pull_deadline: parse_duration(
            "ARUNA_COMPUTE_APPTAINER_PULL_DEADLINE",
            env.var("ARUNA_COMPUTE_APPTAINER_PULL_DEADLINE").as_deref(),
            300,
        )?,
    })
}

fn collect_kubernetes(
    env: &dyn crate::settings::SettingsEnv,
) -> Result<super::KubernetesSettings, ComputeBuildError> {
    let storage_class = env.var("ARUNA_COMPUTE_K8S_STORAGE_CLASS").ok_or_else(|| {
        ComputeBuildError::Invalid(
            "Kubernetes executor requires ARUNA_COMPUTE_K8S_STORAGE_CLASS".to_string(),
        )
    })?;
    let helper_image = env.var("ARUNA_COMPUTE_K8S_HELPER_IMAGE").ok_or_else(|| {
        ComputeBuildError::Invalid(
            "Kubernetes executor requires ARUNA_COMPUTE_K8S_HELPER_IMAGE".to_string(),
        )
    })?;
    let s3_cidrs = env
        .var("ARUNA_COMPUTE_K8S_S3_CIDRS")
        .map(|value| parse_s3_cidrs(&value))
        .transpose()?
        .unwrap_or_default();
    let policy_manifests = env
        .var("ARUNA_COMPUTE_K8S_POLICY_MANIFESTS")
        .map(|value| policy_paths(&value))
        .transpose()?
        .unwrap_or_default();
    Ok(super::KubernetesSettings {
        namespace: env
            .var("ARUNA_COMPUTE_K8S_NAMESPACE")
            .unwrap_or_else(|| "default".to_string()),
        storage_class,
        helper_image,
        s3_cidrs,
        s3_port: parse_nonempty(env.var("ARUNA_COMPUTE_K8S_S3_PORT").as_deref()),
        mount_driver: parse_nonempty(env.var("ARUNA_COMPUTE_K8S_S3_MOUNT_DRIVER").as_deref()),
        policy_manifests,
        service_account: env
            .var("ARUNA_COMPUTE_K8S_SERVICE_ACCOUNT")
            .unwrap_or_else(|| aruna_compute::DEFAULT_WORKLOAD_SA.to_string()),
        execution_location: env
            .var("ARUNA_COMPUTE_K8S_EXECUTION_LOCATION")
            .unwrap_or_default(),
        execution_labels: parse_labels(
            "ARUNA_COMPUTE_K8S_EXECUTION_LABELS",
            env.var("ARUNA_COMPUTE_K8S_EXECUTION_LABELS").as_deref(),
        )?,
        node_selector: parse_labels(
            "ARUNA_COMPUTE_K8S_NODE_SELECTOR",
            env.var("ARUNA_COMPUTE_K8S_NODE_SELECTOR").as_deref(),
        )?,
        pull_deadline: parse_duration(
            "ARUNA_COMPUTE_K8S_PULL_DEADLINE",
            env.var("ARUNA_COMPUTE_K8S_PULL_DEADLINE").as_deref(),
            300,
        )?,
    })
}

/// The gateway address of the Docker session bridge, on the configured S3 port.
/// It is what a session container targets, whatever the node itself binds.
/// Pure in the typed settings: the caller already knows the selected backend.
#[cfg(feature = "docker")]
pub(super) fn session_s3_address(
    s3_address: Option<&str>,
    subnet: &str,
) -> Option<std::net::SocketAddr> {
    use aruna_compute::executor::docker::session_gateway;

    let port = s3_address?.parse::<std::net::SocketAddr>().ok()?.port();
    match session_gateway(subnet) {
        Ok(gateway) => Some(std::net::SocketAddr::new(gateway.into(), port)),
        Err(error) => {
            tracing::warn!(subnet = %subnet, error = %error, "Session subnet has no gateway; sessions reach no S3 endpoint");
            None
        }
    }
}

#[cfg(not(feature = "docker"))]
pub(super) fn session_s3_address(
    _s3_address: Option<&str>,
    _subnet: &str,
) -> Option<std::net::SocketAddr> {
    None
}

/// The container-facing S3 endpoint, from the compute override else the
/// portal-facing public url.
pub(super) fn compute_s3_endpoint(settings: &ComputeSettings, config: &Config) -> Option<String> {
    settings
        .s3_url
        .clone()
        .or_else(|| config.s3_public_url.clone())
}

/// A configured string; blank and absent both mean unset. The value is trimmed.
pub(super) fn parse_nonempty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// The accepted boolean spellings are exact and case-sensitive; use `1`, `true`
/// or `yes`. `` `TRUE` ``, `1 ` and an absent value are all false.
pub(super) fn parse_bool(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "true" | "yes"))
}

/// A configured path; blank and absent both mean unset.
pub(super) fn parse_path(value: Option<&str>) -> Option<PathBuf> {
    parse_nonempty(value).map(PathBuf::from)
}

fn compute_envelope(env: &dyn crate::settings::SettingsEnv) -> Result<ResourceEnvelope, String> {
    Ok(ResourceEnvelope {
        max_cpu_cores: parse_number(
            "ARUNA_COMPUTE_MAX_CPU_CORES",
            env.var("ARUNA_COMPUTE_MAX_CPU_CORES").as_deref(),
        )?,
        max_ram_bytes: parse_number(
            "ARUNA_COMPUTE_MAX_RAM_BYTES",
            env.var("ARUNA_COMPUTE_MAX_RAM_BYTES").as_deref(),
        )?,
        max_disk_bytes: parse_number(
            "ARUNA_COMPUTE_MAX_DISK_BYTES",
            env.var("ARUNA_COMPUTE_MAX_DISK_BYTES").as_deref(),
        )?,
        max_concurrent: parse_number(
            "ARUNA_COMPUTE_MAX_CONCURRENT",
            env.var("ARUNA_COMPUTE_MAX_CONCURRENT").as_deref(),
        )?,
    })
}

/// An unset value stays unmeasured; a set value is trimmed and must parse and
/// be non-zero.
pub(super) fn parse_number<T: std::str::FromStr + Default + PartialEq>(
    name: &str,
    value: Option<&str>,
) -> Result<Option<T>, String> {
    value
        .map(|value| parse_positive(name, value.trim()))
        .transpose()
}

/// Zero is rejected rather than silently making this node ineligible: leaving
/// the variable unset is how a dimension stays unmeasured.
pub(super) fn parse_positive<T: std::str::FromStr + Default + PartialEq>(
    name: &str,
    value: &str,
) -> Result<T, String> {
    let parsed = value
        .parse::<T>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    match parsed == T::default() {
        true => Err(format!("{name} must be greater than zero")),
        false => Ok(parsed),
    }
}

/// A duration in whole seconds. Absent means the default; a present value is
/// parsed exactly, so surrounding whitespace is an error rather than a trim.
pub(super) fn parse_duration(
    name: &str,
    value: Option<&str>,
    default: u64,
) -> Result<Duration, String> {
    let Some(value) = value else {
        return Ok(Duration::from_secs(default));
    };
    let seconds = value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if seconds == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(Duration::from_secs(seconds))
}

/// Parses a bounded `key=value,key2=value2` label or selector list. Entries and
/// keys are trimmed; absent means an empty map.
pub(super) fn parse_labels(
    name: &str,
    value: Option<&str>,
) -> Result<BTreeMap<String, String>, String> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .split_once('=')
                .filter(|(key, _)| !key.trim().is_empty())
                .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
                .ok_or_else(|| format!("{name} entries must be key=value"))
        })
        .collect()
}

/// Expands a comma-separated list of manifest files and directories. A
/// directory contributes its YAML files in name order.
pub(super) fn policy_paths(value: &str) -> Result<Vec<PathBuf>, String> {
    let mut paths = Vec::new();
    for entry in value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let path = PathBuf::from(entry);
        let metadata = std::fs::metadata(&path).map_err(|error| {
            format!("ARUNA_COMPUTE_K8S_POLICY_MANIFESTS entry `{entry}` is unreadable: {error}")
        })?;
        if !metadata.is_dir() {
            paths.push(path);
            continue;
        }
        let mut found = Vec::new();
        for file in std::fs::read_dir(&path).map_err(|error| {
            format!("ARUNA_COMPUTE_K8S_POLICY_MANIFESTS entry `{entry}` is unreadable: {error}")
        })? {
            let file = file
                .map_err(|error| {
                    format!("ARUNA_COMPUTE_K8S_POLICY_MANIFESTS entry `{entry}`: {error}")
                })?
                .path();
            if file
                .extension()
                .is_some_and(|suffix| suffix == "yaml" || suffix == "yml")
            {
                found.push(file);
            }
        }
        found.sort();
        paths.append(&mut found);
    }
    Ok(paths)
}

pub(super) fn parse_s3_cidrs(value: &str) -> Result<Vec<String>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|cidr| !cidr.is_empty())
        .map(|cidr| {
            let (address, prefix) = cidr
                .split_once('/')
                .ok_or_else(|| format!("invalid Kubernetes S3 CIDR `{cidr}`"))?;
            let address = address
                .parse::<std::net::IpAddr>()
                .map_err(|_| format!("invalid Kubernetes S3 CIDR `{cidr}`"))?;
            let prefix = prefix
                .parse::<u8>()
                .map_err(|_| format!("invalid Kubernetes S3 CIDR `{cidr}`"))?;
            let max_prefix = if address.is_ipv4() { 32 } else { 128 };
            if prefix > max_prefix {
                return Err(format!("invalid Kubernetes S3 CIDR `{cidr}`"));
            }
            Ok(cidr.to_string())
        })
        .collect()
}

pub(super) fn parse_disk_limit(value: Option<&str>) -> Result<Option<u64>, &'static str> {
    let Some(value) = value else {
        return Ok(None);
    };
    let bytes = value
        .parse::<u64>()
        .map_err(|_| "disk ceiling must be an integer byte count")?;
    if bytes == 0 {
        return Err("disk ceiling must be greater than zero");
    }
    Ok(Some(bytes))
}

pub(super) fn container_local_endpoint(endpoint: &str) -> bool {
    let Some(host) = reqwest::Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
    else {
        return true;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback() || address.is_unspecified())
}

/// The container-facing S3 endpoint the Docker registry carries, or `None` in
/// the local-only profile a user device runs: it exposes no S3 listener, so the
/// checks that keep a shared deployment reachable would only refuse it.
pub(super) fn docker_workspace(
    local_only: bool,
    endpoint: Option<&str>,
    s3_address: Option<&str>,
) -> Result<Option<String>, ComputeBuildError> {
    if local_only {
        return Ok(None);
    }
    let endpoint = endpoint.ok_or_else(|| {
        "Docker executor requires ARUNA_COMPUTE_S3_URL or S3_PUBLIC_URL".to_string()
    })?;
    if container_local_endpoint(endpoint) {
        return Err(
            "Docker executor requires a container-reachable S3_PUBLIC_URL"
                .to_string()
                .into(),
        );
    }
    if !s3_address
        .and_then(|address| address.parse::<std::net::SocketAddr>().ok())
        .is_some_and(|address| !address.ip().is_loopback())
    {
        return Err("Docker executor requires a non-loopback S3_ADDRESS"
            .to_string()
            .into());
    }
    Ok(Some(endpoint.to_string()))
}

/// The pod-facing S3 endpoint sessions and direct-S3 tasks read, or `None` in
/// the local-only profile, which exposes no S3 listener for them at all.
pub(super) fn kubernetes_workspace(
    local_only: bool,
    endpoint: Option<&str>,
) -> Result<Option<String>, ComputeBuildError> {
    if local_only {
        return Ok(None);
    }
    let endpoint = endpoint.ok_or_else(|| {
        "Kubernetes executor requires ARUNA_COMPUTE_S3_URL or S3_PUBLIC_URL".to_string()
    })?;
    if container_local_endpoint(endpoint) {
        return Err("Kubernetes executor requires a pod-reachable S3_PUBLIC_URL"
            .to_string()
            .into());
    }
    Ok(Some(endpoint.to_string()))
}

/// The port the S3 network policy opens. Without an explicit setting it follows
/// the endpoint pods actually connect to, so policy and endpoint cannot drift.
pub(super) fn session_s3_port(
    setting: Option<&str>,
    endpoint: Option<&str>,
) -> Result<u16, String> {
    if let Some(value) = setting {
        return value
            .trim()
            .parse::<u16>()
            .map_err(|_| "ARUNA_COMPUTE_K8S_S3_PORT must be a valid port".to_string());
    }
    Ok(endpoint
        .and_then(|value| reqwest::Url::parse(value).ok())
        .and_then(|url| url.port_or_known_default())
        .unwrap_or(443))
}

pub(super) fn kubernetes_s3_access(
    workspace: Option<&str>,
    cidrs: Vec<String>,
    mount_driver: Option<String>,
) -> (Vec<String>, Option<String>) {
    match workspace {
        Some(_) => (cidrs, mount_driver),
        None => (Vec::new(), None),
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    // The whole collection runs from a supplied map: no process environment is
    // read or mutated, and only the selected backend's values are parsed.
    #[test]
    fn collects_from_map() {
        let env = BTreeMap::from([
            ("ARUNA_COMPUTE_EXECUTOR".to_string(), "none".to_string()),
            ("ARUNA_COMPUTE_OPTIONAL".to_string(), "true".to_string()),
            ("ARUNA_COMPUTE_LOCAL_ONLY".to_string(), "1".to_string()),
            (
                "ARUNA_COMPUTE_S3_URL".to_string(),
                " https://s3.example.test ".to_string(),
            ),
            // An unused backend's invalid value must not fail the collection.
            (
                "ARUNA_COMPUTE_STOP_GRACE".to_string(),
                "not-a-number".to_string(),
            ),
        ]);
        let settings = collect(&env).expect("the supplied map collects");
        assert!(matches!(settings.backend, BackendSettings::None));
        assert!(settings.optional);
        assert!(settings.local_only);
        assert_eq!(settings.s3_url.as_deref(), Some("https://s3.example.test"));
        assert!(settings.envelope.is_none(), "no backend means no envelope");

        let unknown =
            BTreeMap::from([("ARUNA_COMPUTE_EXECUTOR".to_string(), "podman".to_string())]);
        assert!(collect(&unknown).is_err());
    }

    #[test]
    fn selects_known_backends() {
        assert_eq!(selected_backend("none").unwrap(), SelectedBackend::None);
        assert_eq!(selected_backend("off").unwrap(), SelectedBackend::None);
        assert_eq!(selected_backend("").unwrap(), SelectedBackend::None);
        assert_eq!(selected_backend(" none ").unwrap(), SelectedBackend::None);
        assert_eq!(selected_backend("docker").unwrap(), SelectedBackend::Docker);
        assert_eq!(
            selected_backend("apptainer").unwrap(),
            SelectedBackend::Apptainer
        );
        assert_eq!(
            selected_backend("kubernetes").unwrap(),
            SelectedBackend::Kubernetes
        );
        let error = selected_backend("podman").unwrap_err();
        assert!(
            matches!(&error, ComputeBuildError::Invalid(message) if message.contains("podman")),
            "unknown backend must be a configuration error: {error:?}"
        );
    }

    #[cfg(feature = "docker")]
    #[test]
    fn session_gateway_derived() {
        let address = session_s3_address(Some("127.0.0.1:9000"), "172.30.255.0/24")
            .expect("a docker subnet yields a gateway on the S3 port");
        assert_eq!(address.port(), 9000);
        assert!(session_s3_address(None, "172.30.255.0/24").is_none());
        assert!(session_s3_address(Some("127.0.0.1:9000"), "not-a-subnet").is_none());
    }

    // The accepted spellings are fixed: case-sensitive booleans, exact
    // durations, trimmed numbers and labels, blank means unset.
    #[test]
    fn documents_parser_conventions() {
        assert!(parse_bool(Some("1")));
        assert!(parse_bool(Some("true")));
        assert!(parse_bool(Some("yes")));
        assert!(!parse_bool(Some("TRUE")));
        assert!(!parse_bool(Some(" true")));
        assert!(!parse_bool(Some("")));
        assert!(!parse_bool(None));

        assert_eq!(parse_nonempty(Some(" x ")), Some("x".to_string()));
        assert_eq!(parse_nonempty(Some("   ")), None);
        assert_eq!(parse_nonempty(None), None);

        assert_eq!(parse_path(Some(" /tmp/x ")), Some(PathBuf::from("/tmp/x")));
        assert_eq!(parse_path(Some("")), None);

        assert_eq!(parse_duration("K", None, 7), Ok(Duration::from_secs(7)));
        assert_eq!(
            parse_duration("K", Some("10"), 7),
            Ok(Duration::from_secs(10))
        );
        assert!(parse_duration("K", Some(" 10 "), 7).is_err());
        assert!(parse_duration("K", Some("0"), 7).is_err());
        assert!(parse_duration("K", Some(""), 7).is_err());

        assert_eq!(parse_number::<u32>("K", None), Ok(None));
        assert_eq!(parse_number::<u32>("K", Some(" 4 ")), Ok(Some(4)));
        assert!(parse_number::<u32>("K", Some("0")).is_err());
        assert!(parse_number::<u32>("K", Some("")).is_err());

        assert_eq!(parse_labels("K", None), Ok(BTreeMap::new()));
        assert_eq!(
            parse_labels("K", Some(" a = 1 , b=2 ")),
            Ok(BTreeMap::from([
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]))
        );
        assert!(parse_labels("K", Some("=x")).is_err());
        assert!(parse_labels("K", Some("a")).is_err());
    }

    #[test]
    fn accepts_disk_limit() {
        assert_eq!(
            parse_disk_limit(Some("10737418240")),
            Ok(Some(10_737_418_240))
        );
    }

    #[test]
    fn rejects_disk_limit() {
        assert_eq!(parse_disk_limit(None), Ok(None));
        assert!(parse_disk_limit(Some("invalid")).is_err());
        assert!(parse_disk_limit(Some("0")).is_err());
    }

    #[test]
    fn skips_s3_checks() {
        // A device has no S3 listener and hands its containers no endpoint.
        assert_eq!(docker_workspace(true, None, None).unwrap(), None);
        assert_eq!(
            docker_workspace(true, Some("http://127.0.0.1:9000"), Some("127.0.0.1:9000")).unwrap(),
            None
        );
        assert_eq!(
            docker_workspace(false, Some("https://s3.example.test"), Some("0.0.0.0:9000")).unwrap(),
            Some("https://s3.example.test".to_string())
        );
        assert!(docker_workspace(false, None, Some("0.0.0.0:9000")).is_err());
        assert!(
            docker_workspace(false, Some("http://localhost:9000"), Some("0.0.0.0:9000")).is_err()
        );
        assert!(
            docker_workspace(
                false,
                Some("https://s3.example.test"),
                Some("127.0.0.1:9000")
            )
            .is_err()
        );
    }

    #[test]
    fn derives_s3_port() {
        // An explicit setting stays authoritative; otherwise the policy port is
        // the one pods reach the endpoint on.
        assert_eq!(session_s3_port(Some("9000"), None), Ok(9000));
        assert_eq!(
            session_s3_port(Some("9000"), Some("https://s3.example.test")),
            Ok(9000)
        );
        assert!(session_s3_port(Some("no"), None).is_err());
        assert_eq!(session_s3_port(None, None), Ok(443));
        assert_eq!(
            session_s3_port(None, Some("https://s3.example.test")),
            Ok(443)
        );
        assert_eq!(
            session_s3_port(None, Some("http://s3.example.test")),
            Ok(80)
        );
        assert_eq!(
            session_s3_port(None, Some("https://s3.example.test:9000/")),
            Ok(9000)
        );
    }

    #[test]
    fn guards_pod_endpoint() {
        // A pod cannot reach the controller's loopback, so an unreachable
        // endpoint must refuse startup instead of staging unreadable data.
        assert_eq!(kubernetes_workspace(true, None).unwrap(), None);
        assert_eq!(
            kubernetes_workspace(false, Some("https://s3.example.test")).unwrap(),
            Some("https://s3.example.test".to_string())
        );
        assert!(kubernetes_workspace(false, None).is_err());
        assert!(kubernetes_workspace(false, Some("http://127.0.0.1:9000")).is_err());
        assert!(kubernetes_workspace(false, Some("http://0.0.0.0:9000")).is_err());
        assert!(kubernetes_workspace(false, Some("http://localhost:9000")).is_err());
        assert_eq!(
            kubernetes_s3_access(
                None,
                vec!["10.0.0.0/8".to_string()],
                Some("mount.example".to_string()),
            ),
            (Vec::new(), None)
        );
        assert_eq!(
            kubernetes_s3_access(
                Some("https://s3.example.test"),
                vec!["10.0.0.0/8".to_string()],
                Some("mount.example".to_string()),
            ),
            (
                vec!["10.0.0.0/8".to_string()],
                Some("mount.example".to_string())
            )
        );
    }

    #[test]
    fn rejects_zero_ceiling() {
        // A zero ceiling would silently make this node ineligible for every
        // execution instead of leaving the dimension unmeasured.
        assert_eq!(
            parse_positive::<u32>("ARUNA_COMPUTE_MAX_CONCURRENT", "4"),
            Ok(4)
        );
        assert!(parse_positive::<u32>("ARUNA_COMPUTE_MAX_CONCURRENT", "0").is_err());
        assert!(parse_positive::<u64>("ARUNA_COMPUTE_MAX_RAM_BYTES", "-1").is_err());
    }
}
