//! Compute settings read from the environment.
//!
//! [`collect`] is the only place that reads compute environment variables. It
//! returns typed values the backend builders consume, so a builder never reads
//! the environment again and validation can be tested from explicit values.

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
        other => Err(ComputeBuildError::Config(format!(
            "unknown ARUNA_COMPUTE_EXECUTOR `{other}`"
        ))),
    }
}

/// Reads every compute setting once. Backend-specific settings are parsed only
/// for the selected backend, so an invalid value for an unused backend never
/// fails a start.
pub(super) fn collect() -> Result<ComputeSettings, ComputeBuildError> {
    let selected = dotenvy::var("ARUNA_COMPUTE_EXECUTOR").unwrap_or_else(|_| "none".to_string());
    let backend = match selected_backend(&selected)? {
        SelectedBackend::None => BackendSettings::None,
        SelectedBackend::Docker => BackendSettings::Docker(collect_docker()?),
        SelectedBackend::Apptainer => BackendSettings::Apptainer(collect_apptainer()?),
        SelectedBackend::Kubernetes => BackendSettings::Kubernetes(collect_kubernetes()?),
    };
    let has_backend = !matches!(backend, BackendSettings::None);
    Ok(ComputeSettings {
        backend,
        optional: env_true("ARUNA_COMPUTE_OPTIONAL"),
        local_only: env_true("ARUNA_COMPUTE_LOCAL_ONLY"),
        s3_url: env_nonempty("ARUNA_COMPUTE_S3_URL"),
        envelope: match has_backend {
            true => Some(compute_envelope()?),
            false => None,
        },
    })
}

fn collect_docker() -> Result<super::DockerSettings, ComputeBuildError> {
    Ok(super::DockerSettings {
        disk_bytes: parse_disk_limit(
            dotenvy::var("ARUNA_COMPUTE_DOCKER_DISK_BYTES")
                .ok()
                .as_deref(),
        )?,
        session_subnet: session_subnet(),
        pull_deadline: env_duration("ARUNA_COMPUTE_DOCKER_PULL_DEADLINE", 300)?,
        keep_failed: env_true("ARUNA_COMPUTE_KEEP_FAILED"),
        state_root: env_path("ARUNA_COMPUTE_STATE_ROOT"),
    })
}

fn collect_apptainer() -> Result<super::ApptainerSettings, ComputeBuildError> {
    let cgroup_root = dotenvy::var("ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT")
        .map(PathBuf::from)
        .map_err(|_| {
            ComputeBuildError::Config(
                "Apptainer executor requires ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT".to_string(),
            )
        })?;
    let state_root = dotenvy::var("ARUNA_COMPUTE_APPTAINER_STATE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./compute-state/apptainer"));
    let sif_cache = dotenvy::var("ARUNA_COMPUTE_APPTAINER_SIF_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./compute-state/sif"));
    Ok(super::ApptainerSettings {
        cgroup_root,
        state_root,
        sif_cache,
        stop_grace: env_duration("ARUNA_COMPUTE_STOP_GRACE", 10)?,
        pull_deadline: env_duration("ARUNA_COMPUTE_APPTAINER_PULL_DEADLINE", 300)?,
    })
}

fn collect_kubernetes() -> Result<super::KubernetesSettings, ComputeBuildError> {
    let storage_class = dotenvy::var("ARUNA_COMPUTE_K8S_STORAGE_CLASS").map_err(|_| {
        ComputeBuildError::Config(
            "Kubernetes executor requires ARUNA_COMPUTE_K8S_STORAGE_CLASS".to_string(),
        )
    })?;
    let helper_image = dotenvy::var("ARUNA_COMPUTE_K8S_HELPER_IMAGE").map_err(|_| {
        ComputeBuildError::Config(
            "Kubernetes executor requires ARUNA_COMPUTE_K8S_HELPER_IMAGE".to_string(),
        )
    })?;
    let s3_cidrs = dotenvy::var("ARUNA_COMPUTE_K8S_S3_CIDRS")
        .ok()
        .map(|value| parse_s3_cidrs(&value))
        .transpose()?
        .unwrap_or_default();
    let policy_manifests = dotenvy::var("ARUNA_COMPUTE_K8S_POLICY_MANIFESTS")
        .ok()
        .map(|value| policy_paths(&value))
        .transpose()?
        .unwrap_or_default();
    Ok(super::KubernetesSettings {
        namespace: dotenvy::var("ARUNA_COMPUTE_K8S_NAMESPACE")
            .unwrap_or_else(|_| "default".to_string()),
        storage_class,
        helper_image,
        s3_cidrs,
        s3_port: env_nonempty("ARUNA_COMPUTE_K8S_S3_PORT"),
        mount_driver: env_nonempty("ARUNA_COMPUTE_K8S_S3_MOUNT_DRIVER"),
        policy_manifests,
        service_account: dotenvy::var("ARUNA_COMPUTE_K8S_SERVICE_ACCOUNT")
            .unwrap_or_else(|_| aruna_compute::DEFAULT_WORKLOAD_SA.to_string()),
        execution_location: dotenvy::var("ARUNA_COMPUTE_K8S_EXECUTION_LOCATION")
            .unwrap_or_default(),
        execution_labels: env_labels("ARUNA_COMPUTE_K8S_EXECUTION_LABELS")?,
        node_selector: env_labels("ARUNA_COMPUTE_K8S_NODE_SELECTOR")?,
        pull_deadline: env_duration("ARUNA_COMPUTE_K8S_PULL_DEADLINE", 300)?,
    })
}

/// The session subnet an operator configured, else the default.
pub(crate) fn session_subnet() -> String {
    dotenvy::var("ARUNA_COMPUTE_DOCKER_SESSION_SUBNET")
        .unwrap_or_else(|_| aruna_compute::executor::config::DEFAULT_SESSION_SUBNET.to_string())
}

/// The gateway address of the Docker session bridge, on the configured S3 port.
/// It is what a session container targets, whatever the node itself binds.
#[cfg(feature = "docker")]
pub(crate) fn session_s3_address(config: &Config, subnet: &str) -> Option<std::net::SocketAddr> {
    use aruna_compute::executor::docker::session_gateway;

    if dotenvy::var("ARUNA_COMPUTE_EXECUTOR")
        .unwrap_or_default()
        .trim()
        != "docker"
    {
        return None;
    }
    let port = config
        .s3_address
        .as_deref()?
        .parse::<std::net::SocketAddr>()
        .ok()?
        .port();
    match session_gateway(subnet) {
        Ok(gateway) => Some(std::net::SocketAddr::new(gateway.into(), port)),
        Err(error) => {
            tracing::warn!(subnet = %subnet, error = %error, "Session subnet has no gateway; sessions reach no S3 endpoint");
            None
        }
    }
}

#[cfg(not(feature = "docker"))]
pub(crate) fn session_s3_address(_config: &Config, _subnet: &str) -> Option<std::net::SocketAddr> {
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

fn env_nonempty(name: &str) -> Option<String> {
    dotenvy::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_true(name: &str) -> bool {
    dotenvy::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn compute_envelope() -> Result<ResourceEnvelope, String> {
    Ok(ResourceEnvelope {
        max_cpu_cores: env_number::<u32>("ARUNA_COMPUTE_MAX_CPU_CORES")?,
        max_ram_bytes: env_number::<u64>("ARUNA_COMPUTE_MAX_RAM_BYTES")?,
        max_disk_bytes: env_number::<u64>("ARUNA_COMPUTE_MAX_DISK_BYTES")?,
        max_concurrent: env_number::<u32>("ARUNA_COMPUTE_MAX_CONCURRENT")?,
    })
}

fn env_number<T: std::str::FromStr + Default + PartialEq>(name: &str) -> Result<Option<T>, String> {
    dotenvy::var(name)
        .ok()
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

fn env_duration(name: &str, default: u64) -> Result<Duration, String> {
    let seconds = dotenvy::var(name)
        .map(|value| value.parse::<u64>())
        .unwrap_or(Ok(default))
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if seconds == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(Duration::from_secs(seconds))
}

/// Parses a bounded `key=value,key2=value2` label or selector list.
pub(super) fn env_labels(name: &str) -> Result<BTreeMap<String, String>, String> {
    let Ok(value) = dotenvy::var(name) else {
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

/// A configured filesystem path; an empty value is treated as unset.
pub(super) fn env_path(name: &str) -> Option<PathBuf> {
    env_nonempty(name).map(PathBuf::from)
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
            matches!(&error, ComputeBuildError::Config(message) if message.contains("podman")),
            "unknown backend must be a configuration error: {error:?}"
        );
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
