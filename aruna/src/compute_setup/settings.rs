//! Compute settings read from the environment, with the planning helpers the
//! backend builders and their table tests share.

#[cfg(any(feature = "docker", feature = "kubernetes", test))]
use super::ComputeBuildError;
#[cfg(any(feature = "docker", feature = "apptainer", feature = "kubernetes"))]
use crate::config::Config;

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

pub(super) fn env_true(name: &str) -> bool {
    dotenvy::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

/// Static ceilings this node offers for execution. They hard-filter placement
/// and are the basis of the advertised ranking availability, so an unset
/// dimension stays unmeasured instead of becoming a false capacity claim.
#[cfg(any(feature = "docker", feature = "apptainer", feature = "kubernetes"))]
pub(super) fn compute_envelope() -> Result<aruna_core::compute::ResourceEnvelope, String> {
    Ok(aruna_core::compute::ResourceEnvelope {
        max_cpu_cores: env_number::<u32>("ARUNA_COMPUTE_MAX_CPU_CORES")?,
        max_ram_bytes: env_number::<u64>("ARUNA_COMPUTE_MAX_RAM_BYTES")?,
        max_disk_bytes: env_number::<u64>("ARUNA_COMPUTE_MAX_DISK_BYTES")?,
        max_concurrent: env_number::<u32>("ARUNA_COMPUTE_MAX_CONCURRENT")?,
    })
}

#[cfg(any(feature = "docker", feature = "apptainer", feature = "kubernetes"))]
pub(super) fn env_number<T: std::str::FromStr + Default + PartialEq>(
    name: &str,
) -> Result<Option<T>, String> {
    dotenvy::var(name)
        .ok()
        .map(|value| parse_positive(name, value.trim()))
        .transpose()
}

/// Zero is rejected rather than silently making this node ineligible: leaving
/// the variable unset is how a dimension stays unmeasured.
#[cfg(any(
    feature = "docker",
    feature = "apptainer",
    feature = "kubernetes",
    test
))]
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

#[cfg(any(feature = "docker", feature = "apptainer", feature = "kubernetes"))]
pub(super) fn env_duration(name: &str, default: u64) -> Result<std::time::Duration, String> {
    let seconds = dotenvy::var(name)
        .map(|value| value.parse::<u64>())
        .unwrap_or(Ok(default))
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if seconds == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(std::time::Duration::from_secs(seconds))
}

/// Parses a bounded `key=value,key2=value2` label or selector list.
#[cfg(feature = "kubernetes")]
pub(super) fn env_labels(name: &str) -> Result<std::collections::BTreeMap<String, String>, String> {
    let Ok(value) = dotenvy::var(name) else {
        return Ok(std::collections::BTreeMap::new());
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

#[cfg(feature = "kubernetes")]
pub(super) fn read_mount_driver() -> Option<String> {
    dotenvy::var("ARUNA_COMPUTE_K8S_S3_MOUNT_DRIVER")
        .ok()
        .filter(|driver| !driver.is_empty())
}

#[cfg(any(feature = "docker", feature = "apptainer", feature = "kubernetes"))]
/// Containers may need a different S3 endpoint than browsers: the override
/// keeps the portal-facing url on loopback (strict CSP) while container
/// workloads get a host-reachable one.
pub(super) fn compute_s3_endpoint(config: &Config) -> Option<String> {
    dotenvy::var("ARUNA_COMPUTE_S3_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| config.s3_public_url.clone())
}

/// A configured filesystem path; an empty value is treated as unset.
#[cfg(feature = "docker")]
pub(super) fn env_path(name: &str) -> Option<std::path::PathBuf> {
    dotenvy::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
}

#[cfg(any(feature = "docker", feature = "kubernetes", test))]
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

#[cfg(any(feature = "docker", test))]
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

/// The container-facing S3 endpoint the Docker registry carries, or `None` in
/// the local-only profile a user device runs: it exposes no S3 listener, so the
/// checks that keep a shared deployment reachable would only refuse it.
#[cfg(any(feature = "docker", test))]
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
#[cfg(any(feature = "kubernetes", test))]
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
#[cfg(any(feature = "kubernetes", test))]
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

#[cfg(any(feature = "kubernetes", test))]
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
