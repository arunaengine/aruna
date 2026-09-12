//! Kubernetes backend construction.

use aruna_compute::ExecutorRegistry;
use tracing::info;

use super::ComputeBuildError;
use super::settings::{
    compute_envelope, compute_s3_endpoint, env_duration, env_labels, kubernetes_s3_access,
    kubernetes_workspace, read_mount_driver, session_s3_port,
};
use crate::config::Config;

pub(super) async fn build(config: &Config) -> Result<ExecutorRegistry, ComputeBuildError> {
    let storage_class = dotenvy::var("ARUNA_COMPUTE_K8S_STORAGE_CLASS")
        .map_err(|_| "Kubernetes executor requires ARUNA_COMPUTE_K8S_STORAGE_CLASS".to_string())?;
    let helper_image = dotenvy::var("ARUNA_COMPUTE_K8S_HELPER_IMAGE")
        .map_err(|_| "Kubernetes executor requires ARUNA_COMPUTE_K8S_HELPER_IMAGE".to_string())?;
    let s3_cidrs = dotenvy::var("ARUNA_COMPUTE_K8S_S3_CIDRS")
        .ok()
        .map(|value| parse_s3_cidrs(&value))
        .transpose()?
        .unwrap_or_default();
    let workspace = kubernetes_workspace(
        super::settings::env_true("ARUNA_COMPUTE_LOCAL_ONLY"),
        compute_s3_endpoint(config).as_deref(),
    )?;
    let s3_port = session_s3_port(
        dotenvy::var("ARUNA_COMPUTE_K8S_S3_PORT").ok().as_deref(),
        workspace.as_deref(),
    )?;
    let (s3_cidrs, s3_mount_driver) =
        kubernetes_s3_access(workspace.as_deref(), s3_cidrs, read_mount_driver());
    let policy_manifests = dotenvy::var("ARUNA_COMPUTE_K8S_POLICY_MANIFESTS")
        .ok()
        .map(|value| policy_paths(&value))
        .transpose()?
        .unwrap_or_default();
    let backend = aruna_compute::executor::kubernetes::KubernetesBackend::with_config(
        aruna_compute::KubernetesConfig {
            namespace: dotenvy::var("ARUNA_COMPUTE_K8S_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string()),
            storage_class,
            helper_image,
            pull_deadline: env_duration("ARUNA_COMPUTE_K8S_PULL_DEADLINE", 300)?,
            s3_cidrs: if workspace.is_some() {
                s3_cidrs
            } else {
                Vec::new()
            },
            s3_port,
            s3_mount_driver,
            policy_manifests,
            service_account: dotenvy::var("ARUNA_COMPUTE_K8S_SERVICE_ACCOUNT")
                .unwrap_or_else(|_| aruna_compute::DEFAULT_WORKLOAD_SA.to_string()),
            execution_location: dotenvy::var("ARUNA_COMPUTE_K8S_EXECUTION_LOCATION")
                .unwrap_or_default(),
            execution_labels: env_labels("ARUNA_COMPUTE_K8S_EXECUTION_LABELS")?,
            node_selector: env_labels("ARUNA_COMPUTE_K8S_NODE_SELECTOR")?,
            envelope: compute_envelope()?,
            ..aruna_compute::KubernetesConfig::default()
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    aruna_compute::ExecutorBackend::health(&backend)
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    // Pods keep the policy they started with, so already running sessions only
    // get a repaired one once the node applies it again.
    backend
        .apply_network()
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    info!(
        local_only = workspace.is_none(),
        "Kubernetes executor backend enabled"
    );
    Ok(ExecutorRegistry::new()
        .with_backend(std::sync::Arc::new(backend))
        .with_workspace_endpoint(workspace, "eu-central-1".to_string()))
}

/// Expands a comma-separated list of manifest files and directories. A
/// directory contributes its YAML files in name order.
fn policy_paths(value: &str) -> Result<Vec<std::path::PathBuf>, String> {
    let mut paths = Vec::new();
    for entry in value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let path = std::path::PathBuf::from(entry);
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

fn parse_s3_cidrs(value: &str) -> Result<Vec<String>, String> {
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

#[cfg(test)]
mod pure_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn validates_cluster_cidrs() {
        assert_eq!(
            parse_s3_cidrs(" 10.0.0.0/8, 2001:db8::/32 ").unwrap(),
            ["10.0.0.0/8", "2001:db8::/32"]
        );
        assert!(parse_s3_cidrs("10.0.0.0/33").is_err());
        assert!(parse_s3_cidrs("2001:db8::/129").is_err());
        assert!(parse_s3_cidrs("invalid/8").is_err());
    }

    #[test]
    fn expands_policy_paths() {
        let dir = tempdir().expect("temp dir");
        for name in ["b.yaml", "a.yml", "notes.txt"] {
            std::fs::write(dir.path().join(name), "").expect("write entry");
        }
        let single = dir.path().join("b.yaml");

        let paths = policy_paths(&format!(" {}, {} ", dir.path().display(), single.display()))
            .expect("the paths expand");

        assert_eq!(
            paths,
            [dir.path().join("a.yml"), dir.path().join("b.yaml"), single]
        );
        assert!(policy_paths(&dir.path().join("missing").display().to_string()).is_err());
    }
}
