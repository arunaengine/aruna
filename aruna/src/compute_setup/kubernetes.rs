//! Builds the Kubernetes executor backend, applies its network policy and returns the registry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_compute::ExecutorRegistry;
use tracing::info;

use super::settings::{
    compute_s3_endpoint, kubernetes_s3_access, kubernetes_workspace, session_s3_port,
};
use super::{ComputeBuildError, ComputeSettings, KubernetesSettings};
use crate::config::Config;

pub(super) async fn build(
    settings: &ComputeSettings,
    kubernetes: &KubernetesSettings,
    config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    let workspace = kubernetes_workspace(
        settings.local_only,
        compute_s3_endpoint(settings, config).as_deref(),
    )?;
    let s3_port = session_s3_port(kubernetes.s3_port.as_deref(), workspace.as_deref())?;
    let (s3_cidrs, s3_mount_driver) = kubernetes_s3_access(
        workspace.as_deref(),
        kubernetes.s3_cidrs.clone(),
        kubernetes.mount_driver.clone(),
    );
    let backend = aruna_compute::executor::kubernetes::KubernetesBackend::with_config(
        aruna_compute::KubernetesConfig {
            namespace: kubernetes.namespace.clone(),
            storage_class: kubernetes.storage_class.clone(),
            helper_image: kubernetes.helper_image.clone(),
            pull_deadline: kubernetes.pull_deadline,
            s3_cidrs: if workspace.is_some() {
                s3_cidrs
            } else {
                Vec::new()
            },
            s3_port,
            s3_mount_driver,
            policy_manifests: kubernetes.policy_manifests.clone(),
            service_account: kubernetes.service_account.clone(),
            execution_location: kubernetes.execution_location.clone(),
            execution_labels: kubernetes.execution_labels.clone(),
            node_selector: kubernetes.node_selector.clone(),
            envelope: settings
                .envelope
                .expect("a selected kubernetes backend has an envelope"),
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

#[cfg(test)]
mod pure_tests {
    use super::super::settings::parse_s3_cidrs;

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
}

/// Filesystem-boundary coverage for policy-path expansion; kept out of the
/// pure selection because it writes to a temporary directory.
#[cfg(test)]
mod tests {
    use super::super::settings::policy_paths;
    use tempfile::tempdir;

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
