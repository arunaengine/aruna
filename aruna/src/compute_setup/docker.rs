//! Builds the Docker executor backend, its session network and the executor registry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_compute::ExecutorRegistry;
use tracing::info;

use super::settings::{compute_s3_endpoint, docker_workspace, session_s3_address};
use super::{ComputeBuildError, ComputeSettings, DockerSettings};
use crate::config::Config;

pub(super) async fn build(
    settings: &ComputeSettings,
    docker: &DockerSettings,
    config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    let workspace = docker_workspace(
        settings.local_only,
        compute_s3_endpoint(settings, config).as_deref(),
        config.s3_address.as_deref(),
    )?;
    let mut docker_config = aruna_compute::DockerConfig {
        default_disk_bytes: docker.disk_bytes,
        session_subnet: docker.session_subnet.clone(),
        pull_deadline: docker.pull_deadline,
        envelope: settings
            .envelope
            .expect("a selected docker backend has an envelope"),
        keep_failed: docker.keep_failed,
        ..aruna_compute::DockerConfig::default()
    };
    if let Some(state_root) = &docker.state_root {
        docker_config.state_root = state_root.clone();
    }
    let session_s3 = session_s3_address(config.s3_address.as_deref(), &docker.session_subnet);
    let backend = aruna_compute::executor::docker::DockerBackend::with_config(docker_config)
        .map_err(|error| error.to_string())?;
    aruna_compute::ExecutorBackend::health(&backend)
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    // The bridge must exist before the S3 listener binds its gateway address.
    backend
        .ensure_session_network()
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    info!(
        local_only = workspace.is_none(),
        "Docker executor backend enabled"
    );
    Ok(ExecutorRegistry::new()
        .with_backend(std::sync::Arc::new(backend))
        .with_workspace_endpoint(workspace, "eu-central-1".to_string())
        .with_session_endpoint(session_s3.map(|address| format!("http://{address}"))))
}
