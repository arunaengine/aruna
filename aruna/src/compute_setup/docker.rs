//! Docker backend construction.

use aruna_compute::ExecutorRegistry;
use tracing::info;

use super::ComputeBuildError;
use super::settings::{
    compute_envelope, compute_s3_endpoint, docker_workspace, env_duration, env_path, env_true,
    parse_disk_limit, session_s3_address, session_subnet,
};
use crate::config::Config;

pub(super) async fn build(config: &Config) -> Result<ExecutorRegistry, ComputeBuildError> {
    let disk_bytes = parse_disk_limit(
        dotenvy::var("ARUNA_COMPUTE_DOCKER_DISK_BYTES")
            .ok()
            .as_deref(),
    )?;
    let workspace = docker_workspace(
        env_true("ARUNA_COMPUTE_LOCAL_ONLY"),
        compute_s3_endpoint(config).as_deref(),
        config.s3_address.as_deref(),
    )?;
    let mut docker_config = aruna_compute::DockerConfig {
        default_disk_bytes: disk_bytes,
        session_subnet: session_subnet(),
        pull_deadline: env_duration("ARUNA_COMPUTE_DOCKER_PULL_DEADLINE", 300)?,
        envelope: compute_envelope()?,
        keep_failed: env_true("ARUNA_COMPUTE_KEEP_FAILED"),
        ..aruna_compute::DockerConfig::default()
    };
    if let Some(state_root) = env_path("ARUNA_COMPUTE_STATE_ROOT") {
        docker_config.state_root = state_root;
    }
    let session_subnet = docker_config.session_subnet.clone();
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
        .with_session_endpoint(
            session_s3_address(config, &session_subnet).map(|address| format!("http://{address}")),
        ))
}
