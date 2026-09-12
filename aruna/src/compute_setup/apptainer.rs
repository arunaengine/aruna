//! Apptainer backend construction.

use aruna_compute::ExecutorRegistry;
use tracing::info;

use super::ComputeBuildError;
use super::settings::{compute_envelope, compute_s3_endpoint, env_duration};
use crate::config::Config;

pub(super) async fn build(config: &Config) -> Result<ExecutorRegistry, ComputeBuildError> {
    let cgroup_root = dotenvy::var("ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT")
        .map(std::path::PathBuf::from)
        .map_err(|_| {
            "Apptainer executor requires ARUNA_COMPUTE_APPTAINER_CGROUP_ROOT".to_string()
        })?;
    let state_root = dotenvy::var("ARUNA_COMPUTE_APPTAINER_STATE_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("./compute-state/apptainer"));
    let sif_cache = dotenvy::var("ARUNA_COMPUTE_APPTAINER_SIF_CACHE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("./compute-state/sif"));
    let backend = aruna_compute::executor::apptainer::ApptainerBackend::with_config(
        aruna_compute::ApptainerConfig {
            state_root,
            sif_cache,
            cgroup_root,
            stop_grace: env_duration("ARUNA_COMPUTE_STOP_GRACE", 10)?,
            pull_deadline: env_duration("ARUNA_COMPUTE_APPTAINER_PULL_DEADLINE", 300)?,
            envelope: compute_envelope()?,
            ..aruna_compute::ApptainerConfig::default()
        },
    )
    .map_err(|error| error.to_string())?;
    aruna_compute::ExecutorBackend::health(&backend)
        .await
        .map_err(|error| ComputeBuildError::Unavailable(error.to_string()))?;
    info!("Apptainer executor backend enabled");
    Ok(ExecutorRegistry::new()
        .with_backend(std::sync::Arc::new(backend))
        .with_workspace_endpoint(compute_s3_endpoint(config), "eu-central-1".to_string()))
}
