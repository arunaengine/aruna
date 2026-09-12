//! Selection and construction of the node's compute executor.
//!
//! `settings::collect` reads the operator's compute environment once into
//! [`ComputeSettings`]; the backend modules only consume those typed values.
//! The node keeps running without compute when the operator marks compute
//! optional.

// Without a compiled backend the parsed settings are only constructed, never
// read; keep the no-backend feature check warning-free.
#![cfg_attr(
    not(any(feature = "docker", feature = "apptainer", feature = "kubernetes")),
    allow(dead_code)
)]

mod settings;

#[cfg(feature = "apptainer")]
mod apptainer;
#[cfg(feature = "docker")]
mod docker;
#[cfg(feature = "kubernetes")]
mod kubernetes;

use aruna_compute::ExecutorRegistry;
use aruna_core::compute::ResourceEnvelope;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

use crate::config::Config;

pub(crate) use settings::{session_s3_address, session_subnet};

/// Why a selected compute backend could not be built. The categories stay
/// distinct so an invalid configuration is never silently treated as
/// unavailable compute.
#[derive(Debug)]
pub(crate) enum ComputeBuildError {
    Config(String),
    Unavailable(String),
}

impl From<String> for ComputeBuildError {
    fn from(error: String) -> Self {
        Self::Config(error)
    }
}

impl From<&'static str> for ComputeBuildError {
    fn from(error: &'static str) -> Self {
        Self::Config(error.to_string())
    }
}

/// One selected compute backend with its already-parsed settings.
pub(super) enum BackendSettings {
    None,
    Docker(DockerSettings),
    Apptainer(ApptainerSettings),
    Kubernetes(KubernetesSettings),
}

/// The operator's compute inputs, parsed at one boundary. Builders consume
/// these values and never read the environment again.
pub(super) struct ComputeSettings {
    pub(super) backend: BackendSettings,
    /// A missing runtime or feature is tolerated only when the operator marks
    /// compute optional.
    pub(super) optional: bool,
    pub(super) local_only: bool,
    /// `ARUNA_COMPUTE_S3_URL`; the public url is the fallback at build time.
    pub(super) s3_url: Option<String>,
    pub(super) envelope: Option<ResourceEnvelope>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct DockerSettings {
    pub(super) disk_bytes: Option<u64>,
    pub(super) session_subnet: String,
    pub(super) pull_deadline: Duration,
    pub(super) keep_failed: bool,
    pub(super) state_root: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ApptainerSettings {
    pub(super) cgroup_root: PathBuf,
    pub(super) state_root: PathBuf,
    pub(super) sif_cache: PathBuf,
    pub(super) stop_grace: Duration,
    pub(super) pull_deadline: Duration,
}

#[derive(Clone, Debug, Default)]
pub(super) struct KubernetesSettings {
    pub(super) namespace: String,
    pub(super) storage_class: String,
    pub(super) helper_image: String,
    pub(super) s3_cidrs: Vec<String>,
    pub(super) s3_port: Option<String>,
    pub(super) mount_driver: Option<String>,
    pub(super) policy_manifests: Vec<PathBuf>,
    pub(super) service_account: String,
    pub(super) execution_location: String,
    pub(super) execution_labels: std::collections::BTreeMap<String, String>,
    pub(super) node_selector: std::collections::BTreeMap<String, String>,
    pub(super) pull_deadline: Duration,
}

/// Builds the registry for the selected backend, or `None` when compute is
/// turned off. An unavailable backend is allowed only when the operator marks
/// compute optional; invalid configuration always fails.
pub(crate) async fn build_registry(
    config: &Config,
) -> Result<Option<Arc<ExecutorRegistry>>, String> {
    let settings = settings::collect().map_err(|error| match error {
        ComputeBuildError::Config(message) | ComputeBuildError::Unavailable(message) => message,
    })?;
    let result = match &settings.backend {
        BackendSettings::None => return Ok(None),
        BackendSettings::Docker(docker) => build_docker(&settings, docker, config).await,
        BackendSettings::Apptainer(apptainer) => {
            build_apptainer(&settings, apptainer, config).await
        }
        BackendSettings::Kubernetes(kubernetes) => {
            build_kubernetes(&settings, kubernetes, config).await
        }
    };
    let registry = match result {
        Ok(registry) => Some(Arc::new(registry)),
        Err(ComputeBuildError::Unavailable(error)) if settings.optional => {
            warn!(reason = %error, "Compute executor unavailable; running without compute");
            None
        }
        Err(ComputeBuildError::Config(error) | ComputeBuildError::Unavailable(error)) => {
            return Err(error);
        }
    };
    Ok(registry)
}

#[cfg(feature = "docker")]
use docker::build as build_docker;
#[cfg(not(feature = "docker"))]
async fn build_docker(
    _settings: &ComputeSettings,
    _docker: &DockerSettings,
    _config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err("Docker executor feature is not compiled".to_string().into())
}

#[cfg(feature = "apptainer")]
use apptainer::build as build_apptainer;
#[cfg(not(feature = "apptainer"))]
async fn build_apptainer(
    _settings: &ComputeSettings,
    _apptainer: &ApptainerSettings,
    _config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err("Apptainer executor feature is not compiled"
        .to_string()
        .into())
}

#[cfg(feature = "kubernetes")]
use kubernetes::build as build_kubernetes;
#[cfg(not(feature = "kubernetes"))]
async fn build_kubernetes(
    _settings: &ComputeSettings,
    _kubernetes: &KubernetesSettings,
    _config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err("Kubernetes executor feature is not compiled"
        .to_string()
        .into())
}
