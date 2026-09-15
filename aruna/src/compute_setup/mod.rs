//! Selection and construction of the node's compute executor: [`collect`] reads
//! one explicit operator-input source into [`ComputeSettings`], and
//! [`build_registry`] receives that value instead of rereading the environment.

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

/// Why a selected compute backend could not be built. The categories stay
/// distinct so an invalid configuration is never silently treated as
/// unavailable compute.
#[derive(Debug)]
pub(crate) enum ComputeBuildError {
    /// The operator's values are invalid or contradictory.
    Invalid(String),
    /// The selected backend is not compiled into this binary. With every
    /// backend feature enabled only the table tests construct it.
    #[cfg_attr(
        all(feature = "docker", feature = "apptainer", feature = "kubernetes"),
        allow(dead_code)
    )]
    Unsupported(String),
    /// The backend exists but its runtime or daemon is not usable right now.
    Unavailable(String),
}

impl From<String> for ComputeBuildError {
    fn from(error: String) -> Self {
        Self::Invalid(error)
    }
}

impl From<&'static str> for ComputeBuildError {
    fn from(error: &'static str) -> Self {
        Self::Invalid(error.to_string())
    }
}

/// Whether an error may be tolerated by optional compute. Only a temporarily
/// unavailable backend is tolerable; invalid settings and missing compiled
/// support always fail a start.
pub(super) fn optional_tolerates(error: &ComputeBuildError) -> bool {
    matches!(error, ComputeBuildError::Unavailable(_))
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

/// The compute side of the node: the built registry plus every value listener
/// assembly needs, resolved once from the same typed settings.
pub(crate) struct ComputeSetup {
    pub(crate) registry: Option<Arc<ExecutorRegistry>>,
    /// The Docker session bridge gateway, resolved with the backend settings.
    pub(crate) session_s3: Option<std::net::SocketAddr>,
}

impl ComputeSettings {
    /// The session bridge gateway for the selected backend. Docker is the only
    /// backend with a session bridge; the value comes from its typed settings,
    /// never from a second environment read.
    pub(super) fn session_s3(&self, config: &Config) -> Option<std::net::SocketAddr> {
        let BackendSettings::Docker(docker) = &self.backend else {
            return None;
        };
        settings::session_s3_address(config.s3_address.as_deref(), &docker.session_subnet)
    }
}

/// Collects the typed compute settings once from one explicit operator-input
/// source. Production passes the process environment at the resource boundary;
/// tests pass a map.
pub(crate) fn collect(env: &dyn crate::settings::SettingsEnv) -> Result<ComputeSettings, String> {
    settings::collect(env).map_err(compute_error_message)
}

/// Builds the registry for the selected backend from already-collected settings,
/// or `None` when compute is off. An unavailable backend is allowed only when
/// compute is optional; invalid configuration always fails.
pub(crate) async fn build_registry(
    config: &Config,
    settings: &ComputeSettings,
) -> Result<ComputeSetup, String> {
    let session_s3 = settings.session_s3(config);
    let result = match &settings.backend {
        BackendSettings::None => {
            return Ok(ComputeSetup {
                registry: None,
                session_s3,
            });
        }
        BackendSettings::Docker(docker) => build_docker(settings, docker, config).await,
        BackendSettings::Apptainer(apptainer) => build_apptainer(settings, apptainer, config).await,
        BackendSettings::Kubernetes(kubernetes) => {
            build_kubernetes(settings, kubernetes, config).await
        }
    };
    let registry = match result {
        Ok(registry) => Some(Arc::new(registry)),
        Err(error) if settings.optional && optional_tolerates(&error) => {
            warn!(reason = %compute_error_message(error), "Compute executor unavailable; running without compute");
            None
        }
        Err(error) => return Err(compute_error_message(error)),
    };
    Ok(ComputeSetup {
        registry,
        session_s3,
    })
}

fn compute_error_message(error: ComputeBuildError) -> String {
    match error {
        ComputeBuildError::Invalid(message)
        | ComputeBuildError::Unsupported(message)
        | ComputeBuildError::Unavailable(message) => message,
    }
}

#[cfg(feature = "docker")]
use docker::build as build_docker;
#[cfg(not(feature = "docker"))]
async fn build_docker(
    _settings: &ComputeSettings,
    _docker: &DockerSettings,
    _config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err(ComputeBuildError::Unsupported(
        "Docker executor feature is not compiled".to_string(),
    ))
}

#[cfg(feature = "apptainer")]
use apptainer::build as build_apptainer;
#[cfg(not(feature = "apptainer"))]
async fn build_apptainer(
    _settings: &ComputeSettings,
    _apptainer: &ApptainerSettings,
    _config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err(ComputeBuildError::Unsupported(
        "Apptainer executor feature is not compiled".to_string(),
    ))
}

#[cfg(feature = "kubernetes")]
use kubernetes::build as build_kubernetes;
#[cfg(not(feature = "kubernetes"))]
async fn build_kubernetes(
    _settings: &ComputeSettings,
    _kubernetes: &KubernetesSettings,
    _config: &Config,
) -> Result<ExecutorRegistry, ComputeBuildError> {
    Err(ComputeBuildError::Unsupported(
        "Kubernetes executor feature is not compiled".to_string(),
    ))
}

#[cfg(test)]
mod pure_tests {
    use super::{ComputeBuildError, optional_tolerates};

    // Optional compute tolerates exactly one category: a temporarily
    // unavailable backend. Invalid settings and missing compiled support are
    // fatal with or without the flag.
    #[test]
    fn optional_policy_table() {
        let effective =
            |error: &ComputeBuildError, optional: bool| optional && optional_tolerates(error);
        let invalid = ComputeBuildError::Invalid("bad".to_string());
        let unsupported = ComputeBuildError::Unsupported("missing".to_string());
        let unavailable = ComputeBuildError::Unavailable("down".to_string());

        assert!(!effective(&invalid, false));
        assert!(
            !effective(&invalid, true),
            "invalid config is never disabled"
        );
        assert!(!effective(&unsupported, false));
        assert!(
            !effective(&unsupported, true),
            "missing compiled support is never disabled"
        );
        assert!(
            !effective(&unavailable, false),
            "required compute must fail"
        );
        assert!(
            effective(&unavailable, true),
            "optional compute may disable"
        );
    }
}
